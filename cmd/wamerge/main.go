// wamerge: adds the messages captured by wadump (raw history-sync packets)
// to a decrypted Android WhatsApp msgstore.db, for every message newer than
// the newest message already in that backup that predates the cutoff gap.
//
// Usage: wamerge <data dir> <msgstore.db to modify in place> <media staging dir>
//
//	<gap start YYYY-MM-DD> <file listing media already on the phone> [-dry]
//
// Messages already present (same key_id) are skipped. Media files found in
// <data dir>/media are copied into <media staging dir> under WhatsApp's own
// relative layout ("Media/WhatsApp Images/IMG-...jpg") ready for adb push.
package main

import (
	"bufio"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type rec struct {
	chat string
	wm   *waWeb.WebMessageInfo
}

var (
	dataDir, dbPath, stageDir string
	dry                       bool
	db                        *sql.Tx
	jidRow                    = map[string]int64{} // raw_string -> jid._id
	chatRow                   = map[int64]int64{}  // jid._id -> chat._id
	altJID                    = map[string]string{}
	chatNames                 = map[string]string{}
	mediaFiles                = map[string]string{} // message id -> local file
	usedNames                 = map[string]bool{}   // staged relative paths
	myPN, myLID               string
	stats                     = map[string]int{}
	chatCols                  []string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func bare(j string) string {
	// strip device part: user:dev@server -> user@server, and agent "user.0:dev"
	at := strings.IndexByte(j, '@')
	if at < 0 {
		return j
	}
	u, s := j[:at], j[at+1:]
	if i := strings.IndexByte(u, ':'); i >= 0 {
		u = u[:i]
	}
	if i := strings.IndexByte(u, '.'); i >= 0 && s != "g.us" {
		u = u[:i]
	}
	return u + "@" + s
}

func unwrap(m *waE2E.Message) *waE2E.Message {
	for i := 0; i < 5 && m != nil; i++ {
		switch {
		case m.GetEphemeralMessage().GetMessage() != nil:
			m = m.GetEphemeralMessage().GetMessage()
		case m.GetViewOnceMessage().GetMessage() != nil:
			m = m.GetViewOnceMessage().GetMessage()
		case m.GetViewOnceMessageV2().GetMessage() != nil:
			m = m.GetViewOnceMessageV2().GetMessage()
		case m.GetViewOnceMessageV2Extension().GetMessage() != nil:
			m = m.GetViewOnceMessageV2Extension().GetMessage()
		case m.GetDocumentWithCaptionMessage().GetMessage() != nil:
			m = m.GetDocumentWithCaptionMessage().GetMessage()
		case m.GetEditedMessage().GetMessage() != nil:
			m = m.GetEditedMessage().GetMessage()
		case m.GetAssociatedChildMessage().GetMessage() != nil:
			m = m.GetAssociatedChildMessage().GetMessage()
		case m.GetLottieStickerMessage().GetMessage() != nil:
			m = m.GetLottieStickerMessage().GetMessage()
		default:
			return m
		}
	}
	return m
}

func jidType(j string) int {
	switch {
	case strings.HasSuffix(j, "@g.us"):
		return 1
	case strings.HasSuffix(j, "@lid"):
		return 18
	case strings.HasSuffix(j, "@newsletter"):
		return 21
	case strings.HasSuffix(j, "@broadcast"):
		return 5
	}
	return 0
}

// getJID returns the jid row for j, creating it when missing.
func getJID(j string) int64 {
	j = bare(j)
	if id, ok := jidRow[j]; ok {
		return id
	}
	at := strings.IndexByte(j, '@')
	res, err := db.Exec(`INSERT INTO jid(user,server,agent,device,type,raw_string) VALUES(?,?,0,0,?,?)`, j[:at], j[at+1:], jidType(j), j)
	must(err)
	id, _ := res.LastInsertId()
	jidRow[j] = id
	stats["new jid rows"]++
	return id
}

// getChat finds the Android chat for j (trying its LID/PN twin), creating one when missing.
func getChat(j string, ts int64) int64 {
	j = bare(j)
	for _, cand := range []string{j, altJID[j]} {
		if cand == "" {
			continue
		}
		if jr, ok := jidRow[cand]; ok {
			if c, ok := chatRow[jr]; ok {
				return c
			}
		}
	}
	jr := getJID(j)
	// copy every column from a WhatsApp-made chat of the same kind, then
	// override the identity fields; the message pointers are set by fixOrder
	srv := j[strings.IndexByte(j, '@')+1:]
	var tmpl int64
	must(db.QueryRow(`SELECT MAX(c._id) FROM chat c JOIN jid x ON x._id=c.jid_row_id
		WHERE x.server=? AND c.account_jid_row_id IS NOT NULL AND c.hidden=0`, srv).Scan(&tmpl))
	if chatCols == nil {
		rows, err := db.Query(`PRAGMA table_info(chat)`)
		must(err)
		for rows.Next() {
			var cid, notnull, pk int
			var name, typ string
			var dflt any
			rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk)
			if name != "_id" {
				chatCols = append(chatCols, name)
			}
		}
		rows.Close()
	}
	sel := make([]string, len(chatCols))
	for i, c := range chatCols {
		switch c {
		case "jid_row_id", "account_jid_row_id":
			sel[i] = "?1"
		case "subject":
			sel[i] = "?2"
		case "created_timestamp", "sort_timestamp":
			sel[i] = "?3"
		case "display_message_row_id", "last_message_row_id", "last_read_message_row_id",
			"last_read_receipt_sent_message_row_id", "last_important_message_row_id",
			"last_read_message_sort_id", "display_message_sort_id", "last_message_sort_id",
			"last_read_receipt_sent_message_sort_id", "change_number_notified_message_row_id",
			"last_read_ephemeral_message_row_id":
			sel[i] = "NULL"
		case "last_message_reaction_row_id", "last_seen_message_reaction_row_id":
			sel[i] = "0"
		default:
			sel[i] = c
		}
	}
	res, err := db.Exec(`INSERT INTO chat(`+strings.Join(chatCols, ",")+`) SELECT `+strings.Join(sel, ",")+
		` FROM chat WHERE _id=?4`, jr, nullStr(chatNames[j]), ts*1000, tmpl)
	must(err)
	id, _ := res.LastInsertId()
	chatRow[jr] = id
	stats["new chats"]++
	return id
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func b64(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return base64.StdEncoding.EncodeToString(b)
}

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// media describes the common fields of WhatsApp media messages.
type media struct {
	mtype                          int
	mime, url, directPath, caption string
	docCaption, fileName           string
	key, sha, encSha, sidecar      []byte
	length                         uint64
	width, height, seconds, pages  uint32
	keyTS                          int64
	scanLen                        uint32
	animated, ptt, gif             bool
	dir, prefix, ext               string
}

func mediaOf(m *waE2E.Message) *media {
	switch {
	case m.ImageMessage != nil:
		x := m.GetImageMessage()
		md := &media{mtype: 1, mime: x.GetMimetype(), url: x.GetURL(), directPath: x.GetDirectPath(), caption: x.GetCaption(),
			key: x.GetMediaKey(), sha: x.GetFileSHA256(), encSha: x.GetFileEncSHA256(), length: x.GetFileLength(),
			width: x.GetWidth(), height: x.GetHeight(), keyTS: x.GetMediaKeyTimestamp(), sidecar: x.GetFirstScanSidecar(),
			dir: "WhatsApp Images", prefix: "IMG", ext: "jpg"}
		md.scanLen = x.GetFirstScanLength()
		return md
	case m.VideoMessage != nil || m.PtvMessage != nil:
		x := m.GetVideoMessage()
		if x == nil {
			x = m.GetPtvMessage()
		}
		md := &media{mtype: 3, mime: x.GetMimetype(), url: x.GetURL(), directPath: x.GetDirectPath(), caption: x.GetCaption(),
			key: x.GetMediaKey(), sha: x.GetFileSHA256(), encSha: x.GetFileEncSHA256(), length: x.GetFileLength(),
			width: x.GetWidth(), height: x.GetHeight(), seconds: x.GetSeconds(), keyTS: x.GetMediaKeyTimestamp(),
			dir: "WhatsApp Video", prefix: "VID", ext: "mp4"}
		if x.GetGifPlayback() {
			md.mtype, md.gif, md.dir = 13, true, "WhatsApp Animated Gifs"
		}
		return md
	case m.AudioMessage != nil:
		x := m.GetAudioMessage()
		md := &media{mtype: 2, mime: x.GetMimetype(), url: x.GetURL(), directPath: x.GetDirectPath(),
			key: x.GetMediaKey(), sha: x.GetFileSHA256(), encSha: x.GetFileEncSHA256(), length: x.GetFileLength(),
			seconds: x.GetSeconds(), keyTS: x.GetMediaKeyTimestamp(), ptt: x.GetPTT(),
			dir: "WhatsApp Audio", prefix: "AUD", ext: "opus"}
		if md.ptt {
			md.dir, md.prefix = "WhatsApp Voice Notes", "PTT"
		} else if strings.Contains(md.mime, "mp4") || strings.Contains(md.mime, "aac") {
			md.ext = "m4a"
		} else if strings.Contains(md.mime, "mpeg") {
			md.ext = "mp3"
		}
		return md
	case m.DocumentMessage != nil:
		x := m.GetDocumentMessage()
		return &media{mtype: 9, mime: x.GetMimetype(), url: x.GetURL(), directPath: x.GetDirectPath(), docCaption: x.GetCaption(),
			fileName: x.GetFileName(), key: x.GetMediaKey(), sha: x.GetFileSHA256(), encSha: x.GetFileEncSHA256(),
			length: x.GetFileLength(), pages: x.GetPageCount(), keyTS: x.GetMediaKeyTimestamp(), dir: "WhatsApp Documents"}
	case m.StickerMessage != nil:
		x := m.GetStickerMessage()
		return &media{mtype: 20, mime: x.GetMimetype(), url: x.GetURL(), directPath: x.GetDirectPath(),
			key: x.GetMediaKey(), sha: x.GetFileSHA256(), encSha: x.GetFileEncSHA256(), length: x.GetFileLength(),
			width: x.GetWidth(), height: x.GetHeight(), keyTS: x.GetMediaKeyTimestamp(), animated: x.GetIsAnimated(),
			dir: "WhatsApp Stickers", prefix: "STK", ext: "webp"}
	}
	return nil
}

// textOf returns (android message_type, text_data) for non-media messages, or ok=false when unsupported.
func textOf(m *waE2E.Message) (int, string, bool) {
	switch {
	case m.Conversation != nil:
		return 0, m.GetConversation(), true
	case m.ExtendedTextMessage != nil:
		return 0, m.GetExtendedTextMessage().GetText(), true
	case m.PollCreationMessage != nil || m.PollCreationMessageV2 != nil || m.PollCreationMessageV3 != nil:
		p := m.GetPollCreationMessage()
		if p == nil {
			p = m.GetPollCreationMessageV2()
		}
		if p == nil {
			p = m.GetPollCreationMessageV3()
		}
		t := "📊 " + p.GetName()
		for _, o := range p.GetOptions() {
			t += "\n• " + o.GetOptionName()
		}
		return 0, t, true
	case m.ContactMessage != nil:
		return 4, m.GetContactMessage().GetDisplayName(), true
	case m.TemplateMessage != nil:
		t := m.GetTemplateMessage()
		txt := t.GetHydratedTemplate().GetHydratedContentText()
		if txt == "" {
			txt = t.GetHydratedFourRowTemplate().GetHydratedContentText()
		}
		if f := t.GetHydratedTemplate().GetHydratedFooterText(); f != "" {
			txt += "\n" + f
		}
		return 0, txt, txt != ""
	case m.InteractiveMessage != nil:
		i := m.GetInteractiveMessage()
		txt := strings.TrimSpace(i.GetHeader().GetTitle() + "\n" + i.GetBody().GetText() + "\n" + i.GetFooter().GetText())
		return 0, txt, txt != ""
	case m.LocationMessage != nil:
		return 5, "", true
	}
	return 0, "", false
}

func ctxOf(m *waE2E.Message) *waE2E.ContextInfo {
	switch {
	case m.ExtendedTextMessage != nil:
		return m.GetExtendedTextMessage().GetContextInfo()
	case m.ImageMessage != nil:
		return m.GetImageMessage().GetContextInfo()
	case m.VideoMessage != nil:
		return m.GetVideoMessage().GetContextInfo()
	case m.PtvMessage != nil:
		return m.GetPtvMessage().GetContextInfo()
	case m.AudioMessage != nil:
		return m.GetAudioMessage().GetContextInfo()
	case m.DocumentMessage != nil:
		return m.GetDocumentMessage().GetContextInfo()
	case m.StickerMessage != nil:
		return m.GetStickerMessage().GetContextInfo()
	case m.ContactMessage != nil:
		return m.GetContactMessage().GetContextInfo()
	case m.LocationMessage != nil:
		return m.GetLocationMessage().GetContextInfo()
	}
	return nil
}

func isMe(j string) bool {
	b := bare(j)
	return b == myPN || b == myLID
}

// stage copies a downloaded media file into the staging tree and returns its WhatsApp-relative path.
func stage(md *media, id string, ts int64, fromMe bool) (string, int64) {
	src, ok := mediaFiles[id]
	if !ok {
		return "", 0
	}
	st, err := os.Stat(src)
	if err != nil {
		return "", 0
	}
	t := time.Unix(ts, 0)
	dir := "Media/" + md.dir
	if md.ptt {
		y, w := t.ISOWeek()
		dir += fmt.Sprintf("/%d%02d", y, w)
	} else if fromMe && md.mtype != 20 {
		dir += "/Sent"
	}
	var rel string
	if md.mtype == 9 {
		name := strings.Map(func(r rune) rune {
			if r == '/' || r == 0 {
				return '_'
			}
			return r
		}, md.fileName)
		if name == "" {
			name = "DOC-" + t.Format("20060102") + filepath.Ext(src)
		}
		ext := filepath.Ext(name)
		base := strings.TrimSuffix(name, ext)
		rel = dir + "/" + name
		for i := 1; usedNames[rel]; i++ {
			rel = fmt.Sprintf("%s/%s (%d)%s", dir, base, i, ext)
		}
	} else {
		ext := md.ext
		if e := strings.TrimPrefix(filepath.Ext(src), "."); e != "" && md.mtype != 2 {
			ext = e
		}
		for i := 5000; ; i++ {
			rel = fmt.Sprintf("%s/%s-%s-WA%04d.%s", dir, md.prefix, t.Format("20060102"), i, ext)
			if !usedNames[rel] {
				break
			}
		}
	}
	usedNames[rel] = true
	if !dry {
		dst := filepath.Join(stageDir, rel)
		os.MkdirAll(filepath.Dir(dst), 0o755)
		in, err := os.Open(src)
		must(err)
		out, err := os.Create(dst)
		must(err)
		_, err = io.Copy(out, in)
		must(err)
		in.Close()
		out.Close()
		os.Chtimes(dst, t, t)
	}
	stats["media files staged"]++
	return rel, st.Size()
}

func main() {
	if len(os.Args) < 6 {
		fmt.Println("usage: wamerge <data dir> <msgstore.db> <stage dir> <gap start YYYY-MM-DD> <phone media list> [-dry]")
		os.Exit(2)
	}
	dataDir, dbPath, stageDir = os.Args[1], os.Args[2], os.Args[3]
	gapDate := os.Args[4]
	dry = len(os.Args) > 6 && os.Args[6] == "-dry"
	// names already on the phone, so staged files never overwrite one
	if b, err := os.ReadFile(os.Args[5]); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				usedNames[l] = true
			}
		}
	} else {
		panic(err)
	}

	// own identity and LID<->PN map from the whatsmeow session(s)
	for i, sp := range []string{filepath.Join(dataDir, "session.db"), filepath.Join(dataDir, "old-session-1", "session.db")} {
		if _, err := os.Stat(sp); err != nil {
			continue
		}
		sdb, err := sql.Open("sqlite3", "file:"+sp+"?mode=ro")
		must(err)
		if i == 0 || myPN == "" {
			var pn, lid string
			if sdb.QueryRow(`SELECT jid, COALESCE(lid,'') FROM whatsmeow_device`).Scan(&pn, &lid) == nil {
				myPN, myLID = bare(pn), bare(lid)
			}
		}
		if rows, err := sdb.Query(`SELECT lid, pn FROM whatsmeow_lid_map`); err == nil {
			for rows.Next() {
				var l, p string
				rows.Scan(&l, &p)
				altJID[l+"@lid"], altJID[p+"@s.whatsapp.net"] = p+"@s.whatsapp.net", l+"@lid"
			}
			rows.Close()
		}
		sdb.Close()
	}

	// media files downloaded by wadump: <date>_<id>.<ext>
	filepath.Walk(filepath.Join(dataDir, "media"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			b := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))
			if i := strings.IndexByte(b, '_'); i > 0 {
				if j := strings.IndexByte(b[i+1:], '_'); j >= 0 {
					mediaFiles[b[i+1+j+1:]] = p
				}
			}
		}
		return nil
	})

	// all captured messages, best copy per key
	recs := map[string]*rec{}
	files, _ := filepath.Glob(filepath.Join(dataDir, "raw", "history", "*.pb"))
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		must(err)
		var hs waHistorySync.HistorySync
		must(proto.Unmarshal(b, &hs))
		for _, c := range hs.GetConversations() {
			cj := bare(c.GetID())
			if n := c.GetName(); n != "" {
				chatNames[cj] = n
			}
			for _, hm := range c.GetMessages() {
				wm := hm.GetMessage()
				if wm == nil || wm.GetKey().GetID() == "" {
					continue
				}
				k := fmt.Sprintf("%s|%s|%v", cj, wm.GetKey().GetID(), wm.GetKey().GetFromMe())
				if old, ok := recs[k]; ok && old.wm.Message != nil && wm.Message == nil {
					continue
				}
				if old, ok := recs[k]; ok && len(old.wm.GetReactions()) > len(wm.GetReactions()) {
					wm.Reactions = old.wm.GetReactions()
				}
				recs[k] = &rec{chat: cj, wm: wm}
			}
		}
	}

	// live messages received while linked
	if f, err := os.Open(filepath.Join(dataDir, "raw", "live.jsonl")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
		for sc.Scan() {
			var lr struct {
				Info struct {
					Chat, Sender, ID, PushName string
					IsFromMe, IsGroup          bool
					Timestamp                  time.Time
				} `json:"info"`
				Message json.RawMessage `json:"message"`
			}
			if json.Unmarshal(sc.Bytes(), &lr) != nil {
				continue
			}
			var m waE2E.Message
			if protojson.Unmarshal(lr.Message, &m) != nil {
				continue
			}
			cj := bare(lr.Info.Chat)
			k := fmt.Sprintf("%s|%s|%v", cj, lr.Info.ID, lr.Info.IsFromMe)
			if _, ok := recs[k]; ok {
				continue
			}
			wm := &waWeb.WebMessageInfo{
				Key:              &waCommon.MessageKey{RemoteJID: proto.String(cj), FromMe: proto.Bool(lr.Info.IsFromMe), ID: proto.String(lr.Info.ID)},
				Message:          &m,
				MessageTimestamp: proto.Uint64(uint64(lr.Info.Timestamp.Unix())),
			}
			if lr.Info.IsGroup {
				wm.Participant = proto.String(lr.Info.Sender)
			}
			recs[k] = &rec{chat: cj, wm: wm}
			stats["live messages read"]++
		}
		f.Close()
	}

	conn, err := sql.Open("sqlite3", "file:"+dbPath+"?_busy_timeout=10000")
	must(err)
	defer conn.Close()
	db, err = conn.Begin()
	must(err)

	rows, err := db.Query(`SELECT _id, raw_string FROM jid WHERE raw_string IS NOT NULL`)
	must(err)
	for rows.Next() {
		var id int64
		var raw string
		rows.Scan(&id, &raw)
		jidRow[raw] = id
	}
	rows.Close()
	rows, err = db.Query(`SELECT l.raw_string, p.raw_string FROM jid_map m JOIN jid l ON l._id=m.lid_row_id JOIN jid p ON p._id=m.jid_row_id`)
	must(err)
	for rows.Next() {
		var l, p string
		rows.Scan(&l, &p)
		altJID[l], altJID[p] = p, l
	}
	rows.Close()
	rows, err = db.Query(`SELECT _id, jid_row_id FROM chat`)
	must(err)
	for rows.Next() {
		var c, j int64
		rows.Scan(&c, &j)
		chatRow[j] = c
	}
	rows.Close()
	existing := map[string]int64{} // key_id -> message._id
	msgTS := map[string]int64{}    // key_id -> timestamp ms
	rows, err = db.Query(`SELECT _id, key_id, timestamp FROM message`)
	must(err)
	for rows.Next() {
		var id, ts int64
		var k string
		rows.Scan(&id, &k, &ts)
		existing[k], msgTS[k] = id, ts
	}
	rows.Close()
	addons := map[string]bool{}
	rows, err = db.Query(`SELECT key_id FROM message_add_on`)
	must(err)
	for rows.Next() {
		var k string
		rows.Scan(&k)
		addons[k] = true
	}
	rows.Close()

	// cutoff: newest backup message before the gap that starts on gapDate
	var cutoffMS, lastSort int64
	gap, err := time.Parse("2006-01-02", gapDate)
	must(err)
	must(db.QueryRow(`SELECT MAX(timestamp) FROM message WHERE timestamp < ?`, gap.UnixMilli()).Scan(&cutoffMS))
	must(db.QueryRow(`SELECT MAX(sort_id) FROM message WHERE timestamp <= ?`, cutoffMS).Scan(&lastSort))
	fmt.Printf("cutoff %s, last sort_id %d\n", time.UnixMilli(cutoffMS).UTC().Format(time.RFC3339), lastSort)

	// pick the messages to add, oldest first
	var todo []*rec
	for _, r := range recs {
		ts := int64(r.wm.GetMessageTimestamp())
		if ts*1000 <= cutoffMS || r.chat == "status@broadcast" {
			continue
		}
		if _, ok := existing[r.wm.GetKey().GetID()]; ok {
			stats["skipped: already in backup"]++
			continue
		}
		todo = append(todo, r)
	}
	sort.Slice(todo, func(i, j int) bool {
		a, b := todo[i].wm.GetMessageTimestamp(), todo[j].wm.GetMessageTimestamp()
		if a != b {
			return a < b
		}
		return todo[i].wm.GetKey().GetID() < todo[j].wm.GetKey().GetID()
	})

	type edit struct {
		id   string
		text string
	}
	var edits []edit
	var revokes []string
	var withReactions []*rec

	insMsg, err := db.Prepare(`INSERT INTO message(chat_row_id,from_me,key_id,sender_jid_row_id,status,broadcast,recipient_count,
		origination_flags,origin,timestamp,received_timestamp,receipt_server_timestamp,message_type,text_data,starred,lookup_tables,
		message_add_on_flags,view_mode,sort_id) VALUES(?,?,?,?,?,0,?,0,?,?,0,-1,?,?,?,?,0,0,0)`)
	must(err)

	for _, r := range todo {
		wm := r.wm
		k := wm.GetKey()
		ts := int64(wm.GetMessageTimestamp())
		m := unwrap(wm.GetMessage())
		if m == nil {
			if wm.GetMessageStubType() != 0 {
				stats["skipped: system notice"]++
			} else {
				stats["skipped: empty"]++
			}
			continue
		}
		if pm := m.GetProtocolMessage(); pm != nil {
			switch pm.GetType() {
			case waE2E.ProtocolMessage_MESSAGE_EDIT:
				em := unwrap(pm.GetEditedMessage())
				if em != nil {
					t := em.GetConversation()
					if t == "" {
						t = em.GetExtendedTextMessage().GetText()
					}
					if t == "" {
						t = em.GetImageMessage().GetCaption() + em.GetVideoMessage().GetCaption()
					}
					if t != "" {
						edits = append(edits, edit{pm.GetKey().GetID(), t})
					}
				}
			case waE2E.ProtocolMessage_REVOKE:
				revokes = append(revokes, pm.GetKey().GetID())
			}
			stats["skipped: protocol"]++
			continue
		}
		md := mediaOf(m)
		mtype, text := 0, ""
		if md != nil {
			mtype, text = md.mtype, md.caption
		} else {
			var ok bool
			mtype, text, ok = textOf(m)
			if !ok {
				if m.CallLogMesssage != nil {
					stats["skipped: call log"]++
				} else if m.ReactionMessage != nil || m.EncReactionMessage != nil {
					stats["skipped: reaction as message"]++
				} else {
					stats["skipped: unsupported "+kindName(m)]++
				}
				continue
			}
		}
		chat := getChat(r.chat, ts)
		group := strings.HasSuffix(r.chat, "@g.us")
		var sender int64
		status, recip := 0, 0
		if k.GetFromMe() {
			status = 13
		} else if group {
			p := wm.GetParticipant()
			if p == "" {
				p = k.GetParticipant()
			}
			if p != "" {
				sender = getJID(p)
			}
			recip = -1
		}
		origin := 0
		if md != nil && md.ptt {
			origin = 1
		}
		starred := 0
		if wm.GetStarred() {
			starred = 1
		}
		lookup := 0
		ctx := ctxOf(m)
		if ctx.GetStanzaID() != "" && ctx.GetQuotedMessage() != nil {
			lookup |= 2
		}
		if dry {
			stats[fmt.Sprintf("add type %d", mtype)]++
			continue
		}
		res, err := insMsg.Exec(chat, b2i(k.GetFromMe()), k.GetID(), sender, status, recip, origin, ts*1000, mtype, nullStr(text), starred, lookup)
		must(err)
		rowID, _ := res.LastInsertId()
		existing[k.GetID()], msgTS[k.GetID()] = rowID, ts*1000
		stats[fmt.Sprintf("add type %d", mtype)]++

		if md != nil {
			rel, size := stage(md, k.GetID(), ts, k.GetFromMe())
			msgURL := md.url
			if msgURL == "" && md.directPath != "" {
				msgURL = "https://mmg.whatsapp.net" + md.directPath
			}
			mediaName := md.fileName
			if md.ptt && rel != "" {
				mediaName = filepath.Base(rel)
			}
			_, err := db.Exec(`INSERT INTO message_media(message_row_id,chat_row_id,autotransfer_retry_enabled,media_job_uuid,transferred,transcoded,
				file_path,file_size,suspicious_content,trim_from,trim_to,face_x,face_y,media_key,media_key_timestamp,width,height,has_streaming_sidecar,
				gif_attribution,thumbnail_height_width_ratio,direct_path,first_scan_sidecar,first_scan_length,message_url,mime_type,file_length,media_name,
				file_hash,media_duration,page_count,enc_file_hash,is_animated_sticker,mute_video,media_caption,media_source_type,media_transcode_quality,
				media_key_domain,premium_message) VALUES(?,?,1,?,?,0,?,?,0,0,0,0,0,?,?,?,?,0,0,0.0,?,?,?,?,?,?,?,?,?,?,?,?,0,?,-1,0,0,0)`,
				rowID, chat, uuid(), b2i(rel != ""), nullStr(rel), size, nullBytes(md.key), md.keyTS*1000, md.width, md.height,
				nullStr(md.directPath), nullBytes(md.sidecar), md.scanLen, nullStr(msgURL), nullStr(md.mime), md.length, nullStr(mediaName),
				b64(md.sha), md.seconds, md.pages, b64(md.encSha), b2i(md.animated), nullStr(md.docCaption))
			must(err)
			if rel == "" {
				stats["media without file"]++
			}
		}
		if mtype == 4 {
			db.Exec(`INSERT INTO message_vcard(message_row_id,vcard) VALUES(?,?)`, rowID, m.GetContactMessage().GetVcard())
		}
		if mtype == 5 {
			l := m.GetLocationMessage()
			db.Exec(`INSERT INTO message_location(message_row_id,chat_row_id,latitude,longitude,place_name,place_address,url,map_download_status)
				VALUES(?,?,?,?,?,?,?,2)`, rowID, chat, l.GetDegreesLatitude(), l.GetDegreesLongitude(), nullStr(l.GetName()), nullStr(l.GetAddress()), nullStr(l.GetURL()))
		}
		if x := m.GetExtendedTextMessage(); x != nil && x.GetMatchedText() != "" {
			db.Exec(`INSERT INTO message_text(message_row_id,description,page_title,url,preview_type) VALUES(?,?,?,?,0)`,
				rowID, nullStr(x.GetDescription()), nullStr(x.GetTitle()), x.GetMatchedText())
			if len(x.GetJPEGThumbnail()) > 0 {
				db.Exec(`INSERT INTO message_thumbnail(message_row_id,thumbnail) VALUES(?,?)`, rowID, x.GetJPEGThumbnail())
			}
		}
		if lookup&2 != 0 {
			insertQuote(rowID, chat, ctx, msgTS)
		}
		if len(wm.GetReactions()) > 0 {
			withReactions = append(withReactions, r)
		}
	}

	// reactions on any message we hold (new or already in the backup)
	for _, r := range recs {
		if len(r.wm.GetReactions()) > 0 && int64(r.wm.GetMessageTimestamp())*1000 <= cutoffMS {
			withReactions = append(withReactions, r)
		}
	}
	for _, r := range withReactions {
		parent, ok := existing[r.wm.GetKey().GetID()]
		if !ok || dry {
			continue
		}
		chat := getChat(r.chat, int64(r.wm.GetMessageTimestamp()))
		for _, re := range r.wm.GetReactions() {
			rk := re.GetKey()
			if re.GetText() == "" || rk.GetID() == "" || addons[rk.GetID()] {
				continue
			}
			addons[rk.GetID()] = true
			var sender int64 = -1
			if !rk.GetFromMe() {
				p := re.GetKey().GetParticipant()
				if p == "" {
					p = rk.GetRemoteJID()
				}
				if p == "" {
					p = r.chat
				}
				sender = getJID(p)
			}
			res, err := db.Exec(`INSERT INTO message_add_on(chat_row_id,from_me,key_id,sender_jid_row_id,parent_message_row_id,timestamp,status,
				message_add_on_type,received_timestamp,server_timestamp,expiry_type) VALUES(?,?,?,?,?,?,17,56,0,0,0)`,
				chat, b2i(rk.GetFromMe()), rk.GetID(), sender, parent, msgTS[r.wm.GetKey().GetID()])
			must(err)
			aid, _ := res.LastInsertId()
			db.Exec(`INSERT INTO message_add_on_reaction(message_add_on_row_id,reaction,sender_timestamp) VALUES(?,?,?)`, aid, re.GetText(), re.GetSenderTimestampMS())
			db.Exec(`UPDATE message SET message_add_on_flags=1 WHERE _id=?`, parent)
			stats["reactions added"]++
		}
	}

	if !dry {
		for _, e := range edits {
			if id, ok := existing[e.id]; ok && msgTS[e.id] > cutoffMS {
				db.Exec(`UPDATE message SET text_data=? WHERE _id=? AND message_type IN (0,1,3,13)`, e.text, id)
				stats["edits applied"]++
			}
		}
		for _, id := range revokes {
			if rid, ok := existing[id]; ok && msgTS[id] > cutoffMS {
				db.Exec(`UPDATE message SET message_type=15, text_data=NULL WHERE _id=?`, rid)
				db.Exec(`DELETE FROM message_media WHERE message_row_id=?`, rid)
				stats["deletions applied"]++
			}
		}
		fixOrder(cutoffMS, lastSort)
		must(db.Commit())
	} else {
		db.Rollback()
	}
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%-40s %d\n", k, stats[k])
	}
}

func kindName(m *waE2E.Message) string {
	var names []string
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		names = append(names, string(fd.Name()))
		return true
	})
	return strings.Join(names, "+")
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func quotedType(q *waE2E.Message) (int, string) {
	q = unwrap(q)
	if q == nil {
		return 0, ""
	}
	if md := mediaOf(q); md != nil {
		t := md.caption
		if md.mtype == 9 {
			t = md.docCaption
		}
		return md.mtype, t
	}
	if t, s, ok := textOf(q); ok {
		return t, s
	}
	return 0, ""
}

func insertQuote(rowID, chat int64, ctx *waE2E.ContextInfo, msgTS map[string]int64) {
	q := ctx.GetQuotedMessage()
	qt, qtext := quotedType(q)
	fromMe := isMe(ctx.GetParticipant())
	var sender int64
	if !fromMe && ctx.GetParticipant() != "" {
		sender = getJID(ctx.GetParticipant())
	}
	pchat := chat
	if rj := ctx.GetRemoteJID(); rj != "" && !strings.HasSuffix(rj, "status@broadcast") {
		pchat = getChat(rj, 0)
	}
	_, err := db.Exec(`INSERT INTO message_quoted(message_row_id,chat_row_id,parent_message_chat_row_id,from_me,sender_jid_row_id,key_id,
		timestamp,message_type,origin,text_data,lookup_tables,quoted_source,quoted_type) VALUES(?,?,?,?,?,?,?,?,0,?,0,NULL,NULL)`,
		rowID, chat, pchat, b2i(fromMe), sender, ctx.GetStanzaID(), msgTS[ctx.GetStanzaID()], qt, nullStr(qtext))
	must(err)
	if md := mediaOf(unwrap(q)); md != nil {
		var thumb []byte
		uq := unwrap(q)
		switch {
		case uq.ImageMessage != nil:
			thumb = uq.GetImageMessage().GetJPEGThumbnail()
		case uq.VideoMessage != nil:
			thumb = uq.GetVideoMessage().GetJPEGThumbnail()
		case uq.DocumentMessage != nil:
			thumb = uq.GetDocumentMessage().GetJPEGThumbnail()
		}
		db.Exec(`INSERT INTO message_quoted_media(message_row_id,transferred,file_size,media_key,media_key_timestamp,width,height,direct_path,
			message_url,mime_type,file_length,media_name,file_hash,media_duration,page_count,enc_file_hash,thumbnail,media_caption)
			VALUES(?,0,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, rowID, nullBytes(md.key), md.keyTS*1000, md.width, md.height, nullStr(md.directPath),
			nullStr(md.url), nullStr(md.mime), md.length, nullStr(md.fileName), b64(md.sha), md.seconds, md.pages, b64(md.encSha),
			nullBytes(thumb), nullStr(md.docCaption))
	}
	stats["quotes added"]++
}

// fixOrder renumbers sort_id for everything after the cutoff in timestamp
// order and points each chat at its newest message.
func fixOrder(cutoffMS, lastSort int64) {
	rows, err := db.Query(`SELECT _id, sort_id FROM message WHERE timestamp > ? ORDER BY timestamp, _id`, cutoffMS)
	must(err)
	type pair struct{ id, old int64 }
	var ps []pair
	for rows.Next() {
		var p pair
		rows.Scan(&p.id, &p.old)
		ps = append(ps, p)
	}
	rows.Close()
	// move out of the way first (sort_id may be indexed)
	must2(db.Exec(`UPDATE message SET sort_id = -sort_id - 1 WHERE timestamp > ?`, cutoffMS))
	for i, p := range ps {
		must2(db.Exec(`UPDATE message SET sort_id=? WHERE _id=?`, lastSort+1+int64(i), p.id))
	}
	// newest message per chat for every chat touched after the cutoff
	rows, err = db.Query(`SELECT chat_row_id, _id, sort_id, timestamp FROM message m WHERE timestamp > ? AND sort_id =
		(SELECT MAX(sort_id) FROM message WHERE chat_row_id=m.chat_row_id)`, cutoffMS)
	must(err)
	type last struct{ chat, id, sort, ts int64 }
	var ls []last
	for rows.Next() {
		var l last
		rows.Scan(&l.chat, &l.id, &l.sort, &l.ts)
		ls = append(ls, l)
	}
	rows.Close()
	for _, l := range ls {
		must2(db.Exec(`UPDATE chat SET display_message_row_id=?, last_message_row_id=?, last_read_message_row_id=?,
			last_read_receipt_sent_message_row_id=?, last_important_message_row_id=?, sort_timestamp=?,
			display_message_sort_id=?, last_message_sort_id=?, last_read_message_sort_id=?, last_read_receipt_sent_message_sort_id=?,
			unseen_message_count=0, unseen_row_count=0, unseen_important_message_count=0, unseen_earliest_message_received_time=0
			WHERE _id=?`, l.id, l.id, l.id, l.id, l.id, l.ts, l.sort, l.sort, l.sort, l.sort, l.chat))
	}
	stats["chats updated"] = len(ls)
}

func must2(_ sql.Result, err error) { must(err) }

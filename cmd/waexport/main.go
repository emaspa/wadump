// waexport: turns the raw history-sync packets saved by wadump into
// archive.db (SQLite) plus a browsable HTML page and a .txt file per chat.
// Read-only on the raw data; safe to re-run at any time.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type msgRow struct {
	chat, id, sender string
	fromMe           bool
	ts               int64
	kind, text       string
	mediaName, mime  string
	raw              []byte // marshalled waE2E.Message
	stub             string
	quotedID         string
}

type chatInfo struct {
	jid, name string
	lastTS    int64
}

var (
	names    = map[string]string{} // jid -> best known name
	lidToPN  = map[string]string{}
	chats    = map[string]*chatInfo{}
	messages = map[string]*msgRow{} // chat|id|fromMe -> row
)

func setName(jid, name string, strong bool) {
	if jid == "" || name == "" {
		return
	}
	if _, ok := names[jid]; !ok || strong {
		names[jid] = name
	}
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
		default:
			return m
		}
	}
	return m
}

func describe(m *waE2E.Message) (kind, text, mediaName, mime, quoted string) {
	m = unwrap(m)
	if m == nil {
		return "empty", "", "", "", ""
	}
	var ctx *waE2E.ContextInfo
	switch {
	case m.Conversation != nil:
		kind, text = "text", m.GetConversation()
	case m.ExtendedTextMessage != nil:
		kind, text, ctx = "text", m.GetExtendedTextMessage().GetText(), m.GetExtendedTextMessage().GetContextInfo()
	case m.ImageMessage != nil:
		x := m.GetImageMessage()
		kind, text, mime, ctx = "image", x.GetCaption(), x.GetMimetype(), x.GetContextInfo()
	case m.VideoMessage != nil:
		x := m.GetVideoMessage()
		kind, text, mime, ctx = "video", x.GetCaption(), x.GetMimetype(), x.GetContextInfo()
		if x.GetGifPlayback() {
			kind = "gif"
		}
	case m.PtvMessage != nil:
		x := m.GetPtvMessage()
		kind, mime, ctx = "video-note", x.GetMimetype(), x.GetContextInfo()
	case m.AudioMessage != nil:
		x := m.GetAudioMessage()
		kind, mime, ctx = "audio", x.GetMimetype(), x.GetContextInfo()
		if x.GetPTT() {
			kind = "voice"
		}
		text = fmt.Sprintf("(%ds)", x.GetSeconds())
	case m.DocumentMessage != nil:
		x := m.GetDocumentMessage()
		kind, text, mediaName, mime, ctx = "document", x.GetCaption(), x.GetFileName(), x.GetMimetype(), x.GetContextInfo()
	case m.StickerMessage != nil:
		kind, mime = "sticker", m.GetStickerMessage().GetMimetype()
	case m.ContactMessage != nil:
		kind, text = "contact", m.GetContactMessage().GetDisplayName()+"\n"+m.GetContactMessage().GetVcard()
	case m.ContactsArrayMessage != nil:
		kind = "contacts"
		for _, c := range m.GetContactsArrayMessage().GetContacts() {
			text += c.GetDisplayName() + "; "
		}
	case m.LocationMessage != nil:
		x := m.GetLocationMessage()
		kind, text = "location", fmt.Sprintf("%s %s https://maps.google.com/?q=%f,%f", x.GetName(), x.GetAddress(), x.GetDegreesLatitude(), x.GetDegreesLongitude())
	case m.LiveLocationMessage != nil:
		x := m.GetLiveLocationMessage()
		kind, text = "live-location", fmt.Sprintf("https://maps.google.com/?q=%f,%f", x.GetDegreesLatitude(), x.GetDegreesLongitude())
	case m.ReactionMessage != nil:
		kind, text, quoted = "reaction", m.GetReactionMessage().GetText(), m.GetReactionMessage().GetKey().GetID()
	case m.PollCreationMessage != nil || m.PollCreationMessageV2 != nil || m.PollCreationMessageV3 != nil:
		p := m.GetPollCreationMessage()
		if p == nil {
			p = m.GetPollCreationMessageV2()
		}
		if p == nil {
			p = m.GetPollCreationMessageV3()
		}
		kind, text = "poll", p.GetName()
		for _, o := range p.GetOptions() {
			text += "\n  - " + o.GetOptionName()
		}
	case m.ProtocolMessage != nil:
		kind = "protocol:" + m.GetProtocolMessage().GetType().String()
	case m.CallLogMesssage != nil:
		kind = "call"
	default:
		kind = "other"
		if b, err := protojson.Marshal(m); err == nil {
			text = string(b)
			if len(text) > 500 {
				text = text[:500] + "..."
			}
		}
	}
	if ctx != nil {
		quoted = ctx.GetStanzaID()
	}
	return
}

func addMsg(chat string, hm *waHistorySync.HistorySyncMsg) {
	wm := hm.GetMessage()
	if wm == nil {
		return
	}
	k := wm.GetKey()
	r := &msgRow{chat: chat, id: k.GetID(), fromMe: k.GetFromMe(), ts: int64(wm.GetMessageTimestamp())}
	r.sender = wm.GetParticipant()
	if r.sender == "" {
		r.sender = k.GetParticipant()
	}
	if r.sender == "" && !r.fromMe {
		r.sender = chat
	}
	setName(r.sender, wm.GetPushName(), false)
	if wm.Message != nil {
		r.kind, r.text, r.mediaName, r.mime, r.quotedID = describe(wm.GetMessage())
		r.raw, _ = proto.Marshal(wm.GetMessage())
	} else if wm.GetMessageStubType() != 0 {
		r.kind = "system"
		r.stub = wm.GetMessageStubType().String()
		r.text = r.stub + " " + strings.Join(wm.GetMessageStubParameters(), ", ")
	} else {
		r.kind = "empty"
	}
	store(r)
}

func store(r *msgRow) {
	key := fmt.Sprintf("%s|%s|%v", r.chat, r.id, r.fromMe)
	if old, ok := messages[key]; ok && old.raw != nil && r.raw == nil {
		return
	}
	messages[key] = r
	c := chats[r.chat]
	if c == nil {
		c = &chatInfo{jid: r.chat}
		chats[r.chat] = c
	}
	if r.ts > c.lastTS {
		c.lastTS = r.ts
	}
}

func loadSessionDB(path string) {
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=10000")
	if err != nil {
		return
	}
	defer db.Close()
	if rows, err := db.Query(`SELECT their_jid, COALESCE(full_name,''), COALESCE(first_name,''), COALESCE(push_name,''), COALESCE(business_name,'') FROM whatsmeow_contacts`); err == nil {
		for rows.Next() {
			var j, full, first, push, biz string
			rows.Scan(&j, &full, &first, &push, &biz)
			for _, n := range []string{full, first, biz, push} {
				if n != "" {
					setName(j, n, true)
					break
				}
			}
		}
		rows.Close()
	}
	if rows, err := db.Query(`SELECT lid, pn FROM whatsmeow_lid_map`); err == nil {
		for rows.Next() {
			var l, p string
			rows.Scan(&l, &p)
			lidToPN[l+"@lid"] = p + "@s.whatsapp.net"
		}
		rows.Close()
	}
}

func displayName(jid string) string {
	if n := names[jid]; n != "" {
		return n
	}
	if pn, ok := lidToPN[jid]; ok {
		if n := names[pn]; n != "" {
			return n
		}
		jid = pn
	}
	if i := strings.IndexByte(jid, '@'); i > 0 && strings.HasSuffix(jid, "@s.whatsapp.net") {
		return "+" + jid[:i]
	}
	return jid
}

var unsafeRe = regexp.MustCompile(`[^\p{L}\p{N} ._+-]+`)

func fileName(c *chatInfo) string {
	n := strings.TrimSpace(unsafeRe.ReplaceAllString(displayName(c.jid), "_"))
	if len(n) > 60 {
		n = n[:60]
	}
	user := c.jid
	if i := strings.IndexByte(user, '@'); i > 0 {
		user = user[:i]
	}
	return n + " - " + user
}

func main() {
	home, _ := os.UserHomeDir()
	dataDir := filepath.Join(home, "whatsapp", "data")
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}
	out := filepath.Join(dataDir, "export")
	os.MkdirAll(filepath.Join(out, "chats"), 0o700)

	loadSessionDB(filepath.Join(dataDir, "session.db"))

	files, _ := filepath.Glob(filepath.Join(dataDir, "raw", "history", "*.pb"))
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var hs waHistorySync.HistorySync
		if err := proto.Unmarshal(b, &hs); err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", f, err)
			continue
		}
		for _, p := range hs.GetPushnames() {
			setName(p.GetID(), p.GetPushname(), false)
		}
		for _, c := range hs.GetConversations() {
			ci := chats[c.GetID()]
			if ci == nil {
				ci = &chatInfo{jid: c.GetID()}
				chats[c.GetID()] = ci
			}
			if n := c.GetName(); n != "" {
				ci.name = n
				setName(c.GetID(), n, true)
			} else if n := c.GetDisplayName(); n != "" {
				setName(c.GetID(), n, false)
			}
			if c.GetPnJID() != "" && strings.HasSuffix(c.GetID(), "@lid") {
				lidToPN[c.GetID()] = c.GetPnJID()
			}
			for _, m := range c.GetMessages() {
				addMsg(c.GetID(), m)
			}
		}
	}

	// Live messages received while connected.
	if f, err := os.Open(filepath.Join(dataDir, "raw", "live.jsonl")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			var rec struct {
				Info struct {
					Chat, Sender, ID, PushName string
					IsFromMe                   bool
					Timestamp                  time.Time
				} `json:"info"`
				Message json.RawMessage `json:"message"`
			}
			if json.Unmarshal(sc.Bytes(), &rec) != nil {
				continue
			}
			var m waE2E.Message
			if protojson.Unmarshal(rec.Message, &m) != nil {
				continue
			}
			r := &msgRow{chat: rec.Info.Chat, id: rec.Info.ID, fromMe: rec.Info.IsFromMe, ts: rec.Info.Timestamp.Unix()}
			if !r.fromMe {
				r.sender = rec.Info.Sender
				setName(r.sender, rec.Info.PushName, false)
			}
			r.kind, r.text, r.mediaName, r.mime, r.quotedID = describe(&m)
			r.raw, _ = proto.Marshal(&m)
			store(r)
		}
		f.Close()
	}

	// Strip :device suffixes and dedupe sender JIDs for names.
	writeDB(filepath.Join(out, "archive.db"))
	writeChats(out)
}

func writeDB(path string) {
	os.Remove(path)
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Exec(`CREATE TABLE chats(jid TEXT PRIMARY KEY, name TEXT, pn_jid TEXT, last_ts INTEGER, n_messages INTEGER);
CREATE TABLE messages(chat TEXT, id TEXT, from_me INTEGER, sender TEXT, sender_name TEXT, ts INTEGER, kind TEXT, text TEXT, media_name TEXT, mime TEXT, quoted_id TEXT, raw BLOB, PRIMARY KEY(chat,id,from_me));
CREATE INDEX m_chat_ts ON messages(chat, ts);`)
	tx, _ := db.Begin()
	st, _ := tx.Prepare(`INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`)
	count := map[string]int{}
	for _, r := range messages {
		count[r.chat]++
		sn := "me"
		if !r.fromMe {
			sn = displayName(r.sender)
		}
		st.Exec(r.chat, r.id, r.fromMe, r.sender, sn, r.ts, r.kind, r.text, r.mediaName, r.mime, r.quotedID, r.raw)
	}
	st.Close()
	cs, _ := tx.Prepare(`INSERT INTO chats VALUES(?,?,?,?,?)`)
	for _, c := range chats {
		cs.Exec(c.jid, displayName(c.jid), lidToPN[c.jid], c.lastTS, count[c.jid])
	}
	cs.Close()
	tx.Commit()
	fmt.Printf("archive.db: %d chats, %d messages\n", len(chats), len(messages))
}

func writeChats(out string) {
	byChat := map[string][]*msgRow{}
	for _, r := range messages {
		byChat[r.chat] = append(byChat[r.chat], r)
	}
	list := make([]*chatInfo, 0, len(chats))
	for _, c := range chats {
		list = append(list, c)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].lastTS > list[j].lastTS })

	var idx strings.Builder
	idx.WriteString(pageHead("WhatsApp archive"))
	idx.WriteString("<h1>WhatsApp archive</h1><p class=meta>Generated " + time.Now().Format("2006-01-02 15:04") + "</p><input id=q placeholder='Filter chats' oninput=\"for(const li of document.querySelectorAll('li'))li.style.display=li.textContent.toLowerCase().includes(this.value.toLowerCase())?'':'none'\"><ul>")
	for _, c := range list {
		ms := byChat[c.jid]
		sort.Slice(ms, func(i, j int) bool { return ms[i].ts < ms[j].ts })
		fn := fileName(c)
		var first, last string
		if len(ms) > 0 {
			first = time.Unix(ms[0].ts, 0).Format("2006-01-02")
			last = time.Unix(ms[len(ms)-1].ts, 0).Format("2006-01-02")
		}
		fmt.Fprintf(&idx, "<li><a href=\"chats/%s.html\">%s</a> <span class=meta>%d msgs · %s → %s</span></li>", urlEsc(fn), html.EscapeString(displayName(c.jid)), len(ms), first, last)

		var t, h strings.Builder
		h.WriteString(pageHead(displayName(c.jid)))
		h.WriteString("<p><a href=\"../index.html\">← all chats</a></p><h1>" + html.EscapeString(displayName(c.jid)) + "</h1><p class=meta>" + html.EscapeString(c.jid) + "</p>")
		day := ""
		for _, m := range ms {
			if strings.HasPrefix(m.kind, "protocol:") || m.kind == "empty" {
				continue
			}
			tm := time.Unix(m.ts, 0)
			who := "me"
			if !m.fromMe {
				who = displayName(m.sender)
			}
			body := m.text
			if m.kind != "text" && m.kind != "system" {
				tag := "[" + m.kind
				if m.mediaName != "" {
					tag += ": " + m.mediaName
				}
				tag += "]"
				body = strings.TrimSpace(tag + " " + body)
			}
			fmt.Fprintf(&t, "%s - %s: %s\n", tm.Format("2006-01-02 15:04"), who, body)
			if d := tm.Format("Monday 2 January 2006"); d != day {
				day = d
				h.WriteString("<div class=day>" + d + "</div>")
			}
			cls := "in"
			if m.fromMe {
				cls = "out"
			}
			if m.kind == "system" {
				cls = "sys"
			}
			fmt.Fprintf(&h, "<div class=\"m %s\" id=\"%s\"><b>%s</b> <span class=meta>%s</span><div>%s</div></div>",
				cls, html.EscapeString(m.id), html.EscapeString(who), tm.Format("15:04"), strings.ReplaceAll(html.EscapeString(body), "\n", "<br>"))
		}
		h.WriteString("</body></html>")
		os.WriteFile(filepath.Join(out, "chats", fn+".txt"), []byte(t.String()), 0o600)
		os.WriteFile(filepath.Join(out, "chats", fn+".html"), []byte(h.String()), 0o600)
	}
	idx.WriteString("</ul></body></html>")
	os.WriteFile(filepath.Join(out, "index.html"), []byte(idx.String()), 0o600)
}

func urlEsc(s string) string {
	r := strings.NewReplacer("%", "%25", " ", "%20", "#", "%23", "?", "%3F", "\"", "%22")
	return r.Replace(s)
}

func pageHead(title string) string {
	return `<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>` + html.EscapeString(title) + `</title><style>
:root{--bg:#f4f1ea;--fg:#1d1d1b;--in:#fff;--out:#dcf3d0;--sys:#e8e4da;--meta:#6b675e}
@media(prefers-color-scheme:dark){:root{--bg:#111512;--fg:#e6e6e2;--in:#1f2621;--out:#1f4031;--sys:#2a2e2b;--meta:#9a9e98}}
body{background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,sans-serif;max-width:820px;margin:0 auto;padding:16px}
a{color:inherit}.meta{color:var(--meta);font-size:12px}ul{list-style:none;padding:0}li{padding:6px 0;border-bottom:1px solid var(--sys)}
input{width:100%;padding:8px;font:inherit;box-sizing:border-box}
.day{text-align:center;color:var(--meta);font-size:12px;margin:18px 0 6px}
.m{background:var(--in);border-radius:8px;padding:6px 10px;margin:4px 0;max-width:80%;overflow-wrap:anywhere}
.m.out{background:var(--out);margin-left:auto}.m.sys{background:var(--sys);margin:4px auto;font-size:13px}
</style></head><body>`
}

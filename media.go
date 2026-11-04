package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type mediaItem struct {
	chat, sender, id string
	fromMe           bool
	ts               int64
	msg              *waE2E.Message // unwrapped, holds exactly one media sub-message
	kind, ext        string
}

var (
	retryMu      sync.Mutex
	retryWaiters = map[string]chan *events.MediaRetry{}
)

func handleMediaRetry(v *events.MediaRetry) {
	retryMu.Lock()
	ch := retryWaiters[v.MessageID]
	retryMu.Unlock()
	if ch != nil {
		select {
		case ch <- v:
		default:
		}
	}
}

func unwrapMsg(m *waE2E.Message) *waE2E.Message {
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

var extByMime = map[string]string{
	"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "image/heic": "heic",
	"video/mp4": "mp4", "video/quicktime": "mov", "video/3gpp": "3gp",
	"audio/ogg": "ogg", "audio/mpeg": "mp3", "audio/mp4": "m4a", "audio/aac": "aac", "audio/amr": "amr",
	"application/pdf": "pdf",
}

// mediaOf returns a message holding only the downloadable part, plus kind/ext.
func mediaOf(m *waE2E.Message) (*waE2E.Message, string, string) {
	m = unwrapMsg(m)
	mimeExt := func(mt, def string) string {
		mt = strings.TrimSpace(strings.SplitN(mt, ";", 2)[0])
		if e, ok := extByMime[mt]; ok {
			return e
		}
		return def
	}
	switch {
	case m.GetImageMessage().GetMediaKey() != nil:
		return &waE2E.Message{ImageMessage: m.GetImageMessage()}, "image", mimeExt(m.GetImageMessage().GetMimetype(), "jpg")
	case m.GetVideoMessage().GetMediaKey() != nil:
		return &waE2E.Message{VideoMessage: m.GetVideoMessage()}, "video", mimeExt(m.GetVideoMessage().GetMimetype(), "mp4")
	case m.GetPtvMessage().GetMediaKey() != nil:
		return &waE2E.Message{VideoMessage: m.GetPtvMessage()}, "video-note", "mp4"
	case m.GetAudioMessage().GetMediaKey() != nil:
		return &waE2E.Message{AudioMessage: m.GetAudioMessage()}, "audio", mimeExt(m.GetAudioMessage().GetMimetype(), "ogg")
	case m.GetDocumentMessage().GetMediaKey() != nil:
		d := m.GetDocumentMessage()
		ext := strings.TrimPrefix(filepath.Ext(d.GetFileName()), ".")
		if ext == "" {
			ext = mimeExt(d.GetMimetype(), "bin")
		}
		return &waE2E.Message{DocumentMessage: d}, "document", ext
	case m.GetStickerMessage().GetMediaKey() != nil:
		return &waE2E.Message{StickerMessage: m.GetStickerMessage()}, "sticker", "webp"
	}
	return nil, "", ""
}

func mediaKeyOf(m *waE2E.Message) []byte {
	switch {
	case m.ImageMessage != nil:
		return m.ImageMessage.GetMediaKey()
	case m.VideoMessage != nil:
		return m.VideoMessage.GetMediaKey()
	case m.AudioMessage != nil:
		return m.AudioMessage.GetMediaKey()
	case m.DocumentMessage != nil:
		return m.DocumentMessage.GetMediaKey()
	case m.StickerMessage != nil:
		return m.StickerMessage.GetMediaKey()
	}
	return nil
}

func setDirectPath(m *waE2E.Message, p string) {
	switch {
	case m.ImageMessage != nil:
		m.ImageMessage.DirectPath = &p
	case m.VideoMessage != nil:
		m.VideoMessage.DirectPath = &p
	case m.AudioMessage != nil:
		m.AudioMessage.DirectPath = &p
	case m.DocumentMessage != nil:
		m.DocumentMessage.DirectPath = &p
	case m.StickerMessage != nil:
		m.StickerMessage.DirectPath = &p
	}
}

func collectMedia() []*mediaItem {
	byKey := map[string]*mediaItem{}
	add := func(chat string, wm interface {
		GetMessage() *waE2E.Message
	}, id, sender string, fromMe bool, ts int64) {
		mm, kind, ext := mediaOf(wm.GetMessage())
		if mm == nil {
			return
		}
		mm = proto.Clone(mm).(*waE2E.Message)
		byKey[chat+"|"+id] = &mediaItem{chat: chat, sender: sender, id: id, fromMe: fromMe, ts: ts, msg: mm, kind: kind, ext: ext}
	}
	files, _ := filepath.Glob(filepath.Join(dataDir, "raw", "history", "*.pb"))
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var hs waHistorySync.HistorySync
		if proto.Unmarshal(b, &hs) != nil {
			continue
		}
		for _, c := range hs.GetConversations() {
			for _, hm := range c.GetMessages() {
				wm := hm.GetMessage()
				k := wm.GetKey()
				sender := wm.GetParticipant()
				if sender == "" {
					sender = k.GetParticipant()
				}
				if sender == "" && !k.GetFromMe() {
					sender = c.GetID()
				}
				add(c.GetID(), wm, k.GetID(), sender, k.GetFromMe(), int64(wm.GetMessageTimestamp()))
			}
		}
	}
	list := make([]*mediaItem, 0, len(byKey))
	for _, it := range byKey {
		list = append(list, it)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ts > list[j].ts }) // newest first
	return list
}

func mediaPath(it *mediaItem) string {
	user := it.chat
	if i := strings.IndexByte(user, '@'); i > 0 {
		user = user[:i]
	}
	safeID := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, it.id)
	return filepath.Join(dataDir, "media", user, fmt.Sprintf("%s_%s.%s", time.Unix(it.ts, 0).Format("2006-01-02_150405"), safeID, it.ext))
}

func logFailure(it *mediaItem, stage string, err error) {
	rec, _ := json.Marshal(map[string]any{"chat": it.chat, "id": it.id, "ts": it.ts, "kind": it.kind, "stage": stage, "error": fmt.Sprint(err)})
	f, e := os.OpenFile(filepath.Join(dataDir, "media-failed.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if e == nil {
		f.Write(append(rec, '\n'))
		f.Close()
	}
}

func needsRetry(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrNoURLPresent)
}

func runMedia(ctx context.Context, cli *whatsmeow.Client) {
	for !cli.IsLoggedIn() {
		time.Sleep(time.Second)
	}
	time.Sleep(3 * time.Second)
	items := collectMedia()
	var todo []*mediaItem
	for _, it := range items {
		if _, err := os.Stat(mediaPath(it)); err != nil && it.ts >= mediaSince {
			todo = append(todo, it)
		}
	}
	logf("MEDIA %d items with media, %d still to download", len(items), len(todo))

	// Pass 1: straight from the CDN, 4 at a time.
	var ok, retry atomic.Int64
	var retryList []*mediaItem
	var rlMu sync.Mutex
	work := make(chan *mediaItem)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				data, err := cli.DownloadAny(ctx, it.msg)
				if err == nil {
					saveMedia(it, data)
					ok.Add(1)
				} else if needsRetry(err) {
					retry.Add(1)
					rlMu.Lock()
					retryList = append(retryList, it)
					rlMu.Unlock()
				} else {
					logFailure(it, "download", err)
				}
			}
		}()
	}
	for i, it := range todo {
		work <- it
		if (i+1)%250 == 0 {
			logf("MEDIA pass1 %d/%d: %d saved, %d need phone re-upload", i+1, len(todo), ok.Load(), retry.Load())
		}
	}
	close(work)
	wg.Wait()
	logf("MEDIA pass1 done: %d saved, %d need phone re-upload", ok.Load(), len(retryList))

	// Pass 2: ask the phone to re-upload expired media, one at a time.
	sort.Slice(retryList, func(i, j int) bool { return retryList[i].ts > retryList[j].ts })
	var got, notFound, failed int
	for i, it := range retryList {
		res := retryOne(ctx, cli, it)
		switch res {
		case "ok":
			got++
		case "not-found":
			notFound++
		default:
			failed++
		}
		if (i+1)%50 == 0 {
			logf("MEDIA pass2 %d/%d: %d recovered, %d gone from phone, %d failed", i+1, len(retryList), got, notFound, failed)
		}
		time.Sleep(700 * time.Millisecond)
	}
	logf("MEDIA COMPLETE: pass1 %d saved; pass2 %d recovered, %d gone from phone, %d failed", ok.Load(), got, notFound, failed)
}

func retryOne(ctx context.Context, cli *whatsmeow.Client, it *mediaItem) string {
	chat, err := types.ParseJID(it.chat)
	if err != nil {
		return "bad-jid"
	}
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, IsFromMe: it.fromMe, IsGroup: chat.Server == types.GroupServer},
		ID:            it.id,
	}
	if it.sender != "" {
		if s, err := types.ParseJID(it.sender); err == nil {
			info.Sender = s
		}
	} else if it.fromMe && cli.Store.ID != nil {
		info.Sender = cli.Store.ID.ToNonAD()
	}
	key := mediaKeyOf(it.msg)
	ch := make(chan *events.MediaRetry, 1)
	retryMu.Lock()
	retryWaiters[it.id] = ch
	retryMu.Unlock()
	defer func() {
		retryMu.Lock()
		delete(retryWaiters, it.id)
		retryMu.Unlock()
	}()
	if err := cli.SendMediaRetryReceipt(ctx, info, key); err != nil {
		logFailure(it, "retry-send", err)
		return "failed"
	}
	select {
	case ev := <-ch:
		if ev.Error != nil {
			logFailure(it, "retry-error", fmt.Errorf("code %d", ev.Error.Code))
			return "failed"
		}
		n, err := whatsmeow.DecryptMediaRetryNotification(ev, key)
		if err != nil {
			logFailure(it, "retry-decrypt", err)
			return "failed"
		}
		if n.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
			logFailure(it, "retry-result", fmt.Errorf("%s", n.GetResult()))
			if n.GetResult() == waMmsRetry.MediaRetryNotification_NOT_FOUND {
				return "not-found"
			}
			return "failed"
		}
		setDirectPath(it.msg, n.GetDirectPath())
		data, err := cli.DownloadAny(ctx, it.msg)
		if err != nil {
			logFailure(it, "retry-download", err)
			return "failed"
		}
		saveMedia(it, data)
		return "ok"
	case <-time.After(60 * time.Second):
		logFailure(it, "retry-timeout", errors.New("no reply from phone in 60s"))
		return "failed"
	}
}

func saveMedia(it *mediaItem, data []byte) {
	p := mediaPath(it)
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := writeFileAtomic(p, data); err != nil {
		logFailure(it, "write", err)
		return
	}
	t := time.Unix(it.ts, 0)
	os.Chtimes(p, t, t)
}

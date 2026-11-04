package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// chatState tracks on-demand backfill for one chat. The anchor is the
// message we ask the phone to go back from: the oldest message of the last
// reply (or, initially, the oldest real message we hold).
type chatState struct {
	ID       string `json:"id"`
	FromMe   bool   `json:"from_me"`
	TS       int64  `json:"ts"`
	Seen     int    `json:"seen"`
	Requests int    `json:"requests"`
	Done     bool   `json:"done"`
	Why      string `json:"why,omitempty"`
	Fails    int    `json:"fails"`
	Resumed  bool   `json:"resumed"` // anchor came from an ON_DEMAND reply
}

var (
	bfMu     sync.Mutex
	states   = map[string]*chatState{}
	seen     = map[string]map[string]bool{}
	onDemand = make(chan *waHistorySync.HistorySync, 64)
)

// noteConversation records message IDs; returns how many were new and the
// oldest real message in this batch.
func noteConversation(c *waHistorySync.Conversation) (newCount int, oldest *waHistorySync.HistorySyncMsg) {
	bfMu.Lock()
	defer bfMu.Unlock()
	jid := c.GetID()
	s := seen[jid]
	if s == nil {
		s = map[string]bool{}
		seen[jid] = s
	}
	st := states[jid]
	if st == nil {
		st = &chatState{}
		states[jid] = st
	}
	for _, hm := range c.GetMessages() {
		wm := hm.GetMessage()
		id := wm.GetKey().GetID()
		if id == "" {
			continue
		}
		if !s[id] {
			s[id] = true
			newCount++
		}
		if wm.GetMessageTimestamp() == 0 || wm.Message == nil {
			continue // stubs make poor anchors
		}
		if oldest == nil || wm.GetMessageTimestamp() < oldest.GetMessage().GetMessageTimestamp() {
			oldest = hm
		}
	}
	st.Seen = len(s)
	return
}

func setAnchor(st *chatState, hm *waHistorySync.HistorySyncMsg) {
	wm := hm.GetMessage()
	st.ID, st.FromMe, st.TS = wm.GetKey().GetID(), wm.GetKey().GetFromMe(), int64(wm.GetMessageTimestamp())
}

func loadState() {
	var prev map[string]*chatState
	if b, err := os.ReadFile(filepath.Join(dataDir, "backfill-state.json")); err == nil {
		json.Unmarshal(b, &prev)
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
			_, oldest := noteConversation(c)
			st := states[c.GetID()]
			if oldest != nil && (st.ID == "" || int64(oldest.GetMessage().GetMessageTimestamp()) < st.TS) {
				setAnchor(st, oldest)
			}
		}
	}
	for jid, p := range prev {
		if st := states[jid]; st != nil && p.Resumed {
			st.ID, st.FromMe, st.TS = p.ID, p.FromMe, p.TS
			st.Done, st.Why, st.Requests, st.Resumed = p.Done, p.Why, p.Requests, true
		}
	}
}

func saveState() {
	bfMu.Lock()
	b, _ := json.MarshalIndent(states, "", " ")
	bfMu.Unlock()
	writeFileAtomic(filepath.Join(dataDir, "backfill-state.json"), b)
}

func runBackfill(ctx context.Context, cli *whatsmeow.Client) {
	loadState()
	for !cli.IsLoggedIn() {
		time.Sleep(time.Second)
	}
	time.Sleep(5 * time.Second)
	for round := 1; ; round++ {
		bfMu.Lock()
		var todo []string
		for jid, st := range states {
			if !st.Done && st.ID != "" && jid != "status@broadcast" {
				todo = append(todo, jid)
			}
		}
		bfMu.Unlock()
		if len(todo) == 0 {
			logf("BACKFILL COMPLETE: no chat has more history to ask for")
			saveState()
			return
		}
		sort.Strings(todo)
		logf("BACKFILL round %d: %d chats to walk back", round, len(todo))
		for _, jid := range todo {
			walkChat(ctx, cli, jid)
		}
	}
}

func walkChat(ctx context.Context, cli *whatsmeow.Client, jid string) {
	chat, err := types.ParseJID(jid)
	if err != nil {
		bfMu.Lock()
		states[jid].Done, states[jid].Why = true, "bad jid"
		bfMu.Unlock()
		return
	}
	added := 0
	for {
		bfMu.Lock()
		st := *states[jid]
		bfMu.Unlock()
		info := &types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, IsFromMe: st.FromMe},
			ID:            st.ID,
			Timestamp:     time.Unix(st.TS, 0),
		}
		if _, err := cli.SendPeerMessage(ctx, cli.BuildHistorySyncRequest(info, 50)); err != nil {
			logf("ERROR backfill request %s: %v", jid, err)
			time.Sleep(15 * time.Second)
			return
		}
		hs, conv := waitOnDemand(jid, 90*time.Second)
		bfMu.Lock()
		s := states[jid]
		s.Requests++
		stop := ""
		if hs == nil {
			s.Fails++
			if s.Fails >= 3 {
				stop = "no reply x3"
			}
		} else {
			s.Fails = 0
			n, oldest := 0, (*waHistorySync.HistorySyncMsg)(nil)
			if conv != nil {
				bfMu.Unlock()
				n, oldest = noteConversation(conv)
				bfMu.Lock()
			}
			added += n
			switch {
			case conv == nil || len(conv.GetMessages()) == 0:
				stop = "empty reply"
			case n == 0:
				stop = "nothing new"
			case conv.GetEndOfHistoryTransfer() && conv.GetEndOfHistoryTransferType() == waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY:
				stop = "phone says complete"
			}
			if oldest != nil {
				setAnchor(s, oldest)
				s.Resumed = true
			}
		}
		if stop != "" {
			s.Done, s.Why = true, stop
		}
		seenN, oldestDay := s.Seen, time.Unix(s.TS, 0).Format("2006-01-02")
		bfMu.Unlock()
		saveState()
		if stop != "" {
			if added > 0 || stop != "empty reply" {
				logf("BACKFILL %s: done (%s), +%d this run, %d total, back to %s", jid, stop, added, seenN, oldestDay)
			}
			return
		}
		if s.Requests%20 == 0 {
			logf("BACKFILL %s: +%d so far, %d total, back to %s", jid, added, seenN, oldestDay)
		}
		time.Sleep(1200 * time.Millisecond)
	}
}

// waitOnDemand waits for the ON_DEMAND packet answering a request for jid.
func waitOnDemand(jid string, timeout time.Duration) (*waHistorySync.HistorySync, *waHistorySync.Conversation) {
	deadline := time.After(timeout)
	for {
		select {
		case hs := <-onDemand:
			if len(hs.GetConversations()) == 0 {
				return hs, nil
			}
			for _, c := range hs.GetConversations() {
				if c.GetID() == jid {
					return hs, c
				}
			}
			for _, c := range hs.GetConversations() {
				noteConversation(c)
			}
		case <-deadline:
			return nil, nil
		}
	}
}

// wadump: read-only WhatsApp companion that links as a desktop device,
// asks the phone for full history sync and stores every packet raw on disk.
// It never sends messages or presence.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	dataDir    string
	backfill   bool
	media      bool
	mediaSince int64 // only fetch media from messages at or after this unix time
	nHistory   atomic.Int64
	nLive      atomic.Int64
)

func logf(format string, a ...any) {
	fmt.Printf("%s WADUMP "+format+"\n", append([]any{time.Now().Format("2006-01-02 15:04:05")}, a...)...)
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func handler(evt any) {
	switch v := evt.(type) {
	case *events.HistorySync:
		d := v.Data
		raw, err := proto.Marshal(d)
		if err != nil {
			logf("ERROR marshal history sync: %v", err)
			return
		}
		n := nHistory.Add(1)
		name := fmt.Sprintf("hs-%05d-%d-%s-c%d.pb", n, time.Now().UnixNano(), d.GetSyncType().String(), d.GetChunkOrder())
		if err := writeFileAtomic(filepath.Join(dataDir, "raw", "history", name), raw); err != nil {
			logf("ERROR write %s: %v", name, err)
			return
		}
		msgs := 0
		for _, c := range d.GetConversations() {
			msgs += len(c.GetMessages())
		}
		logf("HISTORY #%d type=%s chunk=%d progress=%d convs=%d msgs=%d pushnames=%d bytes=%d",
			n, d.GetSyncType(), d.GetChunkOrder(), d.GetProgress(), len(d.GetConversations()), msgs, len(d.GetPushnames()), len(raw))
		if backfill && d.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND {
			onDemand <- d
		} else if backfill {
			for _, c := range d.GetConversations() {
				noteConversation(c)
			}
		}
	case *events.Message:
		rec := map[string]any{"info": v.Info, "message": json.RawMessage(mustProtoJSON(v.Message))}
		b, _ := json.Marshal(rec)
		f, err := os.OpenFile(filepath.Join(dataDir, "raw", "live.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			f.Write(append(b, '\n'))
			f.Close()
		}
		if n := nLive.Add(1); n%100 == 1 {
			logf("LIVE messages received so far: %d", n)
		}
	case *events.MediaRetry:
		handleMediaRetry(v)
	case *events.PairSuccess:
		logf("PAIRED as %s (%s)", v.ID, v.Platform)
		os.Remove(filepath.Join(dataDir, "qr.txt"))
	case *events.Connected:
		logf("CONNECTED")
	case *events.Disconnected:
		logf("DISCONNECTED")
	case *events.OfflineSyncCompleted:
		logf("OFFLINE-SYNC-COMPLETED count=%d", v.Count)
	case *events.AppStateSyncComplete:
		logf("APPSTATE-SYNC-COMPLETE %s", v.Name)
	case *events.LoggedOut:
		logf("LOGGED-OUT reason=%v onConnect=%v", v.Reason, v.OnConnect)
	case *events.TemporaryBan:
		logf("TEMPORARY-BAN %s", v.String())
	case *events.StreamReplaced:
		logf("STREAM-REPLACED")
	}
}

func mustProtoJSON(m proto.Message) []byte {
	b, err := protojson.Marshal(m)
	if err != nil {
		return []byte("null")
	}
	return b
}

func main() {
	home, _ := os.UserHomeDir()
	dataDir = filepath.Join(home, "whatsapp", "data")
	for _, a := range os.Args[1:] {
		if a == "-backfill" {
			backfill = true
		} else if a == "-media" {
			media = true
		} else if strings.HasPrefix(a, "-since=") {
			t, err := time.Parse("2006-01-02", strings.TrimPrefix(a, "-since="))
			if err != nil {
				panic(err)
			}
			mediaSince = t.Unix()
		} else {
			dataDir = a
		}
	}
	os.MkdirAll(filepath.Join(dataDir, "raw", "history"), 0o700)
	existing, _ := filepath.Glob(filepath.Join(dataDir, "raw", "history", "*.pb"))
	nHistory.Store(int64(len(existing)))
	os.WriteFile(filepath.Join(dataDir, "wadump.pid"), []byte(fmt.Sprint(os.Getpid())), 0o600)

	// Present as a desktop client that wants the full history.
	store.SetOSInfo("Linux", [3]uint32{0, 1, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	store.DeviceProps.RequireFullSync = proto.Bool(true)
	hc := store.DeviceProps.HistorySyncConfig
	hc.FullSyncDaysLimit = proto.Uint32(20 * 365)
	hc.FullSyncSizeMbLimit = proto.Uint32(100 * 1024)
	hc.StorageQuotaMb = proto.Uint32(200 * 1024)
	hc.ThumbnailSyncDaysLimit = proto.Uint32(20 * 365)

	ctx := context.Background()
	dbLog := waLog.Stdout("DB", "WARN", false)
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+filepath.Join(dataDir, "session.db")+"?_foreign_keys=on&_busy_timeout=10000", dbLog)
	if err != nil {
		panic(err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		panic(err)
	}
	cli := whatsmeow.NewClient(device, waLog.Stdout("Client", "INFO", false))
	cli.AutomaticMessageRerequestFromPhone = true
	cli.EnableAutoReconnect = true
	cli.AddEventHandler(handler)

	if cli.Store.ID == nil {
		qrChan, _ := cli.GetQRChannel(ctx)
		if err := cli.Connect(); err != nil {
			panic(err)
		}
		go func() {
			for item := range qrChan {
				switch item.Event {
				case "code":
					var sb strings.Builder
					qrterminal.GenerateWithConfig(item.Code, qrterminal.Config{
						Level: qrterminal.L, Writer: &sb, HalfBlocks: true,
						BlackChar: qrterminal.BLACK_BLACK, WhiteChar: qrterminal.WHITE_WHITE,
						BlackWhiteChar: qrterminal.BLACK_WHITE, WhiteBlackChar: qrterminal.WHITE_BLACK,
						QuietZone: 2,
					})
					txt := "Scan with phone: WhatsApp > Settings > Linked Devices > Link a Device\n" +
						"(code refreshes every ~20s, generated " + time.Now().Format("15:04:05") + ")\n\n" + sb.String()
					writeFileAtomic(filepath.Join(dataDir, "qr.txt"), []byte(txt))
					logf("QR code refreshed (valid %s)", item.Timeout)
				default:
					logf("QR event: %s %v", item.Event, item.Error)
				}
			}
		}()
	} else {
		logf("existing session %s, connecting (backfill=%v)", cli.Store.ID, backfill)
		if err := cli.Connect(); err != nil {
			panic(err)
		}
		if backfill {
			go runBackfill(ctx, cli)
		}
		if media {
			go runMedia(ctx, cli)
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	logf("shutting down (history packets=%d live=%d)", nHistory.Load(), nLive.Load())
	cli.Disconnect()
}

package main

import (
	"fmt"
	"os"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"
)

func main() {
	for _, f := range os.Args[1:] {
		b, _ := os.ReadFile(f)
		var hs waHistorySync.HistorySync
		if err := proto.Unmarshal(b, &hs); err != nil {
			fmt.Println(f, err)
			continue
		}
		fmt.Printf("== %s type=%s convs=%d\n", f, hs.GetSyncType(), len(hs.GetConversations()))
		for _, c := range hs.GetConversations() {
			ms := c.GetMessages()
			fmt.Printf("  conv %s msgs=%d endOfHistory=%v type=%s\n", c.GetID(), len(ms), c.GetEndOfHistoryTransfer(), c.GetEndOfHistoryTransferType())
			for i, m := range ms {
				if i < 3 || i >= len(ms)-3 {
					w := m.GetMessage()
					fmt.Printf("    [%d] order=%d %s id=%s fromMe=%v\n", i, m.GetMsgOrderID(), time.Unix(int64(w.GetMessageTimestamp()), 0).Format("2006-01-02 15:04"), w.GetKey().GetID(), w.GetKey().GetFromMe())
				}
			}
		}
	}
}

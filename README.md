# wadump

Tools for getting WhatsApp history off a phone when the normal export or backup route fails, and for merging it into an Android backup.

I wrote them for one recovery. An iPhone held months of messages that no backup could capture, and an Android phone had an older Google Drive backup. These tools pulled the missing months off the iPhone and merged them into the Android backup, which then restored like any other.

## Tools

| Command | What it does |
|---|---|
| `wadump` | Read-only WhatsApp client that links to the phone as a desktop device (via [whatsmeow](https://github.com/tulir/whatsmeow)). Asks for full history sync, saves every history packet raw to disk, can walk each chat further back on demand, and downloads media, including media that has expired on WhatsApp's servers, by asking the phone to re-upload it. Never sends messages or presence. |
| `waexport` | Turns the raw packets into `archive.db` (SQLite) plus a browsable HTML page and a `.txt` file per chat. |
| `wamerge` | Inserts the captured messages into a decrypted Android `msgstore.db`, skipping any message ID already present, and stages the media files under WhatsApp's own folder layout. |
| `wapeek` | Debugging helper: prints the conversations and messages in one raw packet. |

## Build

```sh
go build -o wadump .
go build -o waexport ./cmd/waexport
go build -o wamerge ./cmd/wamerge
go build -o wapeek ./cmd/wapeek
```

## Capturing from the phone

```sh
./wadump [data dir]                       # first run: writes a QR code to <data>/qr.txt; scan it from
                                          # WhatsApp > Settings > Linked Devices > Link a Device
./wadump -backfill [data dir]             # ask the phone for older history, chat by chat
./wadump -media [-since=YYYY-MM-DD] [data dir]   # download media
./waexport [data dir]                     # build <data>/export/
```

The data dir defaults to `~/whatsapp/data`. It holds the session (`session.db`), raw packets (`raw/history/*.pb`, `raw/live.jsonl`), and media (`media/<chat>/<date>_<id>.<ext>`).

Keep the phone unlocked with WhatsApp open while media downloads, because the phone has to re-upload every expired file.

### Limits seen in practice

- The phone answers on-demand history requests for one year back at most.
- Media retries only work while that same phone is still the active WhatsApp device. Activating WhatsApp elsewhere logs out both the phone and the linked client, and later retries time out with no error.
- A few re-uploaded files fail WhatsApp's hash check because the phone re-encoded them. `wadump` logs them to `media-failed.jsonl` and moves on.

## Merging into an Android backup

You need [wa-crypt-tools](https://github.com/ElDavoo/wa-crypt-tools) and an end-to-end encrypted Android backup made with a **64-digit key**. A password-protected backup won't work, and the older crypt14 format needs root to decrypt.

1. On the Android phone, enable end-to-end encrypted backup with a 64-digit key and tap **Back up**.
2. Pull the backup and the list of media already on the phone:
   ```sh
   adb pull /sdcard/Android/media/com.whatsapp/WhatsApp/accounts/1001/Databases orig/
   adb shell 'cd /sdcard/Android/media/com.whatsapp/WhatsApp && find Media -type f' > phone-media.txt
   ```
   Recent WhatsApp versions keep backups under `accounts/<n>/`, not only `WhatsApp/Databases`.
3. Decrypt, merge, then encrypt again with the original file as the header reference:
   ```sh
   wadecrypt <key> orig/msgstore.db.crypt15 msgstore.db
   ./wamerge <data dir> msgstore.db stage/ <gap start YYYY-MM-DD> phone-media.txt
   waencrypt --reference orig/msgstore.db.crypt15 <key> msgstore.db msgstore.db.crypt15
   ```
   `wamerge` adds every captured message newer than the backup's last message before the gap date. It also does the following:
   - renumbers `sort_id` so the new messages sort in time order
   - adds media, replies, reactions, contacts and locations, and applies edits and deletions
   - creates any missing chats by copying a chat row WhatsApp made itself
4. Restore on the phone:
   1. Copy the whole `Android/media/com.whatsapp` folder to the computer first.
   2. Reset the app with `adb shell pm clear com.whatsapp`. This also deletes `Android/media/com.whatsapp`, so move that folder elsewhere on the phone before the reset and back afterwards.
   3. Put the new `msgstore.db.crypt15` into both `WhatsApp/Databases/` and `WhatsApp/accounts/1001/Databases/`, and copy `stage/Media/` into both `Media` folders.
   4. Grant media access ahead of time: `pm grant com.whatsapp android.permission.READ_MEDIA_IMAGES`, and the same for `READ_MEDIA_VIDEO` and `READ_MEDIA_AUDIO`.
   5. Open WhatsApp, verify the number, **allow** the permission prompt, restore, and enter the key.

### Restore pitfalls

- Denying the permission prompt on the restore screen makes WhatsApp skip the restore with no error.
- A restore that sits on "Preparing…" and then fails means WhatsApp rejected the database contents, not the encryption. To check, re-encrypt an unmodified backup with `waencrypt` and restore it.
- Backups contain no indexes, and WhatsApp rebuilds them during restore. New chat rows without `account_jid_row_id` and its related fields made the restore hang, so `wamerge` now copies an existing chat row as a template.

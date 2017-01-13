package disk

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
)

const dbFilename = "index.db"

var messagesBucket = []byte("messages")

type Mailbox struct {
	Dirpath string

	info *imap.MailboxInfo
	db *bolt.DB

	subscribed bool
}

func NewMailbox(dirpath string, info *imap.MailboxInfo) *Mailbox {
	return &Mailbox{
		Dirpath: dirpath,
		info: info,
		subscribed: true,
	}
}

func (m *Mailbox) Name() string {
	return m.info.Name
}

func (m *Mailbox) Info() (*imap.MailboxInfo, error) {
	return m.info, nil
}

func (m *Mailbox) open() (*bolt.DB, error) {
	if m.db != nil {
		return m.db, nil
	}

	db, err := bolt.Open(filepath.Join(m.Dirpath, dbFilename), 0600, nil)
	if err != nil {
		return nil, err
	}

	m.db = db
	return db, nil
}

func (m *Mailbox) Status(items []string) (*imap.MailboxStatus, error) {
	db, err := m.open()
	if err != nil {
		return nil, err
	}

	status := imap.NewMailboxStatus(m.Name(), items)
	status.PermanentFlags = []string{"\\*"}
	status.UidValidity = 1

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			status.Messages++

			if uid := parseUid(k); uid > status.UidNext {
				status.UidNext = uid
			}

			var msg Message
			if err := json.Unmarshal(v, &msg); err != nil {
				return err
			}

			if msg.Flags[imap.RecentFlag] {
				status.Recent++
			}
			if !msg.Flags[imap.SeenFlag] {
				status.Unseen++
			}
		}

		return nil
	})

	status.UidNext++
	return status, err
}

func (m *Mailbox) SetSubscribed(subscribed bool) error {
	m.subscribed = subscribed
	return nil
}

func (m *Mailbox) Check() error {
	return nil
}

func (m *Mailbox) messageFilepath(k []byte) string {
	return filepath.Join(m.Dirpath, string(k)+".eml")
}

func (m *Mailbox) message(k, v []byte, seqNum, uid uint32) (*Message, error) {
	msg := &Message{
		Filepath: m.messageFilepath(k),
		SeqNum: seqNum,
		Uid: uid,
	}
	if err := json.Unmarshal(v, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *Mailbox) ListMessages(isUid bool, seqSet *imap.SeqSet, items []string, ch chan<- *imap.Message) error {
	defer close(ch)

	db, err := m.open()
	if err != nil {
		return err
	}

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		var seqNum uint32
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			uid := parseUid(k)
			seqNum++
			if (isUid && !seqSet.Contains(uid)) || (!isUid && !seqSet.Contains(seqNum)) {
				continue
			}

			msg, err := m.message(k, v, seqNum, uid)
			if err != nil {
				return err
			}

			imapMsg, err := msg.toIMAP(items)
			if err != nil {
				continue
			}

			ch <- imapMsg
		}

		return nil
	})
}

func (m *Mailbox) SearchMessages(isUid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	db, err := m.open()
	if err != nil {
		return nil, err
	}

	var ids []uint32
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		var seqNum uint32
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Filter sequence number and UID
			uid := parseUid(k)
			seqNum++
			if !backendutil.MatchSeqNumAndUid(seqNum, uid, criteria) {
				continue
			}

			// Read message metadata
			msg, err := m.message(k, v, seqNum, uid)
			if err != nil {
				return err
			}

			// Filter date and flags
			if !backendutil.MatchDate(msg.Date, criteria) {
				continue
			}
			if !backendutil.MatchFlags(msg.flagList(), criteria) {
				continue
			}

			// Filter body
			f, err := os.Open(msg.Filepath)
			if err != nil {
				return err
			}
			e, err := message.Read(f)
			if err != nil {
				f.Close()
				return err
			}
			ok, err := backendutil.Match(e, criteria)
			f.Close()
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			// Add message ID to results
			id := seqNum
			if isUid {
				id = uid
			}
			ids = append(ids, id)
		}

		return nil
	})
	return ids, err
}

func (m *Mailbox) CreateMessage(flags []string, date time.Time, body imap.Literal) error {
	db, err := m.open()
	if err != nil {
		return err
	}

	msg := new(Message)
	msg.setFlagList(flags)
	msg.Date = date

	// Write metadata and assign a new UID
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		id, _ := b.NextSequence()
		msg.Uid = uint32(id)

		k := formatUid(msg.Uid)
		msg.Filepath = m.messageFilepath(k)

		v, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		return b.Put(k, v)
	})
	if err != nil {
		return err
	}

	// Write message body
	f, err := os.Create(msg.Filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, body)
	return err
}

func (m *Mailbox) UpdateMessagesFlags(isUid bool, seqSet *imap.SeqSet, operation imap.FlagsOp, flags []string) error {
	db, err := m.open()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		var seqNum uint32
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Filter messages
			uid := parseUid(k)
			seqNum++
			if (isUid && !seqSet.Contains(uid)) || (!isUid && !seqSet.Contains(seqNum)) {
				continue
			}

			// Read message metadata
			msg, err := m.message(k, v, seqNum, uid)
			if err != nil {
				return err
			}

			// Update flags
			msg.setFlagList(backendutil.UpdateFlags(msg.flagList(), operation, flags))

			// Write message metadata
			v, err = json.Marshal(msg)
			if err != nil {
				return err
			}
			return b.Put(k, v)
		}

		return nil
	})
}

func (m *Mailbox) CopyMessages(isUid bool, seqSet *imap.SeqSet, dest string) error {
	return errors.New("disk: COPY not supported")
}

func (m *Mailbox) Expunge() error {
	db, err := m.open()
	if err != nil {
		return err
	}

	var deleted [][]byte
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(messagesBucket)

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Read message metadata
			msg, err := m.message(k, v, 0, 0)
			if err != nil {
				return err
			}

			// Delete message if it has the \Deleted flag
			if msg.Flags[imap.DeletedFlag] {
				deleted = append(deleted, k)

				if err := b.Delete(k); err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	for _, k := range deleted {
		if err := os.Remove(m.messageFilepath(k)); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mailbox) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

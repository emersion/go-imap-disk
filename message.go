package disk

import (
	"io"
	"os"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message"
)

func parseUid(k []byte) uint32 {
	return 0 // TODO
}

func formatUid(uid uint32) []byte {
	return nil // TODO
}

type Message struct {
	Filepath string `json:"-"`

	SeqNum uint32 `json:"-"`
	Uid    uint32 `json:"-"`

	Flags map[string]bool
	Date  time.Time
}

func (msg *Message) flagList() []string {
	var flags []string
	for name := range msg.Flags {
		flags = append(flags, name)
	}
	return flags
}

func (msg *Message) setFlagList(flags []string) {
	msg.Flags = make(map[string]bool)
	for _, name := range flags {
		msg.Flags[name] = true
	}
}

func (msg *Message) toIMAP(items []string) (*imap.Message, error) {
	imapMsg := imap.NewMessage(msg.SeqNum, items)

	var f *os.File
	read := func() (*message.Entity, error) {
		var err error
		if f == nil {
			f, err = os.Open(msg.Filepath)
		} else {
			_, err = f.Seek(0, io.SeekStart)
		}
		if err != nil {
			return nil, err
		}

		return message.Read(f)
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	for _, item := range items {
		switch item {
		case imap.EnvelopeMsgAttr:
			e, err := read()
			if err != nil {
				return nil, err
			}
			imapMsg.Envelope, _ = backendutil.FetchEnvelope(e.Header)
		case imap.BodyStructureMsgAttr, imap.BodyMsgAttr:
			e, err := read()
			if err != nil {
				return nil, err
			}
			imapMsg.BodyStructure, _ = backendutil.FetchBodyStructure(e, item == imap.BodyMsgAttr)
		case imap.FlagsMsgAttr:
			imapMsg.Flags = msg.flagList()
		case imap.InternalDateMsgAttr:
			imapMsg.InternalDate = msg.Date
		case imap.SizeMsgAttr:
			if f == nil {
				var err error
				if f, err = os.Open(msg.Filepath); err != nil {
					return nil, err
				}
			}

			info, err := f.Stat()
			if err != nil {
				return nil, err
			}
			imapMsg.Size = uint32(info.Size())
		case imap.UidMsgAttr:
			imapMsg.Uid = msg.Uid
		default:
			section, err := imap.NewBodySectionName(item)
			if err != nil {
				break
			}

			e, err := read()
			if err != nil {
				return nil, err
			}

			imapMsg.Body[section], _ = backendutil.FetchBodySection(e, section)
		}
	}

	return imapMsg, nil
}

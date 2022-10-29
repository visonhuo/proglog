package log

import (
	"bytes"
	"io"

	"github.com/hashicorp/raft"
	api "github.com/visonhuo/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type RequestType uint8

const (
	AppendRequestType RequestType = iota
)

// Raft defers the running of your business logic to the FSM
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns an FSMSnapshot that represents a point-in-time snapshot of the FSM’s state
// 1. they allow Raft to compact its log, so it doesn't store logs whose commands Raft has applied already;
// 2. they allow Raft to bootstrap new servers more efficiently than if the leader had to replicate its entire log again and again
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{reader: f.log.Reader()}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore an FSM from a snapshot
// The FSM must discard existing state to make sure its state will match the leader's replicated state.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if i == 0 {
			// reset the log and configure its initial offset to the first record's offset
			// to make log's offset match
			f.log.Config.Segment.InitialOffset = record.Offset
			if err = f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

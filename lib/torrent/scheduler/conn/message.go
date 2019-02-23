// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package conn

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/storage"
)

// Message joins a protobuf message with an optional payload. The only p2p.Message
// type which should include a payload is PiecePayloadMessage.
type Message struct {
	Message *p2p.Message
	Payload storage.PieceReader
}

// NewPiecePayloadMessage returns a Message for sending a piece payload.
func NewPiecePayloadMessage(index int, pr storage.PieceReader) *Message {
	return &Message{
		Message: &p2p.Message{
			Type: p2p.Message_PIECE_PAYLOAD,
			PiecePayload: &p2p.PiecePayloadMessage{
				Index:  int32(index),
				Offset: 0,
				Length: int32(pr.Length()),
			},
		},
		Payload: pr,
	}
}

// NewPieceRequestMessage returns a Message for requesting a piece.
func NewPieceRequestMessage(index int, length int64) *Message {
	return &Message{
		Message: &p2p.Message{
			Type: p2p.Message_PIECE_REQUEST,
			PieceRequest: &p2p.PieceRequestMessage{
				Index:  int32(index),
				Offset: 0,
				Length: int32(length),
			},
		},
	}
}

// NewErrorMessage returns a Message for indicating an error.
func NewErrorMessage(index int, code p2p.ErrorMessage_ErrorCode, err error) *Message {
	return &Message{
		Message: &p2p.Message{
			Type: p2p.Message_ERROR,
			Error: &p2p.ErrorMessage{
				Index: int32(index),
				Code:  code,
				Error: err.Error(),
			},
		},
	}
}

// NewAnnouncePieceMessage returns a Message for announcing a piece.
func NewAnnouncePieceMessage(index int) *Message {
	return &Message{
		Message: &p2p.Message{
			Type: p2p.Message_ANNOUCE_PIECE,
			AnnouncePiece: &p2p.AnnouncePieceMessage{
				Index: int32(index),
			},
		},
	}
}

// NewCompleteMessage returns a Message for a completed torrent.
func NewCompleteMessage() *Message {
	return &Message{
		Message: &p2p.Message{
			Type: p2p.Message_COMPLETE,
		},
	}
}

func sendMessage(nc net.Conn, msg *p2p.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("proto marshal: %s", err)
	}
	if err := binary.Write(nc, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("write data length: %s", err)
	}
	for len(data) > 0 {
		n, err := nc.Write(data)
		if err != nil {
			return fmt.Errorf("write data: %s", err)
		}
		data = data[n:]
	}
	return nil
}

func sendMessageWithTimeout(nc net.Conn, msg *p2p.Message, timeout time.Duration) error {
	// NOTE: We do not use the clock interface here because the net package uses
	// the system clock when evaluating deadlines.
	if err := nc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set write deadline: %s", err)
	}
	return sendMessage(nc, msg)
}

func readMessage(nc net.Conn) (*p2p.Message, error) {
	var msglen [4]byte
	if _, err := io.ReadFull(nc, msglen[:]); err != nil {
		return nil, fmt.Errorf("read message length: %s", err)
	}
	dataLen := binary.BigEndian.Uint32(msglen[:])
	if uint64(dataLen) > maxMessageSize {
		return nil, fmt.Errorf("message exceeds max size: %d > %d", dataLen, maxMessageSize)
	}
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(nc, data); err != nil {
		return nil, fmt.Errorf("read data: %s", err)
	}
	p2pMessage := new(p2p.Message)
	if err := proto.Unmarshal(data, p2pMessage); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %s", err)
	}
	return p2pMessage, nil
}

func readMessageWithTimeout(nc net.Conn, timeout time.Duration) (*p2p.Message, error) {
	// NOTE: We do not use the clock interface here because the net package uses
	// the system clock when evaluating deadlines.
	if err := nc.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %s", err)
	}
	return readMessage(nc)
}

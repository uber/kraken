package scheduler

import "code.uber.internal/infra/kraken/.gen/go/p2p"

// message joins a protobuf message with an optional payload. The only p2p.Message
// type which should include a payload is PiecePayloadMessage.
type message struct {
	Message *p2p.Message
	Payload []byte
}

func newPiecePayloadMessage(index int, payload []byte) *message {
	return &message{
		Message: &p2p.Message{
			Type: p2p.Message_PIECE_PAYLOAD,
			PiecePayload: &p2p.PiecePayloadMessage{
				Index:  int32(index),
				Offset: 0,
				Length: int32(len(payload)),
			},
		},
		Payload: payload,
	}
}

func newPieceRequestMessage(index int, length int64) *message {
	return &message{
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

func newErrorMessage(index int, code p2p.ErrorMessage_ErrorCode, err error) *message {
	return &message{
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

func newAnnouncePieceMessage(index int) *message {
	return &message{
		Message: &p2p.Message{
			Type: p2p.Message_ANNOUCE_PIECE,
			AnnouncePiece: &p2p.AnnouncePieceMessage{
				Index: int32(index),
			},
		},
	}
}

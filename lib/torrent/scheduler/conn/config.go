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
	"time"

	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/memsize"
)

// Config is the configuration for individual live connections.
type Config struct {

	// HandshakeTimeout is the timeout for dialing, writing, and reading connections
	// during handshake.
	HandshakeTimeout time.Duration `yaml:"handshake_timeout"`

	// SenderBufferSize is the size of the sender channel for a connection.
	// Prevents writers to the connection from being blocked if there are many
	// writers trying to send messages at the same time.
	SenderBufferSize int `yaml:"sender_buffer_size"`

	// ReceiverBufferSize is the size of the receiver channel for a connection.
	// Prevents the connection reader from being blocked if a receiver consumer
	// is taking a long time to process a message.
	ReceiverBufferSize int `yaml:"receiver_buffer_size"`

	Bandwidth bandwidth.Config `yaml:"bandwidth"`
}

func (c Config) applyDefaults() Config {
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.SenderBufferSize == 0 {
		c.SenderBufferSize = 10000
	}
	if c.ReceiverBufferSize == 0 {
		c.ReceiverBufferSize = 10000
	}
	if c.Bandwidth.EgressBitsPerSec == 0 {
		c.Bandwidth.EgressBitsPerSec = 200 * 8 * memsize.Mbit
	}
	if c.Bandwidth.IngressBitsPerSec == 0 {
		c.Bandwidth.IngressBitsPerSec = 300 * 8 * memsize.Mbit
	}
	return c
}

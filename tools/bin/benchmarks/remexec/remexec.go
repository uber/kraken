package remexec

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

const (
	_timeout = 10 * time.Second
	_port    = 22
)

// Dialer connects to hosts over SSH.
type Dialer struct {
	config *ssh.ClientConfig
	agent  agent.Agent
}

// NewDialer creates a new Dialer.
func NewDialer() (*Dialer, error) {
	c, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		return nil, fmt.Errorf("dial ssh agent: %s", err)
	}
	a := agent.NewClient(c)
	signers, err := a.Signers()
	if err != nil {
		return nil, fmt.Errorf("ssh agent signers: %s", err)
	}
	auth := ssh.PublicKeys(signers...)
	d := &Dialer{
		config: &ssh.ClientConfig{
			User:            os.Getenv("USER"),
			Auth:            []ssh.AuthMethod{auth},
			Timeout:         _timeout,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
		agent: a,
	}
	return d, nil
}

// Dial connects to host and returns an Execer for the connection.
func (d *Dialer) Dial(host string) (*Execer, error) {
	addr := fmt.Sprintf("%s:%d", host, _port)
	client, err := ssh.Dial("tcp", addr, d.config)
	if err != nil {
		return nil, fmt.Errorf("ssh dial: %s", err)
	}
	agent.ForwardToAgent(client, d.agent)
	agent.ForwardToRemote(client, os.Getenv("SSH_AUTH_SOCK"))
	return &Execer{client}, nil
}

// Execer performs remote execution of commands over SSH.
type Execer struct {
	client *ssh.Client
}

// Exec executes cmd.
func (e *Execer) Exec(cmd string) error {
	s, err := e.client.NewSession()
	if err != nil {
		return fmt.Errorf("new session: %s", err)
	}
	defer s.Close()
	if err := agent.RequestAgentForwarding(s); err != nil {
		return fmt.Errorf("agent forwarding: %s", err)
	}
	stderr := new(bytes.Buffer)
	s.Stderr = stderr
	if err := s.Run(cmd); err != nil {
		return fmt.Errorf("run `%s`: %s, stderr: %s", cmd, err, stderr.String())
	}
	return nil
}

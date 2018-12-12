package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/uber/kraken/agent/agentserver"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/tools/bin/benchmarks/remexec"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/osutil"
	"github.com/uber/kraken/utils/stringset"
)

const (
	_agentServerPort   = 7602
	_agentRegistryPort = 8991
)

type result struct {
	agent string
	t     time.Duration
	err   error
}

type agentRunner struct {
	agents []string
}

func newAgentRunner(agentFile *os.File) (*agentRunner, error) {
	agents, err := osutil.ReadLines(agentFile)
	if err != nil {
		return nil, fmt.Errorf("read agent file: %s", err)
	}

	healthyAgents := filterHealthyAgents(agents)

	log.Printf("Filtered out %d unhealthy agents (%d remaining)\n",
		len(agents)-len(healthyAgents), len(healthyAgents))

	return &agentRunner{healthyAgents}, nil
}

func (r *agentRunner) numAgents() int {
	return len(r.agents)
}

func (r *agentRunner) download(d core.Digest) chan result {
	log.Println("Running agent downloads...")

	results := make(chan result)

	go func() {
		var wg sync.WaitGroup
		for _, agent := range r.agents {
			wg.Add(1)
			go func(agent string) {
				defer wg.Done()
				start := time.Now()
				addr := fmt.Sprintf("%s:%d", agent, _agentServerPort)
				resp, err := agentserver.NewClient(addr).Download("noexist", d)
				if resp != nil {
					resp.Close()
				}
				t := time.Since(start)
				results <- result{agent, t, err}
			}(agent)
		}
		wg.Wait()
		close(results)
	}()

	return results
}

func (r *agentRunner) dockerPull(name string) (chan result, error) {
	log.Println("Pulling image from agents...")

	dialer, err := remexec.NewDialer()
	if err != nil {
		return nil, fmt.Errorf("new dialer: %s", err)
	}

	results := make(chan result)

	go func() {
		var wg sync.WaitGroup
		for _, agent := range r.agents {
			wg.Add(1)
			go func(agent string) {
				defer wg.Done()
				e, err := dialer.Dial(agent)
				if err != nil {
					results <- result{agent, 0, fmt.Errorf("dial: %s", err)}
					return
				}
				pull := fmt.Sprintf("sudo docker pull localhost:%d/%s", _agentRegistryPort, name)
				start := time.Now()
				err = e.Exec(pull)
				if err != nil {
					err = fmt.Errorf("remote exec: %s", err)
				}
				t := time.Since(start)
				results <- result{agent, t, err}
			}(agent)
		}
		wg.Wait()
		close(results)
	}()

	return results, nil
}

func filterHealthyAgents(agents []string) []string {
	// Remove duplicates.
	agents = stringset.FromSlice(agents).ToSlice()

	var mu sync.Mutex
	var healthy []string

	var wg sync.WaitGroup
	for _, agent := range agents {
		wg.Add(1)
		go func(agent string) {
			defer wg.Done()

			url := fmt.Sprintf("http://%s:%d/health", agent, _agentServerPort)
			_, err := httputil.Get(url, httputil.SendTimeout(5*time.Second))
			if err != nil {
				log.Printf("Ping %s error: %s\n", url, err)
				return
			}

			mu.Lock()
			healthy = append(healthy, agent)
			mu.Unlock()
		}(agent)
	}
	wg.Wait()

	return healthy
}

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/montanaflynn/stats"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/osutil"
	"code.uber.internal/infra/kraken/utils/stringset"
)

const originDNS = "kraken-origin-sjc1.uber.internal:9003"

const trackerDNS = "kraken-tracker-sjc1.uber.internal:8351"

const agentPort = 7602

func filterHealthyAgents(agents []string) []string {
	var mu sync.Mutex
	var healthy []string

	var wg sync.WaitGroup
	for _, host := range agents {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			url := fmt.Sprintf("http://%s:%d/health", host, agentPort)
			_, err := httputil.Get(url, httputil.SendTimeout(5*time.Second))
			if err != nil {
				log.Printf("Ping %s error: %s\n", url, err)
				return
			}

			mu.Lock()
			healthy = append(healthy, host)
			mu.Unlock()
		}(host)
	}
	wg.Wait()

	return healthy
}

func generateBlobFile(size int64) (digest string, f *os.File, err error) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	f, err = ioutil.TempFile("", "")
	if err != nil {
		return "", nil, err
	}
	h := sha256.New()
	w := io.MultiWriter(f, h)
	if _, err := io.CopyN(w, r, size); err != nil {
		return "", nil, err
	}
	digest = hex.EncodeToString(h.Sum(nil))
	return digest, f, nil
}

type fileCloner struct {
	name string
}

func (c fileCloner) Clone() (store.FileReader, error) {
	return os.Open(c.name)
}

type benchmarkRunner struct {
	agents         []string
	metaInfoClient metainfoclient.Client
	originResolver blobclient.ClusterResolver
}

func (r *benchmarkRunner) upload(fileSize, pieceSize int64) (digest string, err error) {
	config := transfer.OriginClusterTransfererConfig{
		TorrentPieceLength: pieceSize,
	}
	// TODO(codyg): Split up OriginClusterTransferer so we don't need to supply
	// unused metainfo client and file store for uploading.
	t := transfer.NewOriginClusterTransferer(config, r.originResolver, nil, r.metaInfoClient, nil)

	digest, f, err := generateBlobFile(fileSize)
	if err != nil {
		return "", fmt.Errorf("generate blob file: %s", err)
	}
	defer os.Remove(f.Name())

	if err := t.Upload(digest, fileCloner{f.Name()}, fileSize); err != nil {
		return "", fmt.Errorf("origin upload: %s", err)
	}
	return digest, nil
}

type result struct {
	agent string
	t     time.Duration
	err   error
}

func (r *benchmarkRunner) run(fileSize, pieceSize int64) (chan result, error) {
	log.Println("Uploading file to origin cluster...")

	digest, err := r.upload(fileSize, pieceSize)
	if err != nil {
		return nil, fmt.Errorf("upload to origin cluster: %s", err)
	}

	log.Println("Running agent downloads...")

	results := make(chan result)

	go func() {
		var wg sync.WaitGroup
		for _, host := range r.agents {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				start := time.Now()
				_, err := httputil.Get(
					fmt.Sprintf("http://%s:%d/blobs/%s", host, agentPort, digest),
					httputil.SendTimeout(15*time.Minute))
				t := time.Since(start)
				results <- result{host, t, err}
			}(host)
		}
		wg.Wait()
		close(results)
	}()

	return results, nil
}

func main() {
	agentFile := flag.String("agents", "", "file of agents to download torrents from (newline delimited)")
	out := flag.String("out", "", "output file where results are written in tsv format")
	flag.Parse()

	if *agentFile == "" {
		log.Fatal("flag -agents required")
	}
	if *out == "" {
		log.Fatal("flag -out required")
	}

	outfile, err := os.Create(*out)
	if err != nil {
		log.Fatalf("Error opening output file: %s", err)
	}

	agents, err := osutil.ReadLines(*agentFile)
	if err != nil {
		log.Fatalf("Error reading lines from agent file: %s", err)
	}
	healthyAgents := filterHealthyAgents(agents)

	log.Printf("Filtered out %d unhealthy agents (%d remaining)\n",
		len(agents)-len(healthyAgents), len(healthyAgents))

	origins, err := serverset.NewRoundRobin(serverset.RoundRobinConfig{Addrs: []string{originDNS}})
	if err != nil {
		log.Fatalf("Error creating origin server set: %s", err)
	}

	trackers, err := serverset.NewRoundRobin(serverset.RoundRobinConfig{Addrs: []string{trackerDNS}})
	if err != nil {
		log.Fatalf("Error creating tracker server set: %s", err)
	}

	r := &benchmarkRunner{
		agents:         healthyAgents,
		metaInfoClient: metainfoclient.Default(trackers),
		originResolver: blobclient.NewClusterResolver(blobclient.NewProvider(blobclient.Config{}), origins),
	}

	fileSizes := []uint64{
		memsize.GB / 8,
		memsize.GB / 4,
		memsize.GB / 2,
		memsize.GB,
		2 * memsize.GB,
		4 * memsize.GB,
		8 * memsize.GB,
	}
	pieceSizes := []uint64{
		memsize.MB,
		2 * memsize.MB,
		4 * memsize.MB,
		8 * memsize.MB,
	}
	fmt.Fprintf(outfile, "peers\tfsz(gb)\tpsz(mb)\tmin(s)\tp50(s)\tp95(s)\tp99(s)\tmax(s)\n")
	for _, fileSize := range fileSizes {
		for _, pieceSize := range pieceSizes {
			log.Printf("---- file size %s / piece size %s ----\n",
				memsize.Format(fileSize), memsize.Format(pieceSize))

			results, err := r.run(int64(fileSize), int64(pieceSize))
			if err != nil {
				log.Printf("Error running benchmark: %s", err)
				continue
			}
			fmt.Fprintf(outfile, "%d\t%.3f\t%.1f\t",
				len(r.agents), float64(fileSize)/float64(memsize.GB), float64(pieceSize)/float64(memsize.MB))
			var times stats.Float64Data
			var errs []error
			pending := stringset.FromSlice(r.agents)
		RESULT_LOOP:
			for {
				select {
				case <-time.After(15 * time.Second):
					log.Printf("Pending hosts: %v\n", pending.ToSlice())
				case res, ok := <-results:
					if !ok {
						break RESULT_LOOP
					}
					pending.Remove(res.agent)
					i := len(r.agents) - len(pending)
					if res.err != nil {
						log.Printf("(%d/%d) FAILURE %s %s\n", i, len(r.agents), res.agent, res.err)
						errs = append(errs, fmt.Errorf("agent %s: %s", res.agent, res.err))
						continue
					}
					t := res.t.Seconds()
					log.Printf("(%d/%d) SUCCESS %s %.2fs\n", i, len(r.agents), res.agent, t)
					times = append(times, t)
				}
			}
			if len(errs) > 0 {
				fmt.Fprintf(outfile, "download failures: %s\n", errutil.Join(errs))
				continue
			}
			if len(times) == 0 {
				fmt.Fprintf(outfile, "no times recorded\n")
				continue
			}
			min, _ := stats.Min(times)
			p50, _ := stats.Median(times)
			p95, _ := stats.Percentile(times, 95)
			p99, _ := stats.Percentile(times, 99)
			max, _ := stats.Max(times)
			fmt.Fprintf(outfile, "%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", min, p50, p95, p99, max)
		}
	}
}

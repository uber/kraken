package main

import (
	"crypto/tls"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/tools/lib"
	"github.com/uber/kraken/tracker/announceclient"

	"github.com/alecthomas/kingpin"
	"github.com/montanaflynn/stats"
)

type torrent struct {
	digest core.Digest
	hash   core.InfoHash
}

type result struct {
	latency time.Duration
	err     error
}

func simulateAnnounce(
	tracker string, pctx core.PeerContext, torrents []torrent, interval time.Duration, results chan result, tls *tls.Config) {

	client := announceclient.New(pctx, healthcheck.NoopFailed(hostlist.Fixture(tracker)), tls)
	i := rand.Intn(len(torrents))
	for {
		t := torrents[i]
		start := time.Now()
		_, _, err := client.Announce(t.digest, t.hash, false, 1)
		results <- result{time.Since(start), err}
		time.Sleep(interval)
		i = (i + 1) % len(torrents)
	}
}

func main() {
	app := kingpin.New("trackerload", "Kraken tracker load testing tool")

	tracker := app.Flag("tracker", "Tracker address").Required().String()
	numPeers := app.Flag("num_peers", "Number of peers to simulate").Short('n').Required().Int()
	interval := app.Flag("interval", "Announce interval").Short('i').Required().Duration()
	sample := app.Flag("sample", "Sample seconds").Short('s').Duration()

	announce := app.Command("announce", "Test announce endpoint")
	numTorrents := announce.Flag("num_torrents", "Number of torrents per peer").Short('t').Required().Int()
	tlsPath := app.Arg("tls", "TLS yaml configuration path").Default("").String()

	results := make(chan result)

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case announce.FullCommand():
		tls, err := lib.ReadTLSFile(tlsPath)
		if err != nil {
			log.Fatalf("Error reading tls file: %s", err)
		}
		var torrents []torrent
		for i := 0; i < *numTorrents; i++ {
			d := core.DigestFixture()
			hash := core.InfoHashFixture()
			torrents = append(torrents, torrent{d, hash})
		}
		for i := 0; i < *numPeers; i++ {
			pctx := core.PeerContextFixture()
			go simulateAnnounce(*tracker, pctx, torrents, *interval, results, tls)
			time.Sleep(*interval / time.Duration(*numPeers))
		}
	}

	var stop <-chan time.Time
	var times stats.Float64Data
	if *sample > 0 {
		stop = time.After(*sample)
	}

	for {
		select {
		case res := <-results:
			if res.err != nil {
				log.Printf("ERROR: %s", res.err)
			} else {
				latency := res.latency.Seconds()
				log.Printf("%.2fs", latency)
				if *sample > 0 {
					times = append(times, latency)
				}
			}
		case <-stop:
			p50, _ := stats.Median(times)
			p95, _ := stats.Percentile(times, 95)
			p99, _ := stats.Percentile(times, 99)
			log.Printf("p50: %.2f\n", p50)
			log.Printf("p95: %.2f\n", p95)
			log.Printf("p99: %.2f\n", p99)
			return
		}
	}
}

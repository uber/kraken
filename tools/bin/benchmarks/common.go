package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/uber/kraken/utils/memsize"
)

type param struct {
	fileSize  uint64
	pieceSize uint64
}

func suiteParams() []param {
	return []param{
		{memsize.MB * 128, memsize.MB},
		{memsize.MB * 256, memsize.MB},
		{memsize.MB * 512, memsize.MB},
		{memsize.GB, memsize.MB},
		{2 * memsize.GB, 4 * memsize.MB},
		{4 * memsize.GB, 4 * memsize.MB},
		{8 * memsize.GB, 8 * memsize.MB},
	}
}

func blobHeader() string {
	return "peers\tfsz(gb)\tpsz(mb)\tmin(s)\tp50(s)\tp95(s)\tp99(s)\tmax(s)\n"
}

func blobRowPrefix(numAgents int, fileSize, pieceSize uint64) string {
	return fmt.Sprintf(
		"%d\t%.3f\t%.1f\t",
		numAgents, float64(fileSize)/float64(memsize.GB), float64(pieceSize)/float64(memsize.MB))
}

func imageHeader() string {
	return "peers\timg(gb)\tlayers\tmin(s)\tp50(s)\tp95(s)\tp99(s)\tmax(s)\n"
}

func imageRowPrefix(numAgents int, imageSize uint64, numLayers int) string {
	return fmt.Sprintf(
		"%d\t%.3f\t%d\t",
		numAgents, float64(imageSize)/float64(memsize.GB), numLayers)
}

func rowSuffix(times stats.Float64Data) string {
	min, _ := stats.Min(times)
	p50, _ := stats.Median(times)
	p95, _ := stats.Percentile(times, 95)
	p99, _ := stats.Percentile(times, 99)
	max, _ := stats.Max(times)
	return fmt.Sprintf("%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", min, p50, p95, p99, max)
}

func drainKafkaEvents(f *os.File, hash string) chan struct{} {
	msgReceived := make(chan struct{})

	// Scrollback a single message as a hacky way of knowing that k8read is
	// running and consuming messages.
	k8read := exec.Command("k8read", "-t", "kraken-p2pnetwork", "-s", "1")
	stdout, err := k8read.StdoutPipe()
	if err != nil {
		log.Fatalf("Error creating k8read stdout pipe: %s", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Split(bufio.ScanLines)
	ready := make(chan struct{})
	go func() {
		var gotFirst bool
		for scanner.Scan() {
			if !gotFirst {
				close(ready)
				gotFirst = true
			}
			msg := scanner.Text()
			if !strings.Contains(msg, hash) {
				continue
			}
			select {
			case msgReceived <- struct{}{}:
			default:
				// Don't block if no one's listening.
			}
			if _, err := io.WriteString(f, msg+"\n"); err != nil {
				log.Fatalf("Error writing to event file: %s", err)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("k8read line scanner error: %s", err)
		}
	}()
	if err := k8read.Start(); err != nil {
		log.Fatalf("Error starting k8read: %s", err)
	}
	log.Println("Waiting for k8read to start...")
	<-ready

	done := make(chan struct{})
	go func() {
		<-done
		for {
			select {
			case <-msgReceived:
			case <-time.After(10 * time.Second):
				log.Println("Kafka events timed out")
				done <- struct{}{}
			}
		}
	}()

	return done
}

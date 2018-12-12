package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/alecthomas/units"
	"github.com/montanaflynn/stats"

	"github.com/uber/kraken/tools/lib"
	"github.com/uber/kraken/tools/lib/image"
	"github.com/uber/kraken/utils/memsize"
)

const _newFileFlags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC

var (
	_profile *profile
)

type command interface {
	full() string
	run()
}

type suiteCmd struct {
	_full      string
	agentFile  *os.File
	resultFile *os.File
	tlsPath    *string
}

func newSuiteCmd(app *kingpin.Application) *suiteCmd {
	suite := &suiteCmd{}
	cmd := app.Command("suite", "Runs the benchmarking suite")
	cmd.Arg("agents", "Newline delimited agent file").Required().FileVar(&suite.agentFile)
	cmd.Arg("results", "TSV result file").Required().OpenFileVar(&suite.resultFile, _newFileFlags, 0775)
	suite.tlsPath = cmd.Flag("tls", "TLS yaml configuration path").Default("").String()
	suite._full = cmd.FullCommand()
	return suite
}

func (c *suiteCmd) full() string { return c._full }

func (c *suiteCmd) run() {
	tls, err := lib.ReadTLSFile(c.tlsPath)
	if err != nil {
		log.Fatalf("Error reading tls file: %s", err)
	}
	runner, err := newAgentRunner(c.agentFile)
	if err != nil {
		log.Fatalf("Error creating agent runner: %s", err)
	}
	u := newUploader(_profile.origin, _profile.tracker, tls)

	if _, err := fmt.Fprintf(c.resultFile, blobHeader()); err != nil {
		log.Fatalf("Result file: %s", err)
	}

	for _, p := range suiteParams() {
		log.Printf("---- file size %s / piece size %s ----\n",
			memsize.Format(p.fileSize), memsize.Format(p.pieceSize))

		if _, err := fmt.Fprintf(
			c.resultFile, blobRowPrefix(runner.numAgents(), p.fileSize, p.pieceSize)); err != nil {

			log.Fatalf("Result file: %s", err)
		}

		_, digest, err := u.upload(p.fileSize, p.pieceSize)
		if err != nil {
			log.Fatalf("Error uploading to origin cluster: %s", err)
		}

		results := runner.download(digest)

		var times stats.Float64Data
		var i int
		for res := range results {
			i++
			if res.err != nil {
				log.Printf("(%d/%d) FAILURE %s %s\n", i, runner.numAgents(), res.agent, res.err)
				continue
			}
			t := res.t.Seconds()
			log.Printf("(%d/%d) SUCCESS %s %.2fs\n", i, runner.numAgents(), res.agent, t)
			times = append(times, t)
		}
		if len(times) == 0 {
			if _, err := fmt.Fprintf(c.resultFile, "no times recorded\n"); err != nil {
				log.Fatalf("Result file: %s", err)
			}
			continue
		}
		if _, err := fmt.Fprintf(c.resultFile, rowSuffix(times)); err != nil {
			log.Fatalf("Result file: %s", err)
		}
	}
}

type runCmd struct {
	_full     string
	agentFile *os.File
	fileSize  units.Base2Bytes
	pieceSize units.Base2Bytes
	eventFile *os.File
	tlsPath   *string
}

func newRunCmd(app *kingpin.Application) *runCmd {
	run := &runCmd{}
	cmd := app.Command("run", "Runs an individual blob distribution")
	cmd.Arg("agents", "Newline delimited agent file").Required().FileVar(&run.agentFile)
	cmd.Arg("file_size", "File size").Required().BytesVar(&run.fileSize)
	cmd.Arg("piece_size", "Piece size").Required().BytesVar(&run.pieceSize)
	cmd.Flag("events", "File to drain kafka events into").
		OpenFileVar(&run.eventFile, _newFileFlags, 0775)
	run.tlsPath = cmd.Flag("tls", "TLS yaml configuration path").Default("").String()
	run._full = cmd.FullCommand()
	return run
}

func (c *runCmd) full() string { return c._full }

func (c *runCmd) run() {
	tls, err := lib.ReadTLSFile(c.tlsPath)
	if err != nil {
		log.Fatalf("Error reading tls file: %s", err)
	}
	runner, err := newAgentRunner(c.agentFile)
	if err != nil {
		log.Fatalf("Error creating agent runner: %s", err)
	}
	u := newUploader(_profile.origin, _profile.tracker, tls)

	hash, digest, err := u.upload(uint64(c.fileSize), uint64(c.pieceSize))
	if err != nil {
		log.Fatalf("Error uploading to origin cluster: %s", err)
	}

	if c.eventFile != nil {
		done := drainKafkaEvents(c.eventFile, hash.String())
		defer func() {
			done <- struct{}{}
			log.Println("Waiting for kafka events to drain...")
			<-done
		}()
	}

	var times stats.Float64Data
	var i int
	for res := range runner.download(digest) {
		i++
		if res.err != nil {
			log.Printf("(%d/%d) FAILURE %s %s\n", i, runner.numAgents(), res.agent, res.err)
			continue
		}
		t := res.t.Seconds()
		log.Printf("(%d/%d) SUCCESS %s %.2fs\n", i, runner.numAgents(), res.agent, t)
		times = append(times, t)
	}
	if len(times) == 0 {
		log.Fatal("No times recorded")
	}
	fmt.Print(blobHeader())
	fmt.Print(blobRowPrefix(
		runner.numAgents(), uint64(c.fileSize), uint64(c.pieceSize)) + rowSuffix(times))
}

type imageCmd struct {
	_full     string
	agentFile *os.File
	imageSize units.Base2Bytes
	numLayers int
}

func newImageCmd(app *kingpin.Application) *imageCmd {
	image := &imageCmd{}
	cmd := app.Command("image", "Push / pull docker images")
	cmd.Arg("agents", "Newline delimited agent file").Required().FileVar(&image.agentFile)
	cmd.Arg("image_size", "Image size").Required().BytesVar(&image.imageSize)
	cmd.Arg("num_layers", "Number of layers").Required().IntVar(&image.numLayers)
	image._full = cmd.FullCommand()
	return image
}

func (c *imageCmd) full() string { return c._full }

func (c *imageCmd) run() {
	runner, err := newAgentRunner(c.agentFile)
	if err != nil {
		log.Fatalf("Error creating agent runner: %s", err)
	}

	name, err := image.Generate(uint64(c.imageSize), c.numLayers)
	if err != nil {
		log.Fatalf("Error generating image: %s", err)
	}
	if err := image.Push(name, _profile.proxy); err != nil {
		log.Fatalf("Error pushing image to proxy: %s", err)
	}

	results, err := runner.dockerPull(name)
	if err != nil {
		log.Fatalf("Error starting docker pulls: %s", err)
	}

	var times stats.Float64Data
	var i int
	for res := range results {
		i++
		if res.err != nil {
			log.Printf("(%d/%d) FAILURE %s %s\n", i, runner.numAgents(), res.agent, res.err)
			continue
		}
		t := res.t.Seconds()
		log.Printf("(%d/%d) SUCCESS %s %.2fs\n", i, runner.numAgents(), res.agent, t)
		times = append(times, t)
	}
	if len(times) == 0 {
		log.Fatal("No times recorded")
	}
	fmt.Print(imageHeader())
	fmt.Print(imageRowPrefix(runner.numAgents(), uint64(c.imageSize), c.numLayers) + rowSuffix(times))
}

func main() {
	app := kingpin.New("benchmarks", "Kraken benchmark tool")

	cluster := app.Flag("cluster", "Zone to run benchmarks in").Short('c').Required().String()
	commands := []command{
		newSuiteCmd(app),
		newRunCmd(app),
		newImageCmd(app),
	}

	args, err := app.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("Error parsing: %s", err)
	}

	_profile = getProfile(*cluster)

	for _, cmd := range commands {
		if args == cmd.full() {
			cmd.run()
		}
	}
}

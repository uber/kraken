package image

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func run(name string, args ...string) error {
	stderr := new(bytes.Buffer)
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("exec `%s`: %s, stderr:\n%s",
			strings.Join(cmd.Args, " "), err, stderr.String())
	}
	return nil
}

// Generate creates a random image.
func Generate(size uint64, numLayers int) (name string, err error) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", fmt.Errorf("temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	name = fmt.Sprintf("kraken-test-image-%s", filepath.Base(dir))

	dockerfile, err := os.Create(fmt.Sprintf("%s/Dockerfile", dir))
	if err != nil {
		return "", fmt.Errorf("create dockerfile: %s", err)
	}
	log.Printf("Generating dockerfile %s", dockerfile.Name())

	if _, err := fmt.Fprintln(dockerfile, "FROM scratch"); err != nil {
		return "", fmt.Errorf("fprint dockerfile: %s", err)
	}

	layerSize := size / uint64(numLayers)
	for i := 0; i < numLayers; i++ {
		f, err := os.Create(fmt.Sprintf("%s/file_%d", dir, i))
		if err != nil {
			return "", fmt.Errorf("create file: %s", err)
		}
		r := io.LimitReader(rand.New(rand.NewSource(time.Now().Unix())), int64(layerSize))
		if _, err := io.Copy(f, r); err != nil {
			return "", fmt.Errorf("copy rand: %s", err)
		}
		layerName := filepath.Base(f.Name())
		if _, err := fmt.Fprintf(dockerfile, "COPY %s /\n", layerName); err != nil {
			return "", fmt.Errorf("fprint dockerfile: %s", err)
		}
	}

	if err := run("sudo", "docker", "build", "-t", name, dir); err != nil {
		return "", err
	}
	return name + ":latest", nil
}

// Push pushes an image to a Kraken proxy.
func Push(name string, proxy string) error {
	proxyName := proxy + "/" + name
	log.Printf("Pushing %s to %s", name, proxyName)
	if err := run("sudo", "docker", "tag", name, proxyName); err != nil {
		return err
	}
	return run("sudo", "docker", "push", proxyName)
}

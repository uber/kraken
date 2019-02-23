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
package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/uber/kraken/utils/errutil"
	"github.com/uber/kraken/utils/log"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
)

// guessDigest returns digest from the URL.
// returns empty string if this push action does not look like a tag.
func guessDigest(url string, repo string) string {
	p := fmt.Sprintf("/v2/%s/manifests/", repo)
	idx := strings.Index(url, p)
	if idx < 0 {
		return ""
	}
	return url[idx+len(p):]
}

// PullImage pull images from source registry, it does not check if the file exits
func PullImage(source, repo, tag string, useDocker bool) error {
	t := time.Now()

	if useDocker {
		log.Info("pulling with docker daemon")
		err := exec.Command("docker", "pull", source+"/"+repo+":"+tag).Run()
		if err != nil {
			return fmt.Errorf("failed to pull image: %s:%s: %s", repo, tag, err.Error())
		}
		return nil
	}

	log.Info("pulling with http")
	manifest, err := pullManifest(http.Client{Timeout: transferTimeout}, source, repo, tag)
	if err != nil {
		return fmt.Errorf("failed to pull manifest %s:%s: %s", repo, tag, err)
	}

	layerDigests, err := getLayerDigestsFromManifest(&manifest)
	if err != nil {
		return fmt.Errorf("failed to get layer digests from manifest: %s", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs errutil.MultiError
	for _, d := range layerDigests {
		wg.Add(1)
		d := d
		go func() {
			defer wg.Done()
			err := pullLayer(http.Client{Timeout: transferTimeout}, source, repo, d)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
				return
			}
		}()
	}
	wg.Wait()

	if errs != nil {
		return fmt.Errorf("failed to pull image %s:%s: %s", repo, tag, errs)
	}

	log.Infof("finished pulling image %s:%s in %v", repo, tag, time.Since(t).Seconds())
	return nil
}

func pullManifest(client http.Client, source string, name string, reference string) (distribution.Manifest, error) {
	manifestURL := fmt.Sprintf(baseManifestQuery, source, name, reference)
	req, err := http.NewRequest("GET", manifestURL, bytes.NewReader([]byte{}))
	if err != nil {
		return nil, err
	}
	// Add `Accept` header to indicate schema2 is supported
	req.Header.Add("Accept", schema2.MediaTypeManifest)
	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("manifest not found")
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("server returned %v", resp.Status)
	}

	version := resp.Header.Get("Content-Type")
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	manifest, _, err := distribution.UnmarshalManifest(version, body)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func getLayerDigestsFromManifest(manifest *distribution.Manifest) ([]string, error) {
	var digests []string
	// Get layers from manifest
	switch (*manifest).(type) {
	case *schema1.SignedManifest:
		fsLayers := (*manifest).(*schema1.SignedManifest).FSLayers
		for _, fsLayer := range fsLayers {
			digests = append(digests, fsLayer.BlobSum.String())
		}
		break
	case *schema2.DeserializedManifest:
		layerDescriptors := (*manifest).(*schema2.DeserializedManifest).Layers
		for _, descriptor := range layerDescriptors {
			digests = append(digests, descriptor.Digest.String())
		}
		// for schema2, we also need a config layer
		config := (*manifest).(*schema2.DeserializedManifest).Config
		digests = append(digests, config.Digest.String())
		break
	default:
		mt, _, err := (*manifest).Payload()
		if err == nil {
			err = fmt.Errorf("manifest type %s is not supported", mt)
		}
		return nil, err
	}

	return digests, nil
}

func pullLayer(client http.Client, source, name string, layerDigest string) error {
	layerURL := fmt.Sprintf(baseLayerQuery, source, name, layerDigest)
	resp, err := client.Get(layerURL)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respDump, errDump := httputil.DumpResponse(resp, true)
		if errDump != nil {
			return errDump
		}
		return fmt.Errorf("failed to pull layer: %s", respDump)
	}

	ok, err := verifyLayer(digest.Digest(layerDigest), resp.Body)
	if err != nil {
		return fmt.Errorf("failed to verfiy layer: %s", err)
	}

	if !ok {
		return fmt.Errorf("failed to verify layer: layer digest does not match to the content")
	}

	return nil
}

func verifyLayer(layerDigest digest.Digest, r io.Reader) (bool, error) {
	v := layerDigest.Verifier()

	if _, err := io.Copy(v, r); err != nil {
		return false, err
	}

	return v.Verified(), nil
}

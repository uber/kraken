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
package registrybackend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/testutil"
)

func TestTagDownloadSuccess(t *testing.T) {
	require := require.New(t)

	imageConfig := core.NewBlobFixture()
	layer1 := core.NewBlobFixture()
	layer2 := core.NewBlobFixture()
	digest, manifest := dockerutil.ManifestFixture(
		imageConfig.Digest, layer1.Digest, layer2.Digest)

	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifest)))
		w.Header().Set("Docker-Content-Digest", digest.String())
		_, err := io.Copy(w, bytes.NewReader(manifest))
		require.NoError(err)
	})
	r.Head(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifest)))
		w.Header().Set("Docker-Content-Digest", digest.String())
		_, err := io.Copy(w, bytes.NewReader(manifest))
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := newTestConfig(addr)
	client, err := NewTagClient(config)
	require.NoError(err)

	info, err := client.Stat(tag, tag)
	require.NoError(err)
	require.Equal(int64(len(manifest)), info.Size)

	var b bytes.Buffer
	require.NoError(client.Download(tag, tag, &b))
	require.Equal(digest.String(), string(b.Bytes()))
}

func TestTagDownloadV1Manifest(t *testing.T) {
	require := require.New(t)

	// signed V1 manifest obtained from Docker hub for busybox:1.32.0
	manifest := `{
   "schemaVersion": 1,
   "name": "library/busybox",
   "tag": "1.32.0",
   "architecture": "amd64",
   "fsLayers": [
      {
         "blobSum": "sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"
      },
      {
         "blobSum": "sha256:9758c28807f21c13d05c704821fdd56c0b9574912f9b916c65e1df3e6b8bc572"
      }
   ],
   "history": [
      {
         "v1Compatibility": "{\"architecture\":\"amd64\",\"config\":{\"Hostname\":\"\",\"Domainname\":\"\",\"User\":\"\",\"AttachStdin\":false,\"AttachStdout\":false,\"AttachStderr\":false,\"Tty\":false,\"OpenStdin\":false,\"StdinOnce\":false,\"Env\":[\"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\"],\"Cmd\":[\"sh\"],\"ArgsEscaped\":true,\"Image\":\"sha256:11565868e68267a053372359046e1e70ce095538e95ff8398defd49bb66ddfce\",\"Volumes\":null,\"WorkingDir\":\"\",\"Entrypoint\":null,\"OnBuild\":null,\"Labels\":null},\"container\":\"6f1f5d35fed541933daae185eac73e333818ccec0b0760eb4cc8e30ce8d69de6\",\"container_config\":{\"Hostname\":\"6f1f5d35fed5\",\"Domainname\":\"\",\"User\":\"\",\"AttachStdin\":false,\"AttachStdout\":false,\"AttachStderr\":false,\"Tty\":false,\"OpenStdin\":false,\"StdinOnce\":false,\"Env\":[\"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\"],\"Cmd\":[\"/bin/sh\",\"-c\",\"#(nop) \",\"CMD [\\\"sh\\\"]\"],\"ArgsEscaped\":true,\"Image\":\"sha256:11565868e68267a053372359046e1e70ce095538e95ff8398defd49bb66ddfce\",\"Volumes\":null,\"WorkingDir\":\"\",\"Entrypoint\":null,\"OnBuild\":null,\"Labels\":{}},\"created\":\"2020-10-14T10:07:34.124876277Z\",\"docker_version\":\"18.09.7\",\"id\":\"6ed978c75173f577f023843ea61461568332f466c963e1b088d81fe676e8816c\",\"os\":\"linux\",\"parent\":\"bf938fec00b8d83c6d28a66dd6aa1cf76384aec8e63c7771648007b0dfce6fd8\",\"throwaway\":true}"
      },
      {
         "v1Compatibility": "{\"id\":\"bf938fec00b8d83c6d28a66dd6aa1cf76384aec8e63c7771648007b0dfce6fd8\",\"created\":\"2020-10-14T10:07:33.97009658Z\",\"container_config\":{\"Cmd\":[\"/bin/sh -c #(nop) ADD file:6098f054f12a3651c41038294c56d4a8c5c5d477259386e75ae2af763e84e683 in / \"]}}"
      }
   ],
   "signatures": [
      {
         "header": {
            "jwk": {
               "crv": "P-256",
               "kid": "JOJV:JUGG:HTN3:ABNG:FF4D:KCTY:FESP:BHX7:45NZ:YTDE:NWEF:CK7F",
               "kty": "EC",
               "x": "xPD2kMN77NdX9MlaZcaIVv1MX-89ChYWgVJ_3MFAmVM",
               "y": "I5Qpk8KPtmGJLd77qlpvSEtJ4cKb9MjMqbD2Dp-FR7c"
            },
            "alg": "ES256"
         },
         "signature": "dLme4aiKt0EMdtRcCDox4Q5ntnBL5310_CyROeznc16cygwj6hWWXoUREo25423mWv19d8LtxEubXZcKIQBQRQ",
         "protected": "eyJmb3JtYXRMZW5ndGgiOjIxMjgsImZvcm1hdFRhaWwiOiJDbjAiLCJ0aW1lIjoiMjAyMC0xMC0yOFQxNToxMDo0OFoifQ"
      }
   ]
}`

	var deserializedManifest map[string]interface{}
	require.NoError(json.Unmarshal([]byte(manifest), &deserializedManifest))
	name := deserializedManifest["name"]
	tag := deserializedManifest["tag"]

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/manifests/{tag}", name), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifest)))
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v1+prettyjws")
		_, err := io.WriteString(w, manifest)
		require.NoError(err)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := newTestConfig(addr)
	client, err := NewTagClient(config)
	require.NoError(err)

	var b bytes.Buffer
	imageName := fmt.Sprintf("%s:%s", name, tag)
	require.NoError(client.Download(imageName, imageName, &b))
	require.Equal("sha256:ab2a79aa5da153ea944a10532c529579449afa693c668adcf034602b12eeb675", string(b.Bytes()))
}

func TestTagDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Get(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	r.Head(fmt.Sprintf("/v2/%s/manifests/{tag}", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := newTestConfig(addr)
	client, err := NewTagClient(config)
	require.NoError(err)

	_, err = client.Stat(tag, tag)
	require.Equal(backenderrors.ErrBlobNotFound, err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(tag, tag, &b))
}

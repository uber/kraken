package agentclient

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
)

func TestGetTag(t *testing.T) {
	digest := core.DigestFixture()
	tag := "latest"

	tests := []struct {
		desc       string
		handler    func(w http.ResponseWriter, r *http.Request)
		wantDigest core.Digest
		wantErr    bool
	}{
		{
			desc: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/tags/latest", r.URL.Path)
				_, err := fmt.Fprint(w, digest.String())
				require.NoError(t, err)
			},
			wantDigest: digest,
		},
		{
			desc: "tag not found",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			wantErr: true,
		},
		{
			desc: "internal server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr: true,
		},
		{
			desc: "invalid digest response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, err := fmt.Fprint(w, "invalid-digest")
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			desc: "read body error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "10")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte("123"))
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(test.handler))
			t.Cleanup(s.Close)

			c := New(s.Listener.Addr().String())
			d, err := c.GetTag(tag)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantDigest, d)
			}
		})
	}
}

func TestDownload(t *testing.T) {
	digest := core.DigestFixture()
	namespace := "test-namespace"
	content := "blob-content"

	tests := []struct {
		desc    string
		handler func(w http.ResponseWriter, r *http.Request)
		want    string
		wantErr bool
	}{
		{
			desc: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, fmt.Sprintf("/namespace/%s/blobs/%s", namespace, digest), r.URL.Path)
				_, err := fmt.Fprint(w, content)
				require.NoError(t, err)
			},
			want: content,
		},
		{
			desc: "error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(test.handler))
			t.Cleanup(s.Close)

			c := New(s.Listener.Addr().String())
			r, err := c.Download(namespace, digest)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				defer func() {
					require.NoError(t, r.Close())
				}()
				b, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, test.want, string(b))
			}
		})
	}
}

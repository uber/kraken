package hdfsbackend

import (
	"bytes"
	"errors"
	"path"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend/webhdfs"
	"code.uber.internal/infra/kraken/mocks/lib/backend/hdfsbackend/webhdfs"
	"code.uber.internal/infra/kraken/utils/mockutil"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type clientMocks struct {
	webhdfs *mockwebhdfs.MockClient
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)
	return &clientMocks{
		webhdfs: mockwebhdfs.NewMockClient(ctrl),
	}, ctrl.Finish
}

func (m *clientMocks) new() *Client {
	c, err := NewClient(Config{
		NameNodes:     []string{"some-name-node"},
		RootDirectory: "/root",
		NamePath:      "identity",
		testing:       true,
	}, WithWebHDFS(m.webhdfs))
	if err != nil {
		panic(err)
	}
	return c
}

func TestClientStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	mocks.webhdfs.EXPECT().GetFileStatus("/root/test").Return(webhdfs.FileStatus{Length: 32}, nil)

	info, err := client.Stat("test")
	require.NoError(err)
	require.Equal(core.NewBlobInfo(32), info)
}

func TestClientDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := randutil.Text(32)

	mocks.webhdfs.EXPECT().Open("/root/test", mockutil.MatchWriter(data)).Return(nil)

	var b bytes.Buffer
	require.NoError(client.Download("test", &b))
	require.Equal(data, b.Bytes())
}

func TestClientUpload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := randutil.Text(32)

	mocks.webhdfs.EXPECT().Create(
		mockutil.MatchRegex("/root/_uploads/.+"), mockutil.MatchReader(data)).Return(nil)

	mocks.webhdfs.EXPECT().Mkdirs("/root").Return(nil)

	mocks.webhdfs.EXPECT().Rename(mockutil.MatchRegex("/root/_uploads/.+"), "/root/test").Return(nil)

	require.NoError(client.Upload("test", bytes.NewReader(data)))
}

func TestClientList(t *testing.T) {
	// Tests against the following directory structure:
	//
	//	  root/
	//		foo/
	//		  bar.txt
	//        baz.txt
	//		  cats/
	//			meow.txt
	//      emtpy/

	tests := []struct {
		desc     string
		prefix   string
		expected []string
	}{
		{"root", "", []string{"foo/bar.txt", "foo/baz.txt", "foo/cats/meow.txt"}},
		{"directory", "foo/cats", []string{"foo/cats/meow.txt"}},
		{"emtpy directory", "empty", nil},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newClientMocks(t)
			defer cleanup()

			client := mocks.new()

			mocks.webhdfs.EXPECT().ListFileStatus("/root").Return([]webhdfs.FileStatus{{
				PathSuffix: "foo",
				Type:       "DIRECTORY",
			}, {
				PathSuffix: "empty",
				Type:       "DIRECTORY",
			}}, nil).MaxTimes(1)

			mocks.webhdfs.EXPECT().ListFileStatus("/root/foo").Return([]webhdfs.FileStatus{{
				PathSuffix: "bar.txt",
				Type:       "FILE",
			}, {
				PathSuffix: "baz.txt",
				Type:       "FILE",
			}, {
				PathSuffix: "cats",
				Type:       "DIRECTORY",
			}}, nil).MaxTimes(1)

			mocks.webhdfs.EXPECT().ListFileStatus("/root/foo/cats").Return([]webhdfs.FileStatus{{
				PathSuffix: "meow.txt",
				Type:       "FILE",
			}}, nil).MaxTimes(1)

			mocks.webhdfs.EXPECT().ListFileStatus("/root/empty").Return(nil, nil).MaxTimes(1)

			names, err := client.List(test.prefix)
			require.NoError(err)
			require.Equal(test.expected, names)
		})
	}
}

func genRandomDirs(n int) []webhdfs.FileStatus {
	var dirs []webhdfs.FileStatus
	for i := 0; i < n; i++ {
		dirs = append(dirs, webhdfs.FileStatus{
			PathSuffix: string(randutil.Text(6)),
			Type:       "DIRECTORY",
		})
	}
	return dirs
}

func initDirectoryTree(mocks *clientMocks, dir string, width, depth int) {
	if depth == 0 {
		mocks.webhdfs.EXPECT().ListFileStatus(dir).
			Return(nil, errors.New("some error")).MaxTimes(1)
		return
	}
	children := genRandomDirs(width)
	mocks.webhdfs.EXPECT().ListFileStatus(dir).Return(children, nil).MaxTimes(1)
	for _, c := range children {
		initDirectoryTree(mocks, path.Join(dir, c.PathSuffix), width, depth-1)
	}
}

func TestClientListErrorDoesNotLeakGoroutines(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	initDirectoryTree(mocks, "/root", 10, 3) // 1000 nodes.

	_, err := client.List("")
	require.Error(err)
}

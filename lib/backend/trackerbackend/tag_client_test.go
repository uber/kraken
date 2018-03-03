package trackerbackend

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/mocks/tracker/tagclient"
	"code.uber.internal/infra/kraken/tracker/tagclient"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDockerTagClientDownload(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocktagclient.NewMockClient(ctrl)

	c := DockerTagClient{client}

	name := "repo:tag"
	value := core.DigestFixture().String()

	client.EXPECT().Get(name).Return(value, nil)

	var b bytes.Buffer
	require.NoError(c.Download(name, &b))
	require.Equal(value, b.String())
}

func TestDockerTagClientDownloadNotFound(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocktagclient.NewMockClient(ctrl)

	c := DockerTagClient{client}

	name := "repo:tag"

	client.EXPECT().Get(name).Return("", tagclient.ErrNotFound)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, c.Download(name, &b))
}

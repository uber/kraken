package tagtype

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDefaultGetDependencies(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originClient := mockblobclient.NewMockClusterClient(ctrl)
	r := NewDefaultResolver(originClient)

	tag := "replicator/labrat"
	d := core.DigestFixture()
	deps, err := r.Resolve(tag, d)
	require.NoError(err)
	require.Equal(core.DigestList{d}, deps)
}

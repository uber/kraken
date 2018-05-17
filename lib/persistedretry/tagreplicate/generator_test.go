package tagreplicate

import (
	"testing"

	"github.com/uber-go/tally"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
)

func TestCreateSuccess(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originCluster := mockblobclient.NewMockClusterClient(ctrl)
	tagProvider := tagclient.NewTestProvider()
	sjc1 := mocktagclient.NewMockClient(ctrl)
	dca1 := mocktagclient.NewMockClient(ctrl)

	tagProvider.Register("build-index-sjc1", sjc1)
	tagProvider.Register("build-index-dca1", dca1)

	config := Config{
		"prime/.*": []string{
			"build-index-sjc1",
			"build-index-dca1",
		},
	}

	stats := tally.NoopScope
	tag := "prime/labrat"
	digest := core.DigestFixture()
	deps := core.DigestList{core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}

	task1 := NewTask(originCluster, tagProvider, stats, tag, "build-index-sjc1", digest, deps...)
	task2 := NewTask(originCluster, tagProvider, stats, tag, "build-index-dca1", digest, deps...)

	g, err := NewTaskGenerator(config, stats, originCluster, tagProvider)
	require.NoError(err)

	tasks, err := g.Create(tag, digest, deps...)
	require.NoError(err)
	require.Equal(2, len(tasks))
	EqualTask(t, *task1, *tasks[0])
	EqualTask(t, *task2, *tasks[1])

	task := NewTask(nil, nil, stats, tag, "build-index-sjc1", digest, deps...)
	err = g.Load(task)
	require.NoError(err)
	EqualTask(t, *task1, *task)

	require.True(g.IsValid(*task))
}

func TestCreateError(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originCluster := mockblobclient.NewMockClusterClient(ctrl)
	tagProvider := tagclient.NewTestProvider()
	sjc1 := mocktagclient.NewMockClient(ctrl)
	dca1 := mocktagclient.NewMockClient(ctrl)

	tagProvider.Register("build-index-sjc1", sjc1)
	tagProvider.Register("build-index-dca1", dca1)

	config := Config{
		"prime/.*": []string{
			"build-index-sjc1",
			"build-index-dca1",
		},
	}

	stats := tally.NoopScope
	tag := "base/labrat"
	digest := core.DigestFixture()
	deps := core.DigestList{core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}

	g, err := NewTaskGenerator(config, stats, originCluster, tagProvider)
	require.NoError(err)

	_, err = g.Create(tag, digest, deps...)
	require.Equal(ErrMatchingRemoteNotFound, err)

	task := NewTask(nil, nil, stats, tag, "build-index-sjc1", digest, deps...)
	err = g.Load(task)
	require.Equal(ErrMatchingRemoteNotFound, err)

	require.False(g.IsValid(*NewTask(originCluster, tagProvider, stats, tag, "build-index-sjc1", digest, deps...)))
}

// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/kraken/origin/blobclient (interfaces: Client,Provider,ClusterClient,ClientResolver)

// Package mockblobclient is a generated GoMock package.
package mockblobclient

import (
	image "code.uber.internal/infra/kraken/lib/dockerregistry/image"
	peercontext "code.uber.internal/infra/kraken/lib/peercontext"
	blobclient "code.uber.internal/infra/kraken/origin/blobclient"
	torlib "code.uber.internal/infra/kraken/torlib"
	gomock "github.com/golang/mock/gomock"
	io "io"
	reflect "reflect"
)

// MockClient is a mock of Client interface
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Addr mocks base method
func (m *MockClient) Addr() string {
	ret := m.ctrl.Call(m, "Addr")
	ret0, _ := ret[0].(string)
	return ret0
}

// Addr indicates an expected call of Addr
func (mr *MockClientMockRecorder) Addr() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Addr", reflect.TypeOf((*MockClient)(nil).Addr))
}

// CheckBlob mocks base method
func (m *MockClient) CheckBlob(arg0 image.Digest) (bool, error) {
	ret := m.ctrl.Call(m, "CheckBlob", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckBlob indicates an expected call of CheckBlob
func (mr *MockClientMockRecorder) CheckBlob(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckBlob", reflect.TypeOf((*MockClient)(nil).CheckBlob), arg0)
}

// DeleteBlob mocks base method
func (m *MockClient) DeleteBlob(arg0 image.Digest) error {
	ret := m.ctrl.Call(m, "DeleteBlob", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteBlob indicates an expected call of DeleteBlob
func (mr *MockClientMockRecorder) DeleteBlob(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteBlob", reflect.TypeOf((*MockClient)(nil).DeleteBlob), arg0)
}

// GetBlob mocks base method
func (m *MockClient) GetBlob(arg0 image.Digest) (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "GetBlob", arg0)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBlob indicates an expected call of GetBlob
func (mr *MockClientMockRecorder) GetBlob(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlob", reflect.TypeOf((*MockClient)(nil).GetBlob), arg0)
}

// GetMetaInfo mocks base method
func (m *MockClient) GetMetaInfo(arg0 string, arg1 image.Digest) (*torlib.MetaInfo, error) {
	ret := m.ctrl.Call(m, "GetMetaInfo", arg0, arg1)
	ret0, _ := ret[0].(*torlib.MetaInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetaInfo indicates an expected call of GetMetaInfo
func (mr *MockClientMockRecorder) GetMetaInfo(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetaInfo", reflect.TypeOf((*MockClient)(nil).GetMetaInfo), arg0, arg1)
}

// GetPeerContext mocks base method
func (m *MockClient) GetPeerContext() (peercontext.PeerContext, error) {
	ret := m.ctrl.Call(m, "GetPeerContext")
	ret0, _ := ret[0].(peercontext.PeerContext)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPeerContext indicates an expected call of GetPeerContext
func (mr *MockClientMockRecorder) GetPeerContext() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPeerContext", reflect.TypeOf((*MockClient)(nil).GetPeerContext))
}

// Locations mocks base method
func (m *MockClient) Locations(arg0 image.Digest) ([]string, error) {
	ret := m.ctrl.Call(m, "Locations", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Locations indicates an expected call of Locations
func (mr *MockClientMockRecorder) Locations(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Locations", reflect.TypeOf((*MockClient)(nil).Locations), arg0)
}

// PushBlob mocks base method
func (m *MockClient) PushBlob(arg0 image.Digest, arg1 io.Reader) error {
	ret := m.ctrl.Call(m, "PushBlob", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PushBlob indicates an expected call of PushBlob
func (mr *MockClientMockRecorder) PushBlob(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PushBlob", reflect.TypeOf((*MockClient)(nil).PushBlob), arg0, arg1)
}

// Repair mocks base method
func (m *MockClient) Repair() (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "Repair")
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Repair indicates an expected call of Repair
func (mr *MockClientMockRecorder) Repair() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Repair", reflect.TypeOf((*MockClient)(nil).Repair))
}

// RepairDigest mocks base method
func (m *MockClient) RepairDigest(arg0 image.Digest) (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "RepairDigest", arg0)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RepairDigest indicates an expected call of RepairDigest
func (mr *MockClientMockRecorder) RepairDigest(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RepairDigest", reflect.TypeOf((*MockClient)(nil).RepairDigest), arg0)
}

// RepairShard mocks base method
func (m *MockClient) RepairShard(arg0 string) (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "RepairShard", arg0)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RepairShard indicates an expected call of RepairShard
func (mr *MockClientMockRecorder) RepairShard(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RepairShard", reflect.TypeOf((*MockClient)(nil).RepairShard), arg0)
}

// UploadBlob mocks base method
func (m *MockClient) UploadBlob(arg0 string, arg1 image.Digest, arg2 io.Reader, arg3 bool) error {
	ret := m.ctrl.Call(m, "UploadBlob", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// UploadBlob indicates an expected call of UploadBlob
func (mr *MockClientMockRecorder) UploadBlob(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadBlob", reflect.TypeOf((*MockClient)(nil).UploadBlob), arg0, arg1, arg2, arg3)
}

// MockProvider is a mock of Provider interface
type MockProvider struct {
	ctrl     *gomock.Controller
	recorder *MockProviderMockRecorder
}

// MockProviderMockRecorder is the mock recorder for MockProvider
type MockProviderMockRecorder struct {
	mock *MockProvider
}

// NewMockProvider creates a new mock instance
func NewMockProvider(ctrl *gomock.Controller) *MockProvider {
	mock := &MockProvider{ctrl: ctrl}
	mock.recorder = &MockProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockProvider) EXPECT() *MockProviderMockRecorder {
	return m.recorder
}

// Provide mocks base method
func (m *MockProvider) Provide(arg0 string) blobclient.Client {
	ret := m.ctrl.Call(m, "Provide", arg0)
	ret0, _ := ret[0].(blobclient.Client)
	return ret0
}

// Provide indicates an expected call of Provide
func (mr *MockProviderMockRecorder) Provide(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Provide", reflect.TypeOf((*MockProvider)(nil).Provide), arg0)
}

// MockClusterClient is a mock of ClusterClient interface
type MockClusterClient struct {
	ctrl     *gomock.Controller
	recorder *MockClusterClientMockRecorder
}

// MockClusterClientMockRecorder is the mock recorder for MockClusterClient
type MockClusterClientMockRecorder struct {
	mock *MockClusterClient
}

// NewMockClusterClient creates a new mock instance
func NewMockClusterClient(ctrl *gomock.Controller) *MockClusterClient {
	mock := &MockClusterClient{ctrl: ctrl}
	mock.recorder = &MockClusterClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockClusterClient) EXPECT() *MockClusterClientMockRecorder {
	return m.recorder
}

// DownloadBlob mocks base method
func (m *MockClusterClient) DownloadBlob(arg0 image.Digest) (io.ReadCloser, error) {
	ret := m.ctrl.Call(m, "DownloadBlob", arg0)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DownloadBlob indicates an expected call of DownloadBlob
func (mr *MockClusterClientMockRecorder) DownloadBlob(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadBlob", reflect.TypeOf((*MockClusterClient)(nil).DownloadBlob), arg0)
}

// GetMetaInfo mocks base method
func (m *MockClusterClient) GetMetaInfo(arg0 string, arg1 image.Digest) (*torlib.MetaInfo, error) {
	ret := m.ctrl.Call(m, "GetMetaInfo", arg0, arg1)
	ret0, _ := ret[0].(*torlib.MetaInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetaInfo indicates an expected call of GetMetaInfo
func (mr *MockClusterClientMockRecorder) GetMetaInfo(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetaInfo", reflect.TypeOf((*MockClusterClient)(nil).GetMetaInfo), arg0, arg1)
}

// Owners mocks base method
func (m *MockClusterClient) Owners(arg0 image.Digest) ([]peercontext.PeerContext, error) {
	ret := m.ctrl.Call(m, "Owners", arg0)
	ret0, _ := ret[0].([]peercontext.PeerContext)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Owners indicates an expected call of Owners
func (mr *MockClusterClientMockRecorder) Owners(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Owners", reflect.TypeOf((*MockClusterClient)(nil).Owners), arg0)
}

// UploadBlob mocks base method
func (m *MockClusterClient) UploadBlob(arg0 string, arg1 image.Digest, arg2 io.Reader) error {
	ret := m.ctrl.Call(m, "UploadBlob", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// UploadBlob indicates an expected call of UploadBlob
func (mr *MockClusterClientMockRecorder) UploadBlob(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadBlob", reflect.TypeOf((*MockClusterClient)(nil).UploadBlob), arg0, arg1, arg2)
}

// UploadBlobThrough mocks base method
func (m *MockClusterClient) UploadBlobThrough(arg0 string, arg1 image.Digest, arg2 io.Reader) error {
	ret := m.ctrl.Call(m, "UploadBlobThrough", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// UploadBlobThrough indicates an expected call of UploadBlobThrough
func (mr *MockClusterClientMockRecorder) UploadBlobThrough(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadBlobThrough", reflect.TypeOf((*MockClusterClient)(nil).UploadBlobThrough), arg0, arg1, arg2)
}

// MockClientResolver is a mock of ClientResolver interface
type MockClientResolver struct {
	ctrl     *gomock.Controller
	recorder *MockClientResolverMockRecorder
}

// MockClientResolverMockRecorder is the mock recorder for MockClientResolver
type MockClientResolverMockRecorder struct {
	mock *MockClientResolver
}

// NewMockClientResolver creates a new mock instance
func NewMockClientResolver(ctrl *gomock.Controller) *MockClientResolver {
	mock := &MockClientResolver{ctrl: ctrl}
	mock.recorder = &MockClientResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockClientResolver) EXPECT() *MockClientResolverMockRecorder {
	return m.recorder
}

// Resolve mocks base method
func (m *MockClientResolver) Resolve(arg0 image.Digest) ([]blobclient.Client, error) {
	ret := m.ctrl.Call(m, "Resolve", arg0)
	ret0, _ := ret[0].([]blobclient.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Resolve indicates an expected call of Resolve
func (mr *MockClientResolverMockRecorder) Resolve(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Resolve", reflect.TypeOf((*MockClientResolver)(nil).Resolve), arg0)
}

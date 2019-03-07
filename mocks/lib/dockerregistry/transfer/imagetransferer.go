// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/kraken/lib/dockerregistry/transfer (interfaces: ImageTransferer)

// Package mocktransfer is a generated GoMock package.
package mocktransfer

import (
	gomock "github.com/golang/mock/gomock"
	core "github.com/uber/kraken/core"
	base "github.com/uber/kraken/lib/store/base"
	reflect "reflect"
)

// MockImageTransferer is a mock of ImageTransferer interface
type MockImageTransferer struct {
	ctrl     *gomock.Controller
	recorder *MockImageTransfererMockRecorder
}

// MockImageTransfererMockRecorder is the mock recorder for MockImageTransferer
type MockImageTransfererMockRecorder struct {
	mock *MockImageTransferer
}

// NewMockImageTransferer creates a new mock instance
func NewMockImageTransferer(ctrl *gomock.Controller) *MockImageTransferer {
	mock := &MockImageTransferer{ctrl: ctrl}
	mock.recorder = &MockImageTransfererMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockImageTransferer) EXPECT() *MockImageTransfererMockRecorder {
	return m.recorder
}

// Download mocks base method
func (m *MockImageTransferer) Download(arg0 string, arg1 core.Digest) (base.FileReader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Download", arg0, arg1)
	ret0, _ := ret[0].(base.FileReader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Download indicates an expected call of Download
func (mr *MockImageTransfererMockRecorder) Download(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*MockImageTransferer)(nil).Download), arg0, arg1)
}

// GetTag mocks base method
func (m *MockImageTransferer) GetTag(arg0 string) (core.Digest, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTag", arg0)
	ret0, _ := ret[0].(core.Digest)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTag indicates an expected call of GetTag
func (mr *MockImageTransfererMockRecorder) GetTag(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTag", reflect.TypeOf((*MockImageTransferer)(nil).GetTag), arg0)
}

// ListTags mocks base method
func (m *MockImageTransferer) ListTags(arg0 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTags", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTags indicates an expected call of ListTags
func (mr *MockImageTransfererMockRecorder) ListTags(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTags", reflect.TypeOf((*MockImageTransferer)(nil).ListTags), arg0)
}

// PutTag mocks base method
func (m *MockImageTransferer) PutTag(arg0 string, arg1 core.Digest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutTag", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PutTag indicates an expected call of PutTag
func (mr *MockImageTransfererMockRecorder) PutTag(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutTag", reflect.TypeOf((*MockImageTransferer)(nil).PutTag), arg0, arg1)
}

// Stat mocks base method
func (m *MockImageTransferer) Stat(arg0 string, arg1 core.Digest) (*core.BlobInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stat", arg0, arg1)
	ret0, _ := ret[0].(*core.BlobInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Stat indicates an expected call of Stat
func (mr *MockImageTransfererMockRecorder) Stat(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stat", reflect.TypeOf((*MockImageTransferer)(nil).Stat), arg0, arg1)
}

// Upload mocks base method
func (m *MockImageTransferer) Upload(arg0 string, arg1 core.Digest, arg2 base.FileReader) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Upload", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Upload indicates an expected call of Upload
func (mr *MockImageTransfererMockRecorder) Upload(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Upload", reflect.TypeOf((*MockImageTransferer)(nil).Upload), arg0, arg1, arg2)
}

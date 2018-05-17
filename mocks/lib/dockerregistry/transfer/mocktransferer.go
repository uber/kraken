// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/kraken/lib/dockerregistry/transfer (interfaces: ImageTransferer)

// Package mocktransferer is a generated GoMock package.
package mocktransferer

import (
	core "code.uber.internal/infra/kraken/core"
	base "code.uber.internal/infra/kraken/lib/store/base"
	gomock "github.com/golang/mock/gomock"
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
	ret := m.ctrl.Call(m, "Download", arg0, arg1)
	ret0, _ := ret[0].(base.FileReader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Download indicates an expected call of Download
func (mr *MockImageTransfererMockRecorder) Download(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*MockImageTransferer)(nil).Download), arg0, arg1)
}

// GetTag mocks base method
func (m *MockImageTransferer) GetTag(arg0 string) (core.Digest, error) {
	ret := m.ctrl.Call(m, "GetTag", arg0)
	ret0, _ := ret[0].(core.Digest)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTag indicates an expected call of GetTag
func (mr *MockImageTransfererMockRecorder) GetTag(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTag", reflect.TypeOf((*MockImageTransferer)(nil).GetTag), arg0)
}

// PostTag mocks base method
func (m *MockImageTransferer) PostTag(arg0 string, arg1 core.Digest) error {
	ret := m.ctrl.Call(m, "PostTag", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PostTag indicates an expected call of PostTag
func (mr *MockImageTransfererMockRecorder) PostTag(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PostTag", reflect.TypeOf((*MockImageTransferer)(nil).PostTag), arg0, arg1)
}

// Upload mocks base method
func (m *MockImageTransferer) Upload(arg0 string, arg1 core.Digest, arg2 base.FileReader) error {
	ret := m.ctrl.Call(m, "Upload", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Upload indicates an expected call of Upload
func (mr *MockImageTransfererMockRecorder) Upload(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Upload", reflect.TypeOf((*MockImageTransferer)(nil).Upload), arg0, arg1, arg2)
}

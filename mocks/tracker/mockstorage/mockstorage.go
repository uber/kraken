// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/kraken/tracker/storage (interfaces: Storage)

// Package mockstorage is a generated GoMock package.
package mockstorage

import (
	core "code.uber.internal/infra/kraken/core"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockStorage is a mock of Storage interface
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
}

// MockStorageMockRecorder is the mock recorder for MockStorage
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// GetMetaInfo mocks base method
func (m *MockStorage) GetMetaInfo(arg0 string) ([]byte, error) {
	ret := m.ctrl.Call(m, "GetMetaInfo", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetaInfo indicates an expected call of GetMetaInfo
func (mr *MockStorageMockRecorder) GetMetaInfo(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetaInfo", reflect.TypeOf((*MockStorage)(nil).GetMetaInfo), arg0)
}

// GetOrigins mocks base method
func (m *MockStorage) GetOrigins(arg0 string) ([]*core.PeerInfo, error) {
	ret := m.ctrl.Call(m, "GetOrigins", arg0)
	ret0, _ := ret[0].([]*core.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOrigins indicates an expected call of GetOrigins
func (mr *MockStorageMockRecorder) GetOrigins(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrigins", reflect.TypeOf((*MockStorage)(nil).GetOrigins), arg0)
}

// GetPeers mocks base method
func (m *MockStorage) GetPeers(arg0 string) ([]*core.PeerInfo, error) {
	ret := m.ctrl.Call(m, "GetPeers", arg0)
	ret0, _ := ret[0].([]*core.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPeers indicates an expected call of GetPeers
func (mr *MockStorageMockRecorder) GetPeers(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPeers", reflect.TypeOf((*MockStorage)(nil).GetPeers), arg0)
}

// SetMetaInfo mocks base method
func (m *MockStorage) SetMetaInfo(arg0 *core.MetaInfo) error {
	ret := m.ctrl.Call(m, "SetMetaInfo", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetMetaInfo indicates an expected call of SetMetaInfo
func (mr *MockStorageMockRecorder) SetMetaInfo(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMetaInfo", reflect.TypeOf((*MockStorage)(nil).SetMetaInfo), arg0)
}

// UpdateOrigins mocks base method
func (m *MockStorage) UpdateOrigins(arg0 string, arg1 []*core.PeerInfo) error {
	ret := m.ctrl.Call(m, "UpdateOrigins", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateOrigins indicates an expected call of UpdateOrigins
func (mr *MockStorageMockRecorder) UpdateOrigins(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateOrigins", reflect.TypeOf((*MockStorage)(nil).UpdateOrigins), arg0, arg1)
}

// UpdatePeer mocks base method
func (m *MockStorage) UpdatePeer(arg0 *core.PeerInfo) error {
	ret := m.ctrl.Call(m, "UpdatePeer", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdatePeer indicates an expected call of UpdatePeer
func (mr *MockStorageMockRecorder) UpdatePeer(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdatePeer", reflect.TypeOf((*MockStorage)(nil).UpdatePeer), arg0)
}

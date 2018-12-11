// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/kraken/tracker/originstore (interfaces: Store)

// Package mockoriginstore is a generated GoMock package.
package mockoriginstore

import (
	core "github.com/uber/kraken/core"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockStore is a mock of Store interface
type MockStore struct {
	ctrl     *gomock.Controller
	recorder *MockStoreMockRecorder
}

// MockStoreMockRecorder is the mock recorder for MockStore
type MockStoreMockRecorder struct {
	mock *MockStore
}

// NewMockStore creates a new mock instance
func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &MockStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStore) EXPECT() *MockStoreMockRecorder {
	return m.recorder
}

// GetOrigins mocks base method
func (m *MockStore) GetOrigins(arg0 core.Digest) ([]*core.PeerInfo, error) {
	ret := m.ctrl.Call(m, "GetOrigins", arg0)
	ret0, _ := ret[0].([]*core.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOrigins indicates an expected call of GetOrigins
func (mr *MockStoreMockRecorder) GetOrigins(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrigins", reflect.TypeOf((*MockStore)(nil).GetOrigins), arg0)
}

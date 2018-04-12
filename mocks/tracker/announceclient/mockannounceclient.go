// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/kraken/tracker/announceclient (interfaces: Client)

// Package mockannounceclient is a generated GoMock package.
package mockannounceclient

import (
	core "code.uber.internal/infra/kraken/core"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
	time "time"
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

// Announce mocks base method
func (m *MockClient) Announce(arg0 string, arg1 core.InfoHash, arg2 bool) ([]*core.PeerInfo, time.Duration, error) {
	ret := m.ctrl.Call(m, "Announce", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*core.PeerInfo)
	ret1, _ := ret[1].(time.Duration)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Announce indicates an expected call of Announce
func (mr *MockClientMockRecorder) Announce(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Announce", reflect.TypeOf((*MockClient)(nil).Announce), arg0, arg1, arg2)
}

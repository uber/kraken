// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/kraken/lib/dockerdaemon (interfaces: DockerClient)

// Package mockdockerdaemon is a generated GoMock package.
package mockdockerdaemon

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockDockerClient is a mock of DockerClient interface
type MockDockerClient struct {
	ctrl     *gomock.Controller
	recorder *MockDockerClientMockRecorder
}

// MockDockerClientMockRecorder is the mock recorder for MockDockerClient
type MockDockerClientMockRecorder struct {
	mock *MockDockerClient
}

// NewMockDockerClient creates a new mock instance
func NewMockDockerClient(ctrl *gomock.Controller) *MockDockerClient {
	mock := &MockDockerClient{ctrl: ctrl}
	mock.recorder = &MockDockerClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockDockerClient) EXPECT() *MockDockerClientMockRecorder {
	return m.recorder
}

// PullImage mocks base method
func (m *MockDockerClient) PullImage(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PullImage", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// PullImage indicates an expected call of PullImage
func (mr *MockDockerClientMockRecorder) PullImage(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PullImage", reflect.TypeOf((*MockDockerClient)(nil).PullImage), arg0, arg1, arg2)
}

// Code generated by MockGen. DO NOT EDIT.
// Source: code.uber.internal/infra/kraken/build-index/tagtype (interfaces: DependencyResolver)

// Package mocktagtype is a generated GoMock package.
package mocktagtype

import (
	core "code.uber.internal/infra/kraken/core"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockDependencyResolver is a mock of DependencyResolver interface
type MockDependencyResolver struct {
	ctrl     *gomock.Controller
	recorder *MockDependencyResolverMockRecorder
}

// MockDependencyResolverMockRecorder is the mock recorder for MockDependencyResolver
type MockDependencyResolverMockRecorder struct {
	mock *MockDependencyResolver
}

// NewMockDependencyResolver creates a new mock instance
func NewMockDependencyResolver(ctrl *gomock.Controller) *MockDependencyResolver {
	mock := &MockDependencyResolver{ctrl: ctrl}
	mock.recorder = &MockDependencyResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockDependencyResolver) EXPECT() *MockDependencyResolverMockRecorder {
	return m.recorder
}

// Resolve mocks base method
func (m *MockDependencyResolver) Resolve(arg0 string, arg1 core.Digest) (core.DigestList, error) {
	ret := m.ctrl.Call(m, "Resolve", arg0, arg1)
	ret0, _ := ret[0].(core.DigestList)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Resolve indicates an expected call of Resolve
func (mr *MockDependencyResolverMockRecorder) Resolve(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Resolve", reflect.TypeOf((*MockDependencyResolver)(nil).Resolve), arg0, arg1)
}

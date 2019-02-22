// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/kraken/tracker/originstore (interfaces: Store)

// Package mockoriginstore is a generated GoMock package.
package mockoriginstore

import (
	gomock "github.com/golang/mock/gomock"
	core "github.com/uber/kraken/core"
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
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrigins", arg0)
	ret0, _ := ret[0].([]*core.PeerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOrigins indicates an expected call of GetOrigins
func (mr *MockStoreMockRecorder) GetOrigins(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrigins", reflect.TypeOf((*MockStore)(nil).GetOrigins), arg0)
}

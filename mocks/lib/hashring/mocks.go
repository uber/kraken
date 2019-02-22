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
// limitations under the License.// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/uber/kraken/lib/hashring (interfaces: Ring,Watcher)

// Package mockhashring is a generated GoMock package.
package mockhashring

import (
	gomock "github.com/golang/mock/gomock"
	core "github.com/uber/kraken/core"
	stringset "github.com/uber/kraken/utils/stringset"
	reflect "reflect"
)

// MockRing is a mock of Ring interface
type MockRing struct {
	ctrl     *gomock.Controller
	recorder *MockRingMockRecorder
}

// MockRingMockRecorder is the mock recorder for MockRing
type MockRingMockRecorder struct {
	mock *MockRing
}

// NewMockRing creates a new mock instance
func NewMockRing(ctrl *gomock.Controller) *MockRing {
	mock := &MockRing{ctrl: ctrl}
	mock.recorder = &MockRingMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRing) EXPECT() *MockRingMockRecorder {
	return m.recorder
}

// Contains mocks base method
func (m *MockRing) Contains(arg0 string) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Contains", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Contains indicates an expected call of Contains
func (mr *MockRingMockRecorder) Contains(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Contains", reflect.TypeOf((*MockRing)(nil).Contains), arg0)
}

// Locations mocks base method
func (m *MockRing) Locations(arg0 core.Digest) []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Locations", arg0)
	ret0, _ := ret[0].([]string)
	return ret0
}

// Locations indicates an expected call of Locations
func (mr *MockRingMockRecorder) Locations(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Locations", reflect.TypeOf((*MockRing)(nil).Locations), arg0)
}

// Monitor mocks base method
func (m *MockRing) Monitor(arg0 <-chan struct{}) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Monitor", arg0)
}

// Monitor indicates an expected call of Monitor
func (mr *MockRingMockRecorder) Monitor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Monitor", reflect.TypeOf((*MockRing)(nil).Monitor), arg0)
}

// Refresh mocks base method
func (m *MockRing) Refresh() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Refresh")
}

// Refresh indicates an expected call of Refresh
func (mr *MockRingMockRecorder) Refresh() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Refresh", reflect.TypeOf((*MockRing)(nil).Refresh))
}

// MockWatcher is a mock of Watcher interface
type MockWatcher struct {
	ctrl     *gomock.Controller
	recorder *MockWatcherMockRecorder
}

// MockWatcherMockRecorder is the mock recorder for MockWatcher
type MockWatcherMockRecorder struct {
	mock *MockWatcher
}

// NewMockWatcher creates a new mock instance
func NewMockWatcher(ctrl *gomock.Controller) *MockWatcher {
	mock := &MockWatcher{ctrl: ctrl}
	mock.recorder = &MockWatcherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWatcher) EXPECT() *MockWatcherMockRecorder {
	return m.recorder
}

// Notify mocks base method
func (m *MockWatcher) Notify(arg0 stringset.Set) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Notify", arg0)
}

// Notify indicates an expected call of Notify
func (mr *MockWatcherMockRecorder) Notify(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Notify", reflect.TypeOf((*MockWatcher)(nil).Notify), arg0)
}

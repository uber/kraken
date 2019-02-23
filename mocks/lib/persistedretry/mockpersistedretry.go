// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
// Source: github.com/uber/kraken/lib/persistedretry (interfaces: Store,Task,Executor,Manager)

// Package mockpersistedretry is a generated GoMock package.
package mockpersistedretry

import (
	gomock "github.com/golang/mock/gomock"
	persistedretry "github.com/uber/kraken/lib/persistedretry"
	reflect "reflect"
	time "time"
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

// AddFailed mocks base method
func (m *MockStore) AddFailed(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddFailed", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddFailed indicates an expected call of AddFailed
func (mr *MockStoreMockRecorder) AddFailed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddFailed", reflect.TypeOf((*MockStore)(nil).AddFailed), arg0)
}

// AddPending mocks base method
func (m *MockStore) AddPending(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddPending", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddPending indicates an expected call of AddPending
func (mr *MockStoreMockRecorder) AddPending(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddPending", reflect.TypeOf((*MockStore)(nil).AddPending), arg0)
}

// Find mocks base method
func (m *MockStore) Find(arg0 interface{}) ([]persistedretry.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Find", arg0)
	ret0, _ := ret[0].([]persistedretry.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Find indicates an expected call of Find
func (mr *MockStoreMockRecorder) Find(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Find", reflect.TypeOf((*MockStore)(nil).Find), arg0)
}

// GetFailed mocks base method
func (m *MockStore) GetFailed() ([]persistedretry.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFailed")
	ret0, _ := ret[0].([]persistedretry.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFailed indicates an expected call of GetFailed
func (mr *MockStoreMockRecorder) GetFailed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFailed", reflect.TypeOf((*MockStore)(nil).GetFailed))
}

// GetPending mocks base method
func (m *MockStore) GetPending() ([]persistedretry.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPending")
	ret0, _ := ret[0].([]persistedretry.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPending indicates an expected call of GetPending
func (mr *MockStoreMockRecorder) GetPending() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPending", reflect.TypeOf((*MockStore)(nil).GetPending))
}

// MarkFailed mocks base method
func (m *MockStore) MarkFailed(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkFailed", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkFailed indicates an expected call of MarkFailed
func (mr *MockStoreMockRecorder) MarkFailed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkFailed", reflect.TypeOf((*MockStore)(nil).MarkFailed), arg0)
}

// MarkPending mocks base method
func (m *MockStore) MarkPending(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkPending", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkPending indicates an expected call of MarkPending
func (mr *MockStoreMockRecorder) MarkPending(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkPending", reflect.TypeOf((*MockStore)(nil).MarkPending), arg0)
}

// Remove mocks base method
func (m *MockStore) Remove(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Remove", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Remove indicates an expected call of Remove
func (mr *MockStoreMockRecorder) Remove(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Remove", reflect.TypeOf((*MockStore)(nil).Remove), arg0)
}

// MockTask is a mock of Task interface
type MockTask struct {
	ctrl     *gomock.Controller
	recorder *MockTaskMockRecorder
}

// MockTaskMockRecorder is the mock recorder for MockTask
type MockTaskMockRecorder struct {
	mock *MockTask
}

// NewMockTask creates a new mock instance
func NewMockTask(ctrl *gomock.Controller) *MockTask {
	mock := &MockTask{ctrl: ctrl}
	mock.recorder = &MockTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockTask) EXPECT() *MockTaskMockRecorder {
	return m.recorder
}

// GetFailures mocks base method
func (m *MockTask) GetFailures() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFailures")
	ret0, _ := ret[0].(int)
	return ret0
}

// GetFailures indicates an expected call of GetFailures
func (mr *MockTaskMockRecorder) GetFailures() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFailures", reflect.TypeOf((*MockTask)(nil).GetFailures))
}

// GetLastAttempt mocks base method
func (m *MockTask) GetLastAttempt() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLastAttempt")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// GetLastAttempt indicates an expected call of GetLastAttempt
func (mr *MockTaskMockRecorder) GetLastAttempt() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLastAttempt", reflect.TypeOf((*MockTask)(nil).GetLastAttempt))
}

// Ready mocks base method
func (m *MockTask) Ready() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ready")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Ready indicates an expected call of Ready
func (mr *MockTaskMockRecorder) Ready() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ready", reflect.TypeOf((*MockTask)(nil).Ready))
}

// MockExecutor is a mock of Executor interface
type MockExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockExecutorMockRecorder
}

// MockExecutorMockRecorder is the mock recorder for MockExecutor
type MockExecutorMockRecorder struct {
	mock *MockExecutor
}

// NewMockExecutor creates a new mock instance
func NewMockExecutor(ctrl *gomock.Controller) *MockExecutor {
	mock := &MockExecutor{ctrl: ctrl}
	mock.recorder = &MockExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockExecutor) EXPECT() *MockExecutorMockRecorder {
	return m.recorder
}

// Exec mocks base method
func (m *MockExecutor) Exec(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exec", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Exec indicates an expected call of Exec
func (mr *MockExecutorMockRecorder) Exec(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockExecutor)(nil).Exec), arg0)
}

// Name mocks base method
func (m *MockExecutor) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name
func (mr *MockExecutorMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockExecutor)(nil).Name))
}

// MockManager is a mock of Manager interface
type MockManager struct {
	ctrl     *gomock.Controller
	recorder *MockManagerMockRecorder
}

// MockManagerMockRecorder is the mock recorder for MockManager
type MockManagerMockRecorder struct {
	mock *MockManager
}

// NewMockManager creates a new mock instance
func NewMockManager(ctrl *gomock.Controller) *MockManager {
	mock := &MockManager{ctrl: ctrl}
	mock.recorder = &MockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockManager) EXPECT() *MockManagerMockRecorder {
	return m.recorder
}

// Add mocks base method
func (m *MockManager) Add(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Add", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Add indicates an expected call of Add
func (mr *MockManagerMockRecorder) Add(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockManager)(nil).Add), arg0)
}

// Close mocks base method
func (m *MockManager) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close
func (mr *MockManagerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockManager)(nil).Close))
}

// Find mocks base method
func (m *MockManager) Find(arg0 interface{}) ([]persistedretry.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Find", arg0)
	ret0, _ := ret[0].([]persistedretry.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Find indicates an expected call of Find
func (mr *MockManagerMockRecorder) Find(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Find", reflect.TypeOf((*MockManager)(nil).Find), arg0)
}

// SyncExec mocks base method
func (m *MockManager) SyncExec(arg0 persistedretry.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncExec", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncExec indicates an expected call of SyncExec
func (mr *MockManagerMockRecorder) SyncExec(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncExec", reflect.TypeOf((*MockManager)(nil).SyncExec), arg0)
}

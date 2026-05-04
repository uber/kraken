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

package log

import (
	"sync/atomic"

	"go.uber.org/zap"
)

// Level is a verbosity level used with V(). Higher values are more verbose.
type Level int32

var _verbosity int32 // atomic; default 0 = disabled

// SetVerbosity sets the global V() verbosity threshold at runtime.
func SetVerbosity(l Level) {
	atomic.StoreInt32(&_verbosity, int32(l))
}

// Verbose is returned by V(). Logging methods are no-ops when the requested
// level exceeds the current verbosity threshold.
type Verbose struct {
	enabled bool
}

// Enabled reports whether this verbosity level is active.
func (v Verbose) Enabled() bool { return v.enabled }

// Info calls the global Info when enabled.
func (v Verbose) Info(args ...interface{}) {
	if v.enabled {
		Info(args...)
	}
}

// Infof calls the global Infof when enabled.
func (v Verbose) Infof(format string, args ...interface{}) {
	if v.enabled {
		Infof(format, args...)
	}
}

// InfoS calls the global InfoS when enabled.
func (v Verbose) InfoS(msg string, keysAndValues ...interface{}) {
	if v.enabled {
		InfoS(msg, keysAndValues...)
	}
}

// V returns a Verbose for the given level, mirroring klog.V.
//
//	log.V(2).Info("detailed trace")
//	log.V(4).InfoS("very verbose", "key", val)
func V(level Level) Verbose {
	return Verbose{enabled: level <= Level(atomic.LoadInt32(&_verbosity))}
}

// InfoS structured-logs to INFO with key-value pairs, mirroring klog.InfoS.
//
//	log.InfoS("pod updated", "pod", name, "namespace", ns)
func InfoS(msg string, keysAndValues ...interface{}) {
	zap.S().Infow(msg, keysAndValues...)
}

// ErrorS structured-logs to ERROR with an error and key-value pairs,
// mirroring klog.ErrorS.
//
//	log.ErrorS(err, "dial failed", "host", host, "attempt", n)
func ErrorS(err error, msg string, keysAndValues ...interface{}) {
	zap.S().With(zap.Error(err)).Errorw(msg, keysAndValues...)
}

// WithValues returns a child logger that always emits the given key-value pairs.
//
//	reqLog := log.WithValues("traceID", id)
//	reqLog.Info("handled")
func WithValues(keysAndValues ...interface{}) *zap.SugaredLogger {
	return zap.S().With(keysAndValues...)
}

// WithName returns a child logger with the given name attached.
//
//	sched := log.WithName("scheduler")
//	sched.Info("tick")
func WithName(name string) *zap.SugaredLogger {
	return zap.S().Named(name)
}

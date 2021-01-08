// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package sqlbackend

// Config is used to initialize the SQL Backend Client
type Config struct {
	DebugLogging     bool   `yaml:"debug_logging"`
	Dialect          string `yaml:"dialect"`
	ConnectionString string `yaml:"connection_string"`
	Username         string `yaml:"username"`
}

// UserAuthConfig defines authentication configuration overlayed by Langley/Vault.
// Each key is the iam username of the credentials.
type UserAuthConfig map[string]AuthConfig

// SQL is a struct that holds credentials. This is declared here to make testing easier
type SQL struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// AuthConfig matches Langley format.
type AuthConfig struct {
	SQL SQL `yaml:"sql"`
}

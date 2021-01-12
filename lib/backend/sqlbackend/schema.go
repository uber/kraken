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

import "time"

// Tag represents a Docker tag
type Tag struct {
	ID         uint64 `gorm:"primary_key;auto_increment:true"`
	Repository string `gorm:"not null;type:varchar(255);index:repository;unique_index:repository_tag"`
	Tag        string `gorm:"not null;type:varchar(128);unique_index:repository_tag"`
	ImageID    string `gorm:"not null;type:varchar(2056)"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

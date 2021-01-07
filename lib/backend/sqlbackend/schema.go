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

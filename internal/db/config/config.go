package config

import (
	"gorm.io/gorm"

	"github.com/gromitlee/go-dtf/pkg/dtf_config"
)

type Config struct {
	Log dtf_config.Logger
	DB  *gorm.DB
}

const (
	CompensateRetryInterval int = 5  // second
	SAGASHeartbeatInterval  int = 30 // second
	CleanTimeout            int = 60 // second
	CleanInterval           int = 30 // second
)

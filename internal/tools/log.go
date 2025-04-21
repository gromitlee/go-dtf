package tools

import (
	"log"

	"github.com/gromitlee/go-dtf/pkg/dtf_config"
)

func DefaultLog() dtf_config.Logger {
	return &defaultLog{}
}

func EmptyLog() dtf_config.Logger { return &emptyLog{} }

// --- default log ---

type defaultLog struct{}

func (l *defaultLog) Debugf(fmt string, i ...interface{}) {
	log.Printf("[DTF][DEBUG] "+fmt, i...)
}

func (l *defaultLog) Infof(fmt string, i ...interface{}) {
	log.Printf("[DTF][INFO] "+fmt, i...)
}

func (l *defaultLog) Warnf(fmt string, i ...interface{}) {
	log.Printf("[DTF][WARN] "+fmt, i...)
}

func (l *defaultLog) Errorf(fmt string, i ...interface{}) {
	log.Printf("[DTF][ERROR] "+fmt, i...)
}

// --- empty log ---

type emptyLog struct{}

func (l *emptyLog) Debugf(fmt string, i ...interface{}) {
}

func (l *emptyLog) Infof(fmt string, i ...interface{}) {
}

func (l *emptyLog) Warnf(fmt string, i ...interface{}) {
}

func (l *emptyLog) Errorf(fmt string, i ...interface{}) {
}

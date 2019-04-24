// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
)

// Log prints log only after a certain amount of time
type Log struct {
	sync.RWMutex
	lastTime map[string]time.Time
	interval map[string]time.Duration
}

// NewLog returns a new Log
func NewLog() *Log {
	return &Log{
		lastTime: make(map[string]time.Time),
		interval: make(map[string]time.Duration),
	}
}

// Add adds new label
func (l *Log) Add(label string, interval time.Duration) {
	l.Lock()
	l.interval[label] = interval
	l.Unlock()
}

// Print executes the fn to print log
func (l *Log) Print(label string, fn func()) {
	l.Lock()
	defer l.Unlock()

	_, ok := l.lastTime[label]
	if !ok || time.Since(l.lastTime[label]) > l.interval[label] {
		fn()
		l.lastTime[label] = time.Now()
	}
}

// InitLogger initalizes logger
func InitLogger(level string, file string, rotate string) {
	log.SetLevelByString(level)

	if len(file) > 0 {
		log.SetOutputByName(file)

		if rotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}

	sarama.Logger = NewStdLogger("[sarama] ")
}
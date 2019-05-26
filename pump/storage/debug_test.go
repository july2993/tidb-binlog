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

package storage

import (
	"path"
	"time"

	"github.com/pingcap/check"
	"github.com/syndtr/goleveldb/leveldb"
)

type debugSuite struct{}

var _ = check.Suite(&debugSuite{})

func (e *EncodeTSKeySuite) TestResetHeadPointer(c *check.C) {
	storage := newAppend(c)
	defer cleanAppend(storage)

	populateBinlog(c, storage, 10, 20)

	time.Sleep(time.Second)

	headPointer := storage.headPointer
	handlePointer := storage.handlePointer

	var err error
	var zeroPointer valuePointer
	err = storage.savePointer(headPointerKey, zeroPointer)
	c.Assert(err, check.IsNil)

	err = storage.savePointer(handlePointerKey, zeroPointer)
	c.Assert(err, check.IsNil)

	err = storage.Close()
	c.Assert(err, check.IsNil)

	err = ResetHeadpointer(storage.dir)
	c.Assert(err, check.IsNil)

	db, err := leveldb.OpenFile(path.Join(storage.dir, "kv"), nil)
	c.Assert(err, check.IsNil)

	afterHeadPointer, err := readPointer(db, headPointerKey)
	c.Assert(err, check.IsNil)
	c.Assert(afterHeadPointer, check.Equals, headPointer)

	afterHandlePointer, err := readPointer(db, handlePointerKey)
	c.Assert(err, check.IsNil)
	c.Assert(afterHandlePointer, check.Equals, handlePointer)
}

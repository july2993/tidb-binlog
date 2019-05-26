package storage

import (
	"path"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

func readPointer(db *leveldb.DB, key []byte) (valuePointer, error) {
	var vp valuePointer
	value, err := db.Get(key, nil)
	if err != nil {
		// return zero value when not found
		if err == leveldb.ErrNotFound {
			return vp, nil
		}

		return vp, errors.Trace(err)
	}
	err = vp.UnmarshalBinary(value)
	if err != nil {
		return vp, errors.Trace(err)
	}

	return vp, nil
}

// ResetHeadpointer reset headPointer and handlePointer as the last pointer of vlog
// will make sure all binlog are write to meta db
func ResetHeadpointer(dir string) error {
	kvDir := path.Join(dir, "kv")
	db, err := openMetadataDB(kvDir, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	valueDir := path.Join(dir, "value")
	vlog := new(valueLog)
	err = vlog.open(valueDir, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer vlog.close()

	headPointer, err := readPointer(db, headPointerKey)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("headPointer: ", zap.Reflect("vp", headPointer))

	var lastPointer valuePointer
	var lastPointerData []byte
	var count int
	err = vlog.scan(headPointer, func(vp valuePointer, record *Record) error {
		count++

		binlog := new(pb.Binlog)
		err := binlog.Unmarshal(record.payload)
		if err != nil {
			return errors.Trace(err)
		}

		// skip the wrongly write binlog by pump client previous
		if binlog.StartTs == 0 && binlog.CommitTs == 0 {
			log.Info("skip empty binlog")
			return nil
		}

		// write to kv
		var ts int64
		if binlog.Tp == pb.BinlogType_Prewrite {
			ts = binlog.StartTs
		} else {
			ts = binlog.CommitTs
		}

		pointer, err := vp.MarshalBinary()
		if err != nil {
			panic(err)
		}
		lastPointer = vp
		lastPointerData = pointer

		err = db.Put(encodeTSKey(ts), pointer, nil)
		if err != nil {
			return errors.Trace(err)
		}

		if count%10000 == 0 {
			var batch leveldb.Batch
			batch.Put(headPointerKey, lastPointerData)
			batch.Put(handlePointerKey, lastPointerData)
			err := db.Write(&batch, nil)
			if err != nil {
				return errors.Trace(err)
			}
			log.Info("update headPointer", zap.Reflect("vp", lastPointer))
		}

		return nil
	})

	if err != nil {
		return errors.Annotate(err, "scan failed")
	}

	if count > 0 {
		var batch leveldb.Batch
		batch.Put(headPointerKey, lastPointerData)
		batch.Put(handlePointerKey, lastPointerData)
		err = db.Write(&batch, nil)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("update headPointer", zap.Reflect("vp", lastPointer))
	}

	return nil
}

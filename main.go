package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/jmhodges/levigo"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"code.google.com/p/rayleyva-gocask"
)

func main() {
	max := int64(10000000)
	valueSize := 512

	value := make([]byte, valueSize, valueSize)
	for i := 0; i < valueSize; i++ {
		value[i] = byte(rand.Intn(255))
	}

	if err := writeWithGoleveldb(max, 2048, value); err != nil {
		fmt.Printf("goleveldb failed: %v\n", err.Error())
	}
	if err := writeWiteLevigo(max, 2048, value); err != nil {
		fmt.Printf("levigo failed: %v\n", err.Error())
	}

	// if err := writeWithGocask(max); err != nil {
	// 	fmt.Printf("gocask failed: %v\n", err.Error())
	// }
}

func writeWithGocask(max int64, value []byte) error {
	directory, err := ioutil.TempDir("", "bitcask_")
	if err != nil {
		panic(err)
	}

	fmt.Printf("using directory: %v\n", directory)
	storage, err := gocask.NewGocask(directory)
	if err != nil {
		panic(err)
	}
	defer os.Remove(directory)

	startedAt := time.Now()
	for key := int64(0); key < max; key++ {
		err := storage.Put(string(
			[]byte{
				byte(key << 24),
				byte(key << 16),
				byte(key << 8),
				byte(key << 0),
			}), value)

		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(startedAt)
	fmt.Printf("gocask: wrote %v msgs in %v, %.0f msgs/s\n", max, duration, float64(max)/duration.Seconds())
	return nil
}

func writeWiteLevigo(max int64, batchsize int, value []byte) error {
	directory, err := ioutil.TempDir("", "levigo_")
	if err != nil {
		return err
	}
	fmt.Printf("using directory: %v\n", directory)
	defer os.Remove(directory)

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	defer opts.Close()
	
	db, err := levigo.Open(directory, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	sync := levigo.NewWriteOptions()
	sync.SetSync(true)

	startedAt := time.Now()

	batch := levigo.NewWriteBatch()
	for key := int64(0); key < max; key++ {
		batch.Put([]byte{
			byte(key << 24),
			byte(key << 16),
			byte(key << 8),
			byte(key << 0),
		}, value)

		if key%int64(batchsize) == 0 {
			if err := db.Write(sync, batch); err != nil {
				return err
			}
			batch.Clear()
		}
	}
	if err := db.Write(sync, batch); err != nil {
		return err
	}

	duration := time.Since(startedAt)
	fmt.Printf("levigo: wrote %v msgs in %v, %.0f msgs/s\n", max, duration, float64(max)/duration.Seconds())
	return nil
}

func writeWithGoleveldb(max int64, batchsize int, value []byte) error {
	directory, err := ioutil.TempDir("", "goleveldb_")
	if err != nil {
		return err
	}

	fmt.Printf("using directory: %v\n", directory)
	db, err := leveldb.OpenFile(directory, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	defer os.Remove(directory)
	sync := &opt.WriteOptions{Sync: true}

	startedAt := time.Now()

	batch := new(leveldb.Batch)
	for key := int64(0); key < max; key++ {
		batch.Put([]byte{
			byte(key << 24),
			byte(key << 16),
			byte(key << 8),
			byte(key << 0),
		}, value)

		if key%int64(batchsize) == 0 {
			if err := db.Write(batch, sync); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err := db.Write(batch, sync); err != nil {
		return err
	}

	duration := time.Since(startedAt)
	fmt.Printf("goleveldb: wrote %v msgs in %v, %.0f msgs/s\n", max, duration, float64(max)/duration.Seconds())
	return nil
}

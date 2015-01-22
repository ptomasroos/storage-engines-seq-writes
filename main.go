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
	jwzhbitcask "github.com/JWZH/bitcask_go"
)

func main() {
	max := 5000000
	valueSize := 200
	values := make(chan []byte, 1024)
	go func() {
		defer close(values)

		value := make([]byte, valueSize, valueSize)
		for index, _ := range value {
			value[index] = byte(rand.Intn(255))
		}

		for i := 0; i < max; i++ {
			values <- value
		}
	}()
	if err := writeWiteJwzhBitcask(2048, values); err != nil {
		fmt.Printf("jwzh bitcask failed: %v\n", err.Error())
	}

	values = make(chan []byte, 1024)
	go func() {
		defer close(values)

		value := make([]byte, valueSize, valueSize)
		for index, _ := range value {
			value[index] = byte(rand.Intn(255))
		}

		for i := 0; i < max; i++ {
			values <- value
		}
	}()
	if err := writeSingleFileAppend(2048, values); err != nil {
		fmt.Printf("goleveldb failed: %v\n", err.Error())
	}
	values = make(chan []byte, 1024)
	go func() {
		defer close(values)

		value := make([]byte, valueSize, valueSize)
		for index, _ := range value {
			value[index] = byte(rand.Intn(255))
		}

		for i := 0; i < max; i++ {
			values <- value
		}
	}()
	if err := writeWithGoleveldb(2048, values); err != nil {
		fmt.Printf("goleveldb failed: %v\n", err.Error())
	}
	values = make(chan []byte, 1024)
	go func() {
		defer close(values)

		value := make([]byte, valueSize, valueSize)
		for index, _ := range value {
			value[index] = byte(rand.Intn(255))
		}

		for i := 0; i < max; i++ {
			values <- value
		}
	}()
	if err := writeWiteLevigo(2048, values); err != nil {
		fmt.Printf("levigo failed: %v\n", err.Error())
	}

	if err := writeWithGocask(max); err != nil {
		fmt.Printf("gocask failed: %v\n", err.Error())
	}
}

func writeSingleFileAppend(batchsize int, values chan []byte) error {
	file, err := ioutil.TempFile("", "fileappend_")
	if err != nil {
		panic(err)
	}

	fmt.Printf("using file: %v\n", file.Name())
	defer file.Close()
	defer os.RemoveAll(file.Name())

	startedAt := time.Now()

	var sequence int64
	for value := range values {
		_, err := file.Write(value)
		if err != nil {
			return err
		}

		if sequence%int64(batchsize) == 0 {
			if err := file.Sync(); err != nil {
				return err
			}
		}
		sequence++
	}
	if err := file.Sync(); err != nil {
		return err
	}

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("fileappend: wrote %v msgs in %v, %.0f msgs/s\n", messageCount, duration, float64(messageCount)/duration.Seconds())
	return nil
}

func writeWithPjvdsBitcask(batchsize int, values chan []byte) error {
	directory, err := ioutil.TempDir("", "pjvds_bitcask_")
	if err != nil {
		panic(err)
	}

	fmt.Printf("using directory: %v\n", directory)
	storage, err := bc.Open(directory)
	if err != nil {
		panic(err)
	}
	defer storage.Close()
	defer os.RemoveAll(directory)

	startedAt := time.Now()

	var sequence int64
	for value := range values {
		err := storage.Put([]byte{
			byte(sequence << 24),
			byte(sequence << 16),
			byte(sequence << 8),
			byte(sequence << 0),
		}, value)

		if err != nil {
			panic(err)
		}
		if sequence%int64(batchsize) == 0 {
			if err := storage.Sync(); err != nil {
				panic(err)
			}
		}
		sequence++
	}

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("pjvdsbitcask: wrote %v msgs in %v, %.0f msgs/s\nwrite speed: %v mb/s\n", messageCount, duration, float64(messageCount)/duration.Seconds(), float64((messageCount*200)/1000/1000)/duration.Seconds())
	return nil
}

func writeWithGocask(batchsize int, values chan []byte) error {
	directory, err := ioutil.TempDir("", "bitcask_")
	if err != nil {
		panic(err)
	}

	fmt.Printf("using directory: %v\n", directory)
	storage, err := gocask.NewGocask(directory)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)

	startedAt := time.Now()

	var sequence int64
	for value := range values {
		err := storage.Put(string(
			[]byte{
				byte(sequence << 24),
				byte(sequence << 16),
				byte(sequence << 8),
				byte(sequence << 0),
			}), value)

		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("gocask: wrote %v msgs in %v, %.0f msgs/s\n", messageCount, duration, float64(messageCount)/duration.Seconds())
	return nil
}

func writeWiteJwzhBitcask(batchsize int, values chan []byte) error {
	directory, err := ioutil.TempDir("", "jwzhbitcask")
	if err != nil {
		return err
	}
	fmt.Printf("using directory: %v\n", directory)
	defer os.RemoveAll(directory)

	db, _ := jwzhbitcask.NewBitcask(jwzhbitcask.Options{1024 * 1024 * 1024, [2]int{0, 23}, 1.0, directory})
	defer db.Close()

	startedAt := time.Now()

	var sequence int64
	for value := range values {
		db.Set(string([]byte{
			byte(sequence << 24),
			byte(sequence << 16),
			byte(sequence << 8),
			byte(sequence << 0),
		}), value)

		if sequence%int64(batchsize) == 0 {
			db.Sync()
		}
		sequence++
	}
	db.Sync()

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("jwzhbitcask: wrote %v msgs in %v, %.0f msgs/s\n", messageCount, duration, float64(messageCount)/duration.Seconds())
	return nil
}

func writeWiteLevigo(batchsize int, values chan []byte) error {
	directory, err := ioutil.TempDir("", "levigo_")
	if err != nil {
		return err
	}
	fmt.Printf("using directory: %v\n", directory)
	defer os.RemoveAll(directory)

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
	var sequence int64
	for value := range values {
		batch.Put([]byte{
			byte(sequence << 24),
			byte(sequence << 16),
			byte(sequence << 8),
			byte(sequence << 0),
		}, value)

		if sequence%int64(batchsize) == 0 {
			if err := db.Write(sync, batch); err != nil {
				return err
			}
			batch.Clear()
		}
		sequence++
	}
	if err := db.Write(sync, batch); err != nil {
		return err
	}

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("levigo: wrote %v msgs in %v, %.0f msgs/s\n", messageCount, duration, float64(messageCount)/duration.Seconds())
	return nil
}

func writeWithGoleveldb(batchsize int, values chan []byte) error {
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
	defer os.RemoveAll(directory)
	sync := &opt.WriteOptions{Sync: true}

	startedAt := time.Now()

	batch := new(leveldb.Batch)
	var sequence int64
	for value := range values {
		batch.Put([]byte{
			byte(sequence << 24),
			byte(sequence << 16),
			byte(sequence << 8),
			byte(sequence << 0),
		}, value)

		if sequence%int64(batchsize) == 0 {
			if err := db.Write(batch, sync); err != nil {
				return err
			}
			batch.Reset()
		}
		sequence++
	}
	if err := db.Write(batch, sync); err != nil {
		return err
	}

	duration := time.Since(startedAt)
	messageCount := sequence + 1
	fmt.Printf("goleveldb: wrote %v msgs in %v, %.0f msgs/s\n", messageCount, duration, float64(messageCount)/duration.Seconds())
	return nil
}

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"code.google.com/p/rayleyva-gocask"
)

func main() {
	max := int64(10000000)
	if err := writeWithGoleveldb(max, 2048); err != nil {
		fmt.Printf("goleveldb failed: %v\n", err.Error())
	}
	// if err := writeWithGocask(max); err != nil {
	// 	fmt.Printf("gocask failed: %v\n", err.Error())
	// }
}

func writeWithGocask(max int64) error {
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
	value := []byte("55016240139541825831248235590928388050342796869467125714573772729983527548255642386370229015656387150848807487151339461263449466723879128964904436408494889996394069979929819274982353154433671964674699212887561015326410385441664236925959164219332147778332925795309157503386333233233354364896239995422994795249191515403481140936553663279757290416787757496552237116627244982955104539916919213038155680710328503796426173973700453821840362580703647190956630678410710125906454173642027827141851167868952548")
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

func writeWithGoleveldb(max int64, batchsize int) error {
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
	value := []byte("5501624013954182583124823559092838805034279686946712571457377272998352754825564238637022901565638715084880748715133946126344946672387912896490443640849488999639406997992981927498235315443367196467469921288756101532641038544166423692595916421933214777833292")

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

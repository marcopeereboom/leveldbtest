package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MemTest struct {
	postfix string
	maxSize int64
	mymap   map[string][]byte
}

func NewMemTest(postfix string) (*MemTest, error) {
	mt := MemTest{}
	mt.postfix = postfix
	mt.maxSize = 256 * 1024 * 1024
	return &mt, nil
}

func (mt *MemTest) start() {
	var i, keySize, valueSize int64

	mt.mymap = make(map[string][]byte)
	keySize = sha256.Size * 2 // ascii
	valueSize = 16
	elements := mt.maxSize / (keySize + valueSize) // this really is off by *4 due to overhead
	for i = 0; i < elements; i++ {
		digest := sha256.New()
		sha := make([]byte, 16)
		binary.PutVarint(sha, int64(i))
		digest.Write(sha)
		d := digest.Sum(nil)
		ds := fmt.Sprintf("%x", d)

		buf := make([]byte, valueSize)

		mt.mymap[ds] = buf
	}
	fmt.Fprintf(os.Stderr, "%v: memtest inserted %v elements\n", mt.postfix, elements)
}

func (mt *MemTest) Start() {
	go mt.start()
}

func (mt *MemTest) Delete() {
	mt.mymap = nil
}

type LevelDb struct {
	dbLock     sync.Mutex
	open       bool
	running    bool
	db         *leveldb.DB
	os         opt.OptionsSetter
	at         int64
	iterations int64
	postfix    string

	paused bool
	imsg   chan string
	idone  sync.WaitGroup
	ipause sync.WaitGroup

	lookuppaused bool
	lmsg         chan string
	ldone        sync.WaitGroup
	lpause       sync.WaitGroup
}

func NewTest(postfix string, compress, cacheEnable bool, iterations int64, maxfds int) (*LevelDb, error) {
	db := LevelDb{}
	db.postfix = postfix
	err := db.Open()
	if err != nil {
		return nil, err
	}

	db.postfix = postfix
	db.iterations = iterations
	db.imsg = make(chan string)
	db.lmsg = make(chan string)
	db.os = db.db.GetOptionsSetter()
	if compress == false {
		db.os.SetCompressionType(opt.NoCompression)
	}
	if cacheEnable == false {
		c := cache.EmptyCache{}
		db.os.SetBlockCache(c)
	}
	db.os.SetMaxOpenFiles(maxfds)

	return &db, nil
}

func (db *LevelDb) Open() error {
	var err error

	if db.open == true {
		return fmt.Errorf("db already open")
	}

	db.db, err = leveldb.OpenFile("./ldbtest/test.db"+db.postfix, &opt.Options{Flag: opt.OFCreateIfMissing})
	if err != nil {
		return err
	}
	db.open = true

	return nil
}

func (db *LevelDb) lstart() {
	var lookups int
	defer db.ldone.Done()

	for {
		db.dbLock.Lock()
		at := db.at
		db.dbLock.Unlock()
		if at < 1000 {
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		break
	}
	fmt.Fprintf(os.Stderr, "%v: commencing lookups\n", db.postfix)

	start := time.Now()
	for {
		select {
		case value := <-db.lmsg:
			switch value {
			case "quit":
				fmt.Fprintf(os.Stderr, "%v: lookup quitting\n", db.postfix)
				return
			}
		default:
		}
		db.lpause.Wait()

		db.dbLock.Lock()
		at := db.at
		db.dbLock.Unlock()

		// "random"
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		i := r.Int63n(at)
		// allocate memory all the time
		digest := sha256.New()
		sha := make([]byte, 16)
		binary.PutVarint(sha, int64(i))
		digest.Write(sha)
		d := digest.Sum(nil)

		// find it
		db.dbLock.Lock()
		_, err := db.db.Get(d, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v: %v\n", db.postfix, err)
			db.dbLock.Unlock()
			return
		}
		db.dbLock.Unlock()

		lookups++
		if lookups%10000 == 0 {
			delta := time.Now().Sub(start)
			fmt.Fprintf(os.Stderr, "%v: lookups %v (%v)\n", db.postfix, lookups, delta)
			start = time.Now()
		}
	}
}

func (db *LevelDb) start() {
	var i int64
	db.idone.Add(1)
	defer db.idone.Done()

	db.running = true
	start := time.Now()
	for {
		select {
		case value := <-db.imsg:
			switch value {
			case "quit":
				fmt.Fprintf(os.Stderr, "%v: insert quitting\n", db.postfix)
				return
			}
		default:
		}
		db.ipause.Wait()

		// allocate memory all the time
		digest := sha256.New()
		sha := make([]byte, 16)
		buf := make([]byte, 32)
		binary.PutVarint(sha, i)
		digest.Write(sha)
		d := digest.Sum(nil)

		db.dbLock.Lock()
		err := db.db.Put(d, buf, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			db.dbLock.Unlock()
			return
		}
		db.at++
		i = db.at
		db.dbLock.Unlock()

		if i%10000 == 0 {
			delta := time.Now().Sub(start)
			fmt.Fprintf(os.Stderr, "%v: %v (%v)\n", db.postfix, i, delta)
			start = time.Now()
		}

		if i >= db.iterations {
			break
		}
	}
}

func (db *LevelDb) bstart() {
	var i, x int64
	defer db.idone.Done()

	db.running = true
	start := time.Now()
	batchSize := int64(750)
	actualIterations := db.iterations / batchSize
	actualPrints := int64(10000) / batchSize
	for {
		select {
		case value := <-db.imsg:
			switch value {
			case "quit":
				fmt.Fprintf(os.Stderr, "%v: insert quitting\n", db.postfix)
				return
			}
		default:
		}
		db.ipause.Wait()

		batch := new(leveldb.Batch)
		for x = 0; x < batchSize; x++ {
			// allocate memory all the time
			digest := sha256.New()
			sha := make([]byte, 16)
			buf := make([]byte, 32)
			binary.PutVarint(sha, (i*batchSize)+x)
			digest.Write(sha)
			d := digest.Sum(nil)

			batch.Put(d, buf)
		}

		db.dbLock.Lock()
		err := db.db.Write(batch, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			db.dbLock.Unlock()
			return
		}
		db.at++
		i = db.at
		db.dbLock.Unlock()

		if i%actualPrints == 0 {
			delta := time.Now().Sub(start)
			fmt.Fprintf(os.Stderr, "%v: %v (%v)\n", db.postfix, i, delta)
			start = time.Now()
		}

		if i >= actualIterations {
			break
		}
	}
}

func (db *LevelDb) GetAt() int64 {
	db.dbLock.Lock()
	at := db.at
	db.dbLock.Unlock()

	return at
}

func (db *LevelDb) Start() {
	db.idone.Add(1)
	go db.bstart()
	db.ldone.Add(1)
	go db.lstart()
}

func (db *LevelDb) LookupPause() {
	if db.lookuppaused == false {
		db.lookuppaused = true
		db.lpause.Add(1)
		fmt.Fprintf(os.Stderr, "%v: lookup paused at %v\n", db.postfix, db.GetAt())
	}
}

func (db *LevelDb) InsertPause() {
	if db.paused == false {
		db.paused = true
		db.ipause.Add(1)
		fmt.Fprintf(os.Stderr, "%v: insert paused at %v\n", db.postfix, db.GetAt())
	}
}

func (db *LevelDb) LookupUnpause() {
	if db.lookuppaused == true {
		db.lookuppaused = false
		db.lpause.Done()
		fmt.Fprintf(os.Stderr, "%v: lookup unpaused at %v\n", db.postfix, db.GetAt())
	}
}

func (db *LevelDb) InsertUnpause() {
	if db.paused == true {
		db.paused = false
		db.ipause.Done()
		fmt.Fprintf(os.Stderr, "%v: insert unpaused %v\n", db.postfix, db.GetAt())
	}
}

func (db *LevelDb) Stop() {
	db.LookupUnpause()
	db.lmsg <- "quit"

	db.InsertUnpause()
	db.imsg <- "quit"

	db.ldone.Wait()
	db.idone.Wait()

	db.running = false
}

func (db *LevelDb) Close() {
	if db.open {
		db.db.Close()
		db.open = false
	}
}

func (db *LevelDb) Delete() {
	if db.running {
		db.Stop()
	}
	db.Close()
}

func main() {
	var (
		err error
		s   string
	)

	id := 0
	runtime.GOMAXPROCS(8)

	tests := make(map[string]*LevelDb, 16)
	memtests := make(map[string]*MemTest, 16)
	r := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		s, err = r.ReadString('\n')
		if err == io.EOF {
			s = "quit"
		} else if err != nil {
			fmt.Printf("\nReadString error: %v\n", err)
			continue
		}

		// parse line
		s = strings.TrimSuffix(s, "\n")
		s = strings.TrimSuffix(s, "\r")
		cmd := strings.Split(s, " ")
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "open":
			if len(cmd) < 2 {
				fmt.Printf("usage: open <id>\n")
				continue
			}
			db, err := NewTest(cmd[1], false, true, 200000000, 256)
			if err != nil {
				fmt.Printf("could not create a new open test %v: %#v\n", cmd[1], err)
				if errno, ok := err.(syscall.Errno); ok && uint32(errno) == 0x20 {
					fmt.Printf("GOT IT\n")
				}
				continue
			}
			tests[cmd[1]] = db
			fmt.Printf("created open test %v\n", db.postfix)
			db.Start()
		case "new":
			db, err := NewTest(strconv.Itoa(id), false, true, 200000000, 256)
			if err != nil {
				fmt.Printf("could not create a new test")
				continue
			}
			tests[strconv.Itoa(id)] = db
			fmt.Printf("created test %v\n", db.postfix)
			db.Start()
			id++
		case "list":
			fmt.Printf("ID\n")
			for k, _ := range tests {
				fmt.Printf("%v\n", k)
			}
		case "mem":
			if len(cmd) < 2 {
				fmt.Printf("usage: mem <filename>\n")
				continue
			}
			f, err := os.Create(fmt.Sprintf("/mnt/profile.%s", cmd[1]))
			if err == nil {
				pprof.WriteHeapProfile(f)
				f.Close()
			} else {
				fmt.Printf("profile failed %v", err)
			}
		case "memtest":
			mt, err := NewMemTest(strconv.Itoa(id))
			if err != nil {
				fmt.Printf("could not create a new mem test")
				continue
			}
			memtests[strconv.Itoa(id)] = mt
			fmt.Printf("created mem test %v\n", mt.postfix)
			mt.Start()
			id++
		case "mempause":
		case "memunpause":
		case "memdelete":
			if len(cmd) < 2 {
				fmt.Printf("usage: memdelete <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if mt, ok := memtests[v]; ok {
					mt.Delete()
					fmt.Printf("deleting memtest %v\n", v)
					delete(memtests, v)
					fmt.Printf("deleted memtest %v\n", v)
				} else {
					fmt.Printf("can't delete memtest id %v\n", v)
				}
			}
		case "free":
			fmt.Printf("returning memory to OS\n")
			debug.FreeOSMemory()
		case "reopen", "re":
			if len(cmd) < 2 {
				fmt.Printf("usage: reopen <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.Delete()
					fmt.Printf("deleting %v\n", v)
					fmt.Printf("reopening %v\n", v)
					err := db.Open()
					if err != nil {
						fmt.Printf("can't reopen id %v: %v\n", v, err)
					}
					db.Start()
				} else {
					fmt.Printf("can't reopen id %v\n", v)
				}
			}
		case "delete":
			if len(cmd) < 2 {
				fmt.Printf("usage: delete <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.Delete()
					fmt.Printf("deleting %v\n", v)
					delete(tests, v)
					fmt.Printf("deleted %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't delete id %v\n", v)
				}
			}
		case "lu", "lunpause", "lnsertunpause":
			if len(cmd) < 2 {
				fmt.Printf("usage: lunpause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.LookupUnpause()
					fmt.Printf("lookup unpaused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't lunpause id %v\n", v)
				}
			}
		case "lp", "lpause", "lnsertpause":
			if len(cmd) < 2 {
				fmt.Printf("usage: lpause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.LookupPause()
					fmt.Printf("lookup paused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't lpause id %v\n", v)
				}
			}
		case "iu", "iunpause", "insertunpause":
			if len(cmd) < 2 {
				fmt.Printf("usage: iunpause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.InsertUnpause()
					fmt.Printf("inset unpaused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't iunpause id %v\n", v)
				}
			}
		case "ip", "ipause", "insertpause":
			if len(cmd) < 2 {
				fmt.Printf("usage: ipause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.InsertPause()
					fmt.Printf("insert paused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't ipause id %v\n", v)
				}
			}
		case "p", "pause":
			if len(cmd) < 2 {
				fmt.Printf("usage: pause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.LookupPause()
					db.InsertPause()
					fmt.Printf("paused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't pause id %v\n", v)
				}
			}
		case "up", "unpause":
			if len(cmd) < 2 {
				fmt.Printf("usage: unpause <n>...\n")
				continue
			}
			for _, v := range cmd[1:] {
				if db, ok := tests[v]; ok {
					db.InsertUnpause()
					db.LookupUnpause()
					fmt.Printf("unpaused %v: at %v\n", v, db.GetAt())
				} else {
					fmt.Printf("can't unpause id %v\n", v)
				}
			}
		case "quit":
			for _, db := range tests {
				db.Delete()
				fmt.Printf("stopped %v: at %v\n", db.postfix, db.GetAt())
			}
			return

		default:
			fmt.Printf("invalid command: %v\n", cmd[0])
		}
	}
}

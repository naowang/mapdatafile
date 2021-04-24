package mapdatafile

import (
	"encoding/binary"
	"time"
	"toolfunc"

	"github.com/tidwall/buntdb"

	//"encoding/binary"
	"fmt"
	"testing"
	//"time"
)

func Test1(t *testing.T) {
	DbClear("testBKDRHashdb")
	db := NewMapDataFile("testBKDRHashdb")

	db.Put(BKDRHash([]byte("111,xv")), []byte("222"))
	db.Put(BKDRHash([]byte("dlskajflkdsancxvnknskrfhldsflkdsankcvnck,xv")), []byte("111"))
	db.Put(BKDRHash([]byte("bbb,xv")), []byte("333"))
	db.Put(BKDRHash([]byte("ddd,xv")), []byte("444"))
	db.Put(BKDRHash([]byte("dddd,xv")), []byte("555"))
	db.Put(BKDRHash([]byte("ccc,xv")), []byte("666"))
	db.Put(BKDRHash([]byte("bhhh,xv")), []byte("777"))
	db.Put(BKDRHash([]byte("aafg,xv")), []byte("888"))
	db.Put(BKDRHash([]byte("jjjh,xv")), []byte("999"))
	db.Put(BKDRHash([]byte("hgvvv,xv")), []byte("10101010"))
	db.Put(BKDRHash([]byte("55gv,xv")), []byte("12121212"))
	db.Put(BKDRHash([]byte("ggy66,xv")), []byte("13131313"))
	db.Put(BKDRHash([]byte("ssdggggfdh,xv")), []byte("141414"))
	db.PrintAll()
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))
	rndkey, rndval := db.RandGet()
	fmt.Println(rndkey, string(rndval))
	db.Delete(BKDRHash([]byte("bbb,xv")))
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))
	fmt.Println(db.Exists(BKDRHash([]byte("bbb,xv"))))
	fmt.Println(db.Exists(BKDRHash([]byte("55gv,xv"))))
	db.Close()
	db = NewMapDataFile("testBKDRHashdb")
	fmt.Println(db.Exists(BKDRHash([]byte("bbb,xv"))))
	fmt.Println(db.Exists(BKDRHash([]byte("55gv,xv"))))
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))
	fmt.Println("reopen all:")
	db.PrintAll()
	keybt := make([]byte, 8)
	ts := time.Now().UnixNano()
	dla, _ := buntdb.Open(":memory:")
	for i := 0; i < 20000000; i++ {
		//binary.BigEndian.PutUint64(keybt, uint64(i))
		hkey := SDBMHash(toolfunc.RandPrintChar(24, 50))
		binary.BigEndian.PutUint64(keybt, hkey)
		dla.Update(func(tx *buntdb.Tx) error {
			tx.Set(string(keybt), string(toolfunc.RandAlpha(5, 5)), nil)
			return nil
		})
		if i%100000 == 0 {
			ts2 := time.Now().UnixNano()
			dla.View(func(tx *buntdb.Tx) error {
				cnt := 0
				tx.Ascend("", func(key, value string) bool {
					fmt.Printf("key: %s, value: %s\n", key, value)
					cnt += 1
					if cnt >= 3 {
						return false
					}
					return true
				})
				return nil
			})
			fmt.Println(ts2 - ts)
			ts = ts2
			fmt.Println(i)
		}
	}
	ts = time.Now().UnixNano()
	delcnt := 0
	// dla.View(func(tx *buntdb.Tx) error {
	// 	cnt := 0
	// 	err := tx.Ascend("", func(key, val string) bool {
	// 		if val[len(val)-1]%2 == 0 {
	// 			delete(dla, key)
	// 			delcnt += 1
	// 		}
	// 	})
	// 	return nil
	// })
	ts2 := time.Now().UnixNano()
	fmt.Println(ts2-ts, "delete use time", delcnt)
	db.Close()
	db = NewMapDataFile("testBKDRHashdb")
	fmt.Println("all:")
	db.PrintAll()
	db.Close()
	// db2 := NewBKDRHashDb(`D:\WordNetHost\allpageurlmapdb`)
	// db2.Export("D:\\keyvalue3.txt")
	// db2.Close()
}

// mapdatafile project mapdatafile.go
package mapdatafile

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

type MapDataFile struct {
	mp       map[uint64][]byte
	path     string
	pathfile *os.File
	mu       sync.RWMutex
	filemu   sync.RWMutex
	dbclosed bool
}

func NewMapDataFile(path string) *MapDataFile {
	db := &MapDataFile{path: path}
	var err error
	db.mp = FileToMapU64Bytes(path)
	db.pathfile, err = os.OpenFile(db.path+".data", os.O_CREATE|os.O_RDWR, 0666)
	go dbfilesync(db)
	if err == nil {
		return db
	} else {
		return nil
	}
}
func dbfilesync(db *MapDataFile) {
	for true {
		time.Sleep(300 * time.Second)
		if db.dbclosed {
			break
		}
		db.mu.Lock()
		db.pathfile.Sync()
		db.mu.Unlock()
	}
}

func (db *MapDataFile) Put(key uint64, val []byte) bool {
	valbt := make([]byte, 5)
	tempbt := make([]byte, 8)
	db.mu.Lock()
	if len(val) > 0 {
		keybt := make([]byte, 8)
		curpos, err := db.pathfile.Seek(0, os.SEEK_END)
		if err != nil {
			db.mu.Unlock()
			return false
		}
		binary.BigEndian.PutUint32(keybt[:4], uint32(len(val)))
		db.pathfile.Write(keybt[:4])
		db.pathfile.Write(val)
		binary.BigEndian.PutUint64(tempbt, uint64(curpos))
		copy(valbt, tempbt[3:])
	} else {
		binary.BigEndian.PutUint64(tempbt, ^uint64(0))
		copy(valbt, tempbt[3:])
	}
	db.mp[key] = valbt
	db.mu.Unlock()
	return true
}

func (db *MapDataFile) Exists(key uint64) bool {
	db.mu.RLock()
	_, be := db.mp[key]
	db.mu.RUnlock()
	return be
}

func (db *MapDataFile) Get(key uint64) []byte {
	db.mu.RLock()
	posinfo, be := db.mp[key]
	if be == false {
		db.mu.RUnlock()
		return nil
	}
	keybt := make([]byte, 8)
	copy(keybt[3:], posinfo)
	if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
		db.mu.RUnlock()
		return []byte{}
	} else {
		db.filemu.Lock()
		_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
		if setpose == nil {
			lenbt := make([]byte, 4)
			db.pathfile.Read(lenbt)
			valuebt := make([]byte, binary.BigEndian.Uint32(lenbt))
			db.pathfile.Read(valuebt)
			db.filemu.Unlock()
			db.mu.RUnlock()
			return valuebt
		}
		db.filemu.Unlock()
	}
	db.mu.RUnlock()
	return nil
}

func (db *MapDataFile) RandGet() (key uint64, value []byte) {
	db.mu.Lock()
	for key, posinfo := range db.mp {
		keybt := make([]byte, 8)
		copy(keybt[3:], posinfo)
		if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
			db.mu.Unlock()
			return key, []byte{}
		} else {
			_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
			if setpose == nil {
				db.pathfile.Read(keybt[:4])
				valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
				db.pathfile.Read(valuebt)
				//delete(db.mp, key)
				db.mu.Unlock()
				return key, valuebt
			}
		}
		break
	}
	db.mu.Unlock()
	return 0, nil
}

func (db *MapDataFile) Delete(key uint64) bool {
	db.mu.Lock()
	delete(db.mp, key)
	db.mu.Unlock()
	return true
}

func (db *MapDataFile) Count() uint64 {
	db.mu.Lock()
	cnt := uint64(len(db.mp))
	db.mu.Unlock()
	return cnt
}

func (db *MapDataFile) Flush() bool {
	MapU64BytesToFile(db.mp, db.path)
	db.pathfile.Sync()
	return true
}

func (db *MapDataFile) Close() bool {
	MapU64BytesToFile(db.mp, db.path)
	db.pathfile.Close()
	for key, _ := range db.mp {
		delete(db.mp, key)
	}
	db.dbclosed = true

	return true
}

func (db *MapDataFile) PrintAll() {
	keybt := make([]byte, 8)
	for key, posinfo := range db.mp {
		copy(keybt[:3], []byte{0, 0, 0})
		copy(keybt[3:], posinfo)
		if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
			fmt.Println(key, "")
		} else {
			_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
			if setpose == nil {
				db.pathfile.Read(keybt[:4])
				valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
				db.pathfile.Read(valuebt)
				fmt.Println(key, string(valuebt))
			} else {
				panic("Seek to position error")
			}
		}
	}
}

func (db *MapDataFile) Export(txtpath string) {
	ff, _ := os.OpenFile(txtpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	keybt := make([]byte, 8)
	for key, posinfo := range db.mp {
		copy(keybt[:3], []byte{0, 0, 0})
		copy(keybt[3:], posinfo)
		if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
			ff.Write([]byte(strconv.FormatUint(key, 10)))
			ff.Write([]byte{'\t'})
			ff.Write([]byte{})
			ff.Write([]byte{'\n'})
		} else {
			_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
			if setpose == nil {
				db.pathfile.Read(keybt[:4])
				valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
				db.pathfile.Read(valuebt)
				ff.Write([]byte(strconv.FormatUint(key, 10)))
				ff.Write([]byte{'\t'})
				ff.Write(valuebt)
				ff.Write([]byte{'\n'})
			} else {
				panic("Seek to position error")
			}
		}
	}
	ff.Close()
}

func DbRewrite(path string) bool {
	mp := FileToMapU64Bytes(path)
	pathfile, err := os.OpenFile(path+".data", os.O_RDONLY, 0666)
	if err == nil {
		pathfilenew, err2 := os.OpenFile(path+".data.rewrite", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err2 == nil {
			keybt := make([]byte, 8)
			newpos := 0
			mp2 := treemap.NewWith(func(a, b interface{}) int {
				if a.(uint64) < b.(uint64) {
					return -1
				} else if a.(uint64) > b.(uint64) {
					return 1
				} else {
					return 0
				}
			})
			for key, val := range mp {
				copy(keybt[:3], []byte{0, 0, 0})
				copy(keybt[3:], val)
				mp2.Put(binary.BigEndian.Uint64(keybt), key)
			}
			mp2.Each(func(key, vall interface{}) {
				_, setpose := pathfile.Seek(int64(key.(uint64)), os.SEEK_SET)
				if setpose == nil {
					pathfile.Read(keybt[:4])
					valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
					pathfile.Read(valuebt)
					pathfilenew.Write(keybt[:4])
					pathfilenew.Write(valuebt)
					binary.BigEndian.PutUint64(keybt, uint64(newpos))
					mpval, bmpval := mp[vall.(uint64)]
					if bmpval {
						copy(mpval, keybt[3:])
						mp[vall.(uint64)] = mpval
					}
					newpos += 4 + len(valuebt)
				} else {
					panic("Seek to position error")
				}
			})
			os.Rename(path, path+".old")
			MapU64BytesToFile(mp, path)
			pathfilenew.Close()
		}
		pathfile.Close()
		os.Remove(path + ".data")
		os.Remove(path + ".old")
		os.Rename(path+".data.rewrite", path+".data")
	}
	return true
}

func ToQuickDataFile(path string, bdatarewrite bool) bool {
	mp := FileToMapU64Bytes(path)
	fmt.Println("total count", len(mp))
	pathfile, err := os.OpenFile(path+".data", os.O_RDONLY, 0666)
	if err == nil {
		pathfilenew, err2 := os.OpenFile(path+".data.rewrite", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err2 == nil {
			keybt := make([]byte, 8)
			newpos := 0
			mp2 := treemap.NewWith(func(a, b interface{}) int {
				if a.(uint64) < b.(uint64) {
					return -1
				} else if a.(uint64) > b.(uint64) {
					return 1
				} else {
					return 0
				}
			})
			if bdatarewrite {
				for key, val := range mp {
					copy(keybt[:3], []byte{0, 0, 0})
					copy(keybt[3:], val)
					mp2.Put(binary.BigEndian.Uint64(keybt), key)
				}
				mp2.Each(func(key, vall interface{}) {
					_, setpose := pathfile.Seek(int64(key.(uint64)), os.SEEK_SET)
					if setpose == nil {
						pathfile.Read(keybt[:4])
						valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
						pathfile.Read(valuebt)
						pathfilenew.Write(keybt[:4])
						pathfilenew.Write(valuebt)
						binary.BigEndian.PutUint64(keybt, uint64(newpos))
						mpval, bmpval := mp[vall.(uint64)]
						if bmpval {
							copy(mpval, keybt[3:])
							mp[vall.(uint64)] = mpval
						}
						newpos += 4 + len(valuebt)
					} else {
						panic("Seek to position error")
					}
				})
			}
			pathfilenew.Close()

			os.Rename(path, path+".oldmap")
			mp2.Clear()
			for key, _ := range mp {
				mp2.Put(key, 0)
			}
			pathf, pathfe := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if pathfe == nil {
				cachebt := make([]byte, 0, 8*1024*1024)
				mp2.Each(func(key, val interface{}) {
					binary.BigEndian.PutUint64(keybt, key.(uint64))
					mpval, bmpval := mp[key.(uint64)]
					if bmpval {
						if len(cachebt)+len(keybt)+len(mpval) > cap(cachebt) {
							pathf.Write(cachebt)
							cachebt = cachebt[:0]
						}
						cachebt = append(cachebt, keybt...)
						cachebt = append(cachebt, mpval...)
					} else {
						panic("head map error")
					}
				})
				if len(cachebt) > 0 {
					pathf.Write(cachebt)
				}
				pathf.Close()
			} else {
				panic("open " + path + " error")
			}

		}
		pathfile.Close()
		os.Remove(path + ".oldmap")
		if bdatarewrite {
			os.Remove(path + ".data")
			os.Rename(path+".data.rewrite", path+".data")
		} else {
			os.Remove(path + ".data.rewrite")
		}
	}
	return true
}

// have repeat hash code
// func Hash64(str []byte, mixls ...uint64) uint64 {
// 	// set 'mix' to some value other than zero if you want a tagged hash
// 	mulp := uint64(2654435789)
// 	var mix uint64
// 	if len(mixls) > 0 {
// 		mix = mixls[0]
// 	}
// 	mix ^= uint64(104395301)
// 	for i := 0; i < len(str); i++ {
// 		mix += (uint64(str[i]) * mulp) ^ (mix >> 23)
// 	}
// 	return mix ^ (mix << 37)
// }

//too slow
// func RSHash(str []byte) uint64 {
// 	b := uint64(378551)
// 	a := uint64(63689)
// 	hash := uint64(0)
// 	for i := 0; i < len(str); i++ {
// 		hash = hash*a + uint64(str[i])
// 		a = a * b
// 	}
// 	return hash
// }

// have repeat hash code
// func JSHash(str []byte) uint64 {
// 	hash := uint64(1315423911)
// 	for i := 0; i < len(str); i++ {
// 		hash ^= ((hash << 5) + uint64(str[i]) + (hash >> 2))
// 	}
// 	return hash
// }

// have repeat hash code
// func ELFHash(str []byte) uint64 {
// 	hash := uint64(0)
// 	for i := 0; i < len(str); i++ {
// 		hash = (hash << 4) + uint64(str[i])
// 		x := hash & 0xF000000000000000
// 		if x != 0 {
// 			hash ^= (x >> 24)
// 		}
// 		hash &= ^x
// 	}
// 	return hash
// }

func BKDRHash(str []byte) uint64 {
	seed := uint64(131313)
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return hash
}

func SDBMHash(str []byte) uint64 {
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = uint64(str[i]) + (hash << 6) + (hash << 16) - hash
	}
	return hash
}

// have repeat hash code
// func DJBHash(str []byte) uint64 {
// 	hash := uint64(5381)
// 	for i := 0; i < len(str); i++ {
// 		hash = ((hash << 5) + hash) + uint64(str[i])
// 	}
// 	return hash
// }

// have repeat hash code
// func DEKHash(str []byte) uint64 {
// 	hash := uint64(len(str))
// 	for i := 0; i < len(str); i++ {
// 		hash = ((hash << 5) ^ (hash >> 27)) ^ uint64(str[i])
// 	}
// 	return hash
// }

//too slow
// func APHash(str []byte) uint64 {
// 	hash := uint64(0xAAAAAAAAAAAAAAAA)
// 	for i := 0; i < len(str); i++ {
// 		if (i & 1) == 0 {
// 			hash ^= ((hash << 7) ^ uint64(str[i])*(hash>>3))
// 		} else {
// 			hash ^= (^((hash << 11) + (uint64(str[i]) ^ (hash >> 5))))
// 		}
// 	}
// 	return hash
// }

func DbClear(path string) bool {
	os.Remove(path)
	os.Remove(path + ".data")
	return true
}

func BytesToMapU64Bytes(mdatastr []byte) (mdata map[uint64][]byte) {
	mdata = make(map[uint64][]byte, 0)
	var key, vallen, startpos uint64
	for uint64(startpos)+8 <= uint64(len(mdatastr)) {
		key = binary.BigEndian.Uint64(mdatastr[startpos : startpos+8])
		vallen = uint64(binary.BigEndian.Uint32(mdatastr[startpos+8 : startpos+8+4]))
		mdata[key] = BytesClone(mdatastr[startpos+8+4 : startpos+8+4+vallen])
		startpos += 8 + 4 + vallen
		if startpos >= uint64(len(mdatastr)) {
			break
		}
	}
	return mdata
}

func MapU64BytesToBytes(mdata map[uint64][]byte) (outbt []byte) {
	keybt := make([]byte, 8)
	vallenbt := make([]byte, 4)
	var vallen uint32
	for key, value := range mdata {
		binary.BigEndian.PutUint64(keybt, key)
		vallen = uint32(len(value))
		binary.BigEndian.PutUint32(vallenbt, vallen)
		outbt = append(outbt, keybt...)
		outbt = append(outbt, vallenbt...)
		outbt = append(outbt, value...)
	}
	return outbt
}

func FileToMapU64Bytes(path string) (mdata map[uint64][]byte) {
	mdata = make(map[uint64][]byte, 0)
	ff, ffe := os.OpenFile(path, os.O_RDONLY, 0666)
	if ffe == nil {
		tempbt := make([]byte, 8*1024*1024)
		var key, vallen uint64
		var tempbti int
		readn, _ := ff.Read(tempbt)
		tempbt = tempbt[:readn]
		for true {
			if tempbti+12 > len(tempbt) {
				copy(tempbt, tempbt[tempbti:])
				readn, _ = ff.Read(tempbt[len(tempbt)-tempbti:])
				if readn == 0 {
					break
				}
				tempbt = tempbt[:len(tempbt)-tempbti+readn]
				tempbti = 0
				if len(tempbt) < 12 {
					break
				}
			}
			key = binary.BigEndian.Uint64(tempbt[tempbti : tempbti+8])
			tempbti += 8
			vallen = uint64(binary.BigEndian.Uint32(tempbt[tempbti : tempbti+4]))
			tempbti += 4
			if tempbti+int(vallen) > len(tempbt) {
				copy(tempbt, tempbt[tempbti:])
				readn, _ = ff.Read(tempbt[len(tempbt)-tempbti:])
				if readn == 0 {
					break
				}
				tempbt = tempbt[:len(tempbt)-tempbti+readn]
				tempbti = 0
				if len(tempbt) < int(vallen) {
					break
				}
			}
			valuebt := make([]byte, vallen)
			copy(valuebt, tempbt[tempbti:tempbti+int(vallen)])
			tempbti += int(vallen)
			mdata[key] = valuebt
		}
		ff.Close()
	}
	return mdata
}

func MapU64BytesToFile(mdata map[uint64][]byte, path string) {
	ff, ffe := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if ffe == nil {
		keybt := make([]byte, 8)
		vallenbt := make([]byte, 4)
		outbt := make([]byte, 0, 8*1024*1024)
		var vallen uint32
		for key, value := range mdata {
			binary.BigEndian.PutUint64(keybt, key)
			vallen = uint32(len(value))
			binary.BigEndian.PutUint32(vallenbt, vallen)
			outbt = append(outbt, keybt...)
			outbt = append(outbt, vallenbt...)
			outbt = append(outbt, value...)
			if len(outbt) >= 8*1024*1024 {
				ff.Write(outbt)
				outbt = outbt[:0]
			}
		}
		if len(outbt) > 0 {
			ff.Write(outbt)
			outbt = outbt[:0]
		}
		ff.Close()
	}
}

func BytesClone(src []byte) []byte {
	target := make([]byte, len(src))
	copy(target, src)
	return target
}

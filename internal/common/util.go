package common

import (
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"gdfs/config"
	"gdfs/internal/types"
	"hash/crc32"
	"log"
	"math/big"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func PartionPath(path types.Path) ([]string, string) {
	if path == "/" {
		return []string{""}, ""
	}
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	tokens := strings.Split(string(path), "/")
	return tokens[:len(tokens)-1], tokens[len(tokens)-1]
}

func GetFileNameWithExt(path types.Path) string {

	index := strings.LastIndex(string(path), "/")
	return string(path)[index+1:]
}

func GetFileNameWithoutExt(xpath types.Path) string {
	name := GetFileNameWithExt(xpath)

	return path.Base(name)
}

func IsExist(f string) bool {
	_, err := os.Stat(f)
	return err == nil || os.IsExist(err)
}

func Markmangle(name string) string {
	return time.Now().Format("2006-01-02_15-04-05") + "#" + path.Base(name) + ".del"
}
func Markdemangle(name string) string {
	name = path.Base(name)
	idx := strings.LastIndex(name, "#")
	return name[idx+1:]
}
func SplitEndPoint(endpoint string) (string, string) {
	idx := strings.LastIndex(endpoint, ":")
	return endpoint[:idx], endpoint[idx+1:]
}
func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func GetChunkHandleId() int64 {
	return time.Now().UnixNano()
}

func Uuid() string {
	rand.Seed(time.Now().UnixNano())

	// 定义字符集
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// 生成一个32位的随机字符串
	value := make([]byte, 32)
	for i := range value {
		value[i] = charset[rand.Intn(len(charset))]
	}

	return string(value)
}

func GetSha1(data []byte) string {
	c := sha1.New()
	c.Write(data)
	return hex.EncodeToString(c.Sum(nil))
}

func GetCrc32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)

}

func GetMd5(data []byte) string {
	c := md5.New()
	c.Write(data)
	return hex.EncodeToString(c.Sum(nil))
}

func GetCurrentWorkDir() string {
	path, _ := filepath.Abs(os.Args[0])
	index := strings.LastIndex(path, string(os.PathSeparator))
	path = path[:index]
	log.Println(path)

	return path
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func JoinErrors(errs ...error) error {
	str := make([]string, len(errs))

	for i, v := range errs {
		str[i] = v.Error()
	}

	return fmt.Errorf("%v", strings.Join(str, ";"))
}

func MusOpenLogFile(cc *config.Node) {
	var (
		f   *os.File
		err error
	)
	if IsExist(cc.Debug) {
		f, err = os.Open(cc.Debug)

	} else {
		f, err = os.Create(cc.Debug)
	}
	if err != nil {
		panic(err)
	}

	SetLogger(f)
}

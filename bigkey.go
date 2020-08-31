package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"regexp"
	"strconv"
	"strings"
)
var rdb *redis.Client
var ctx = context.Background()

func initClient() (err error) {
	rdb = redis.NewClient(&redis.Options{
		Addr: "100.73.20.3:6379",
		Password: "",
		DB: 0,
	})
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		return err
	}
	return nil
}

func roleCheck() bool {
	str := rdb.Info(ctx,"Replication").String()
	reg := regexp.MustCompile(`.*(role:[\w]{5,6}).*`)
	role := strings.Split(strings.ToLower(reg.FindStringSubmatch(str)[1]),":")[1]
	if role == "master" {
		return true
	}
	return false
}

func dbTotal() []int {
	var num = []int{}
	str := rdb.Info(ctx,"Keyspace").String()
	db := strings.Split(str,"\\r\\n")
	reg := regexp.MustCompile(`db([\d]{1,3}):keys.*`)

	for i := 1; i < len(db); i++ {
		dbNum := reg.FindStringSubmatch(db[i])
		//filter space record
		if len(dbNum) >= 2 {
			num = append(num,Str2Int(dbNum[1]))
		}
	}

	return num
}

func Str2Int(constr string) int {
	conint, err := strconv.Atoi(constr)
	if err != nil {
		return 0
	}
	return conint
}

func findBigkey(host ,password string,port ,db int,cnt int64,size int) {
	cur := uint64(0)
	_,err := rdb.Do(ctx,"select",db).Result()
	if err != nil {
		fmt.Printf("exec select %d failed,err:%v\n",db,err)
		return
	}
	keys,tCur,err := rdb.Scan(ctx,cur,"*",cnt).Result()
	if err != nil {
		fmt.Printf("scan exec failed ,err:%v\n",err)
		return
	}
	cur = tCur
	for {
		pipe := rdb.Pipeline()
		rdb.TxPipeline()
		for _,k := range keys {
			pipe.DebugObject(ctx,k).Result()
		}
		res, err := pipe.Exec(ctx)
		if err != nil {
			fmt.Printf("pipe exec failed,err:%v\n",err)
		}

		lenReg := regexp.MustCompile(`debug object.*(serializedlength:[\d]{1,}).*`)
		typeReg := regexp.MustCompile(`debug object.*( encoding:.* )serializedlength.*`)

		var str,keyType = "",""
		var length = 0
		for _,object := range res {
			key := strings.Replace(strings.Split(object.String(),":")[0],"debug object ","",-1)

			if len(lenReg.FindStringSubmatch(object.String())) >= 2 && len(typeReg.FindStringSubmatch(object.String())) >= 2{
				str = lenReg.FindStringSubmatch(object.String())[1]
				length = Str2Int(strings.Split(str,":")[1])
				keyType = typeReg.FindStringSubmatch(object.String())[1]
			}

			if length >= size {
				fmt.Printf("key:%v value:%v type:%v\n",key,length,keyType)
			}
		}
		if cur == 0 {
			break
		}

		keys,tCur,err = rdb.Scan(ctx,cur,"*",cnt).Result()
		if err != nil {
			fmt.Printf("scan exec failed ,err:%v\n",err)
			return
		}
		cur = tCur
	}

}
func main() {
	initClient()
	if(roleCheck()) {
		fmt.Printf("The redis server role is master, please choice slave redis server scan bigkey...")
		return
	}
	var db = dbTotal()
	for i := range db {
		fmt.Printf("\ndb%d bigkey information--------\n",i)
		findBigkey("xxx.xxx.xxx.xxx","",6379,i,5,1)
	}

}

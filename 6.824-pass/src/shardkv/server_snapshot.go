package shardkv

import (
	"bytes"

	"6.824/labgob"
	t "6.824/pylog/template"
	sc "6.824/shardctrler"
)

func (kv *ShardKV) PersistDataL() []byte {
	writebuf := new(bytes.Buffer)
	labenc := labgob.NewEncoder(writebuf)
	labenc.Encode(kv.config)
	labenc.Encode(kv.shardArr)
	labenc.Encode(kv.CloneSessionL())
	return writebuf.Bytes()
}

func (kv *ShardKV) ReadPersistL(data []byte) {
	if len(data) < 1 {
		return
	}
	readbuf := bytes.NewBuffer(data)
	labdec := labgob.NewDecoder(readbuf)

	var config sc.Config
	labdec.Decode(&config)

	var shardArr [sc.NShards]ShardEntry
	labdec.Decode(&shardArr)

	var session map[int64]t.SessionEntry
	labdec.Decode(&session)

	kv.printfL("install snapshot")
	kv.config = config
	kv.shardArr = shardArr
	kv.SetSessionL(session)
}

package kvraft

import (
	"bytes"

	"6.824/labgob"
)

func (kv *KVServer) persistDataL() []byte {
	writebuf := new(bytes.Buffer)
	labenc := labgob.NewEncoder(writebuf)
	labenc.Encode(kv.db)
	labenc.Encode(kv.session)
	return writebuf.Bytes()
}

func (kv *KVServer) readPersistL(data []byte) {
	if len(data) < 1 {
		return
	}
	readbuf := bytes.NewBuffer(data)
	labdec := labgob.NewDecoder(readbuf)

	var db map[string]string
	labdec.Decode(&db)

	var session map[int64]int
	labdec.Decode(&session)

	kv.printfL("install snapshot")
	kv.db = db
	kv.session = session
}

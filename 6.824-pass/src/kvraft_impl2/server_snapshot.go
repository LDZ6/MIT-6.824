package kvraft_impl2

import (
	"bytes"

	"6.824/labgob"
	t "6.824/pylog/template"
)

func (kv *KVServer) PersistDataL() []byte {
	writebuf := new(bytes.Buffer)
	labenc := labgob.NewEncoder(writebuf)
	labenc.Encode(kv.db)
	labenc.Encode(kv.CloneSessionL())
	return writebuf.Bytes()
}

func (kv *KVServer) ReadPersistL(data []byte) {
	if len(data) < 1 {
		return
	}
	readbuf := bytes.NewBuffer(data)
	labdec := labgob.NewDecoder(readbuf)

	var db map[string]string
	labdec.Decode(&db)

	var session map[int64]t.SessionEntry
	labdec.Decode(&session)

	kv.db = db
	kv.SetSessionL(session)
	kv.printfL("install snapshot")
}

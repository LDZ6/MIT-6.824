package template

import (
	"time"

	"6.824/pylog/util"
)

type SessionEntry struct {
	// [sequenceNum] -> is cmd exec successfully
	ConcurrentCmd   map[int]bool
	LastSequenceCmd int
}

func (sv *Server) updateSessionL(rpcArgs RpcArgs, state Err) {
	if state != OK {
		return
	}
	entry, exist := sv.session[rpcArgs.ClientId]
	if !exist {
		entry = SessionEntry{}
	}

	if rpcArgs.KeepOrder {
		entry.LastSequenceCmd =
			util.MaxInt(entry.LastSequenceCmd, rpcArgs.SequenceNum)
	} else {
		if entry.ConcurrentCmd == nil {
			entry.ConcurrentCmd = map[int]bool{}
		}
		entry.ConcurrentCmd[rpcArgs.SequenceNum] = true
		go func() {
			time.Sleep(2 * serverWaitApplyTimeout)
			sv.Lock()
			defer sv.Unlock()
			entry, exist := sv.session[rpcArgs.ClientId]
			if !exist {
				return
			}
			delete(entry.ConcurrentCmd, rpcArgs.SequenceNum)
		}()
	}

	sv.session[rpcArgs.ClientId] = entry
}

func (sv *Server) searchSessionL(rpcArgs RpcArgs) bool {
	entry, exist := sv.session[rpcArgs.ClientId]
	if !exist {
		return false
	}
	if rpcArgs.KeepOrder {
		return rpcArgs.SequenceNum <= entry.LastSequenceCmd
	} else {
		return entry.ConcurrentCmd[rpcArgs.SequenceNum]
	}
}

func (sv *Server) waitApply(rpcArgs RpcArgs) (exist bool) {
	exist = false
	t0 := time.Now()
	for !sv.Killed() && time.Since(t0) < serverWaitApplyTimeout {
		sv.Lock()
		exist = sv.searchSessionL(rpcArgs)
		sv.Unlock()
		if exist {
			return
		}
	}
	return
}

// for snapshot
func (sv *Server) SetSessionL(session map[int64]SessionEntry) {
	sv.session = session
}

func (sv *Server) CloneSessionL() map[int64]SessionEntry {
	clone := make(map[int64]SessionEntry, len(sv.session))
	for clientId, entry := range sv.session {
		if entry.LastSequenceCmd == 0 {
			continue
		}
		clone[clientId] = SessionEntry{
			ConcurrentCmd:   nil,
			LastSequenceCmd: entry.LastSequenceCmd,
		}
	}
	return clone
}

// for lab 4B to solve duplication
func (sv *Server) MergeSessionL(session map[int64]SessionEntry) {
	for clientId, entryIn := range session {
		entryThis, exist := sv.session[clientId]
		if !exist {
			entryThis = SessionEntry{}
		}
		entryThis.LastSequenceCmd =
			util.MaxInt(entryThis.LastSequenceCmd, entryIn.LastSequenceCmd)
		sv.session[clientId] = entryThis
	}
}

package util

import "sync"

const (
// DebugMapReduce bool = true

// DebugRaft bool = true

// DebugKvraft bool = true

// DebugTemplate bool = true

// DebugShardCtrler bool = true

// DebugShardKv bool = true
)

const (
	DebugMapReduce bool = false

	DebugRaft bool = false

	DebugKvraft bool = false

	DebugTemplate bool = false

	DebugShardCtrler bool = false

	DebugShardKv bool = false
)

type Mutex struct {
	/* github.com/sasha-s/go-deadlock */
	// deadlock.Mutex

	sync.Mutex
}

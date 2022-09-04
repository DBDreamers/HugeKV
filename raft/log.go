// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 当前规定的节点中已知处于一个稳固的存储中的最高entry的位置
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经被上层应用到状态机的最高entry的位置
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// entries中索引小于等于该stabled的,代表是还没有被持久化存储
	stabled uint64

	// all entries that have not yet compact.
	// 所有的entries
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 传入的不稳定的快照
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 获取初始化状态
	state, _, err := storage.InitialState()
	if err != nil {
		return nil
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return nil
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		return nil
	}
	raftLog := &RaftLog{
		storage:   storage,
		committed: state.Commit,
		stabled:   lastIndex,
		entries:   entries,
		applied:   firstIndex - 1,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	i, _ := l.storage.FirstIndex()
	if l.entries != nil && len(l.entries) > 0 && l.entries[0].Index <= i {
		l.entries = append(make([]pb.Entry, 0), l.entries[i-l.entries[0].Index:]...)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// unstable: index > stabled
	if l.entries == nil || len(l.entries) == 0 {
		return nil
	}
	if firstIndex := l.entries[0].Index; l.stabled >= firstIndex {
		return l.entries[l.stabled-firstIndex+1:]
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// 返回索引在[applied+1, committed+1)
	if l.entries == nil || len(l.entries) == 0 {
		return nil
	}
	firstIndex := l.entries[0].Index
	index := int(l.committed + 1 - firstIndex)
	if index > len(l.entries) {
		index = len(l.entries)
	}
	entries := l.entries[l.applied+1-firstIndex : l.committed+1-firstIndex]
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if l.entries == nil || len(l.entries) == 0 {
		index, err := l.storage.LastIndex()
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index > index {
			index = l.pendingSnapshot.Metadata.Index
		}
		if err != nil {
			return 0
		}
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) LastTerm() uint64 {
	if l.entries == nil || len(l.entries) == 0 {
		index, err := l.storage.LastIndex()
		if err != nil {
			return 0
		}
		entries, err := l.storage.Entries(index, index+1)
		if len(entries) == 0 {
			return 0
		}
		return entries[0].Term
	}
	return l.entries[len(l.entries)-1].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	if l.entries == nil || len(l.entries) == 0 || i < l.entries[0].Index {
		return l.storage.Term(i)
	}
	firstIndex := l.entries[0].Index
	if i < firstIndex {
		return l.storage.Term(i)
	}
	realIndex := i - firstIndex
	if int(realIndex) >= len(l.entries) || int(realIndex) < 0 {
		return 0, ErrUnavailable
	}
	return l.entries[int(realIndex)].Term, nil
}

func (l *RaftLog) FirstIndexOfTargetTerm(term uint64) uint64 {
	var left uint64 = 0
	var right = uint64(len(l.entries) - 1)
	for left <= right {
		mid := left + (right-left)/2
		t, err := l.Term(uint64(mid))
		if err != nil {
			return 0
		}
		if t < term {
			left = mid + 1
		} else if t > term {
			right = mid - 1
		} else {
			right = mid - 1
		}
	}
	if t, _ := l.Term(left); int(left) >= len(l.entries) || t != term {
		return 0
	}
	return l.entries[left].Index
}

func (l *RaftLog) FirstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	if l.entries == nil || len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i
	}
	m := l.entries[0].Index
	s, _ := l.storage.FirstIndex()
	if m < s {
		return m
	}
	return s
}

func (l *RaftLog) SliceIndexOfTargetIndex(index uint64) int {
	if l.entries == nil || len(l.entries) == 0 {
		return -1
	}
	return int(index - l.entries[0].Index)
}

//func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
//	if l.entries == nil || len(l.entries) == 0 {
//		entries, err := l.storage.Entries(lo, hi)
//		if err != nil {
//			return nil
//		}
//		return entries
//	}
//	firstUnstableIndex := l.entries[0].Index
//	if firstUnstableIndex <= lo {
//		return l.entries[lo-firstUnstableIndex : hi-firstUnstableIndex+1]
//	}
//	if firstUnstableIndex > hi {
//		entries, err := l.storage.Entries(lo, hi)
//		if err != nil {
//			return nil
//		}
//		return entries
//	}
//	entries, err := l.storage.Entries(lo, firstUnstableIndex)
//	if err != nil {
//		return nil
//	}
//	entries = append(entries, l.entries[0:hi-firstUnstableIndex+1]...)
//	return entries
//}

func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	if l.entries == nil || len(l.entries) == 0 {
		return nil
	}
	firstIndex := l.entries[0].Index
	return l.entries[lo-firstIndex : hi-firstIndex]
}

func (l *RaftLog) deleteEntries(start uint64) {
	// 删除从start开始的日志
	startRealIndex := l.SliceIndexOfTargetIndex(start)
	l.entries = l.entries[:startRealIndex]
	// 更新stabled
	if l.stabled >= start {
		l.stabled = start - 1
	}
}

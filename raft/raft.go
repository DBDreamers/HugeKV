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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	// Raft配置文件
	config Config
	// Raft的uid
	id uint64
	// 当前的任期
	Term uint64
	// 当前投票给的id
	Vote uint64
	// the log
	RaftLog *RaftLog
	// log replication progress of each peers
	Prs map[uint64]*Progress
	// this peer's role
	State StateType
	// votes records
	votes map[uint64]bool
	// 投出去的票数
	voteCount int
	// msgs need to send
	msgs []pb.Message
	// the leader id
	Lead uint64
	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	return &Raft{
		config:           *c,
		id:               c.ID,
		Term:             0,                     // 初始Term为0
		Vote:             0,                     // 0代表还没有给任何节点投票（Raft节点ID不为0）
		RaftLog:          nil,                   // project2AA不考虑
		Prs:              nil,                   // project2AA不考虑
		State:            StateFollower,         // 初始为StateFollower
		votes:            make(map[uint64]bool), // 记录自己给哪些节点投票，测试要用
		msgs:             make([]pb.Message, 0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0, // project2AA不考虑
		PendingConfIndex: 0, // project2AA不考虑
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
	} else {
		r.electionElapsed += 1
	}

	// 选举超时，发起选举
	if r.State != StateLeader && r.electionElapsed == r.electionTimeout {
		r.becomeCandidate()
		r.startElection()
	}

	// 发送heartbeat
	if r.State == StateLeader && r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		for _, p := range r.config.peers {
			if p != r.id {
				r.sendHeartbeat(p)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.resetElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 改变Raft状态
	// 重置electionElapsed和electionTimeout
	r.resetElectionTimeout()
	r.Term += 1
	r.State = StateCandidate
	// 给自己投票
	r.Vote = r.id
	r.voteCount = 1
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term    ***注意：project2AA并未实现***
	// 改变Raft状态
	r.State = StateLeader
	// 发送heartBeat
	r.heartbeatElapsed = 0
	for _, p := range r.config.peers {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 本地选举消息
			r.becomeCandidate()
			r.startElection()
		case pb.MessageType_MsgRequestVote:
			// 处理投票消息
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			// 处理心跳消息
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			// 处理日志追加
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.startElection()
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgBeat:
			// 发送心跳
			r.heartbeatElapsed = 0
			for _, p := range r.config.peers {
				if p != r.id {
					r.sendHeartbeat(p)
				}
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term >= r.Term {
		// Your Code Here (2A).
		r.electionElapsed = 0
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// handleRequestVote 处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	response := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}

	// 候选者Term比自己小，拒绝投票
	if m.Term < r.Term {
		r.msgs = append(r.msgs, response)
		return
	}

	// 候选者Term比自己大，直接变为Follower
	if m.Term > r.Term {
		// 称为follower
		r.becomeFollower(m.Term, m.From)
		// 该选票不拒绝,即成功投出选票
		response.Reject = false
		response.Term = m.Term
	}

	// 还未给任何节点投票(或者给当前候选者投过票)并且候选者log不落后自己，即可投票
	if (r.Vote == 0 || r.Vote == m.From) && r.checkLogs(m) {
		// 投票
		r.Vote = m.From
		r.electionElapsed = 0
		response.Reject = false
	}
	r.msgs = append(r.msgs, response)
}

// checkLogs 检查候选者的log是否不落后自己
func (r *Raft) checkLogs(m pb.Message) bool {
	// project2AA未实现
	return true
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 成功获得一个选票
	if m.Reject == false {
		r.voteCount++

		// 变为Leader
		if r.voteCount > len(r.config.peers)/2 && r.State == StateCandidate {
			r.becomeLeader()
		}
	}

	// 发现自己Term落后
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
}

// startElection 发起选举
func (r *Raft) startElection() {
	// 只有一个节点
	if len(r.config.peers) == 1 {
		r.becomeLeader()
		return
	}

	// 发送投票请求
	for _, p := range r.config.peers {
		if p != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
}

// resetElectionTimeout 重置ElectionTimeout为随机数
func (r *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	r.electionElapsed = 0
	r.electionTimeout = r.config.ElectionTick + rand.Intn(r.config.ElectionTick)
}

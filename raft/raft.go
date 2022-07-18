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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"strconv"
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

func (s *StateType) StateInfo() string {
	switch *s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return ""
}

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
	// 获得的选票
	voteSuccessCount int
	// 拒绝的选票
	voteFailCount int
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
	state, confState, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}
	var peers []uint64
	if c.peers == nil || len(c.peers) == 0 {
		peers = confState.Nodes
	} else {
		peers = c.peers
	}
	raft := Raft{
		config:           *c,
		id:               c.ID,
		Term:             state.Term,                             // 初始Term为0
		Vote:             state.Vote,                             // 0代表还没有给任何节点投票（Raft节点ID不为0）
		RaftLog:          newLog(c.Storage),                      // project2AA不考虑
		Prs:              make(map[uint64]*Progress, len(peers)), // project2AA不考虑
		State:            StateFollower,                          // 初始为StateFollower
		votes:            make(map[uint64]bool),                  // 记录自己给哪些节点投票，测试要用
		msgs:             make([]pb.Message, 0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0, // project2AA不考虑
		PendingConfIndex: 0, // project2AA不考虑
	}
	raft.log("peers: %v", peers)
	// initial prs
	for _, peerID := range peers {
		// matchIndex初始化为0, nextIndex初始化为lastLogIndex+1
		if peerID == c.ID {
			raft.Prs[peerID] = &Progress{raft.RaftLog.LastIndex(), raft.RaftLog.LastIndex() + 1}
			continue
		}
		raft.Prs[peerID] = &Progress{0, raft.RaftLog.LastIndex() + 1}
	}
	raft.log("init, raftLog: %+v", *raft.RaftLog)
	return &raft
}

func (r *Raft) GetId() uint64 {
	return r.id
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var entries []*pb.Entry
	low := r.Prs[to].Next
	preLogIndex := low - 1
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		return false
	}
	high := r.RaftLog.LastIndex()
	//if high < low {
	//	return false
	//}
	// 需要发送的是[nextIndex, lastLogIndex]
	src := r.RaftLog.Entries(low, high+1)
	for _, entry := range src {
		var newEntry = &pb.Entry{}
		*newEntry = entry
		entries = append(entries, newEntry)
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: entries,
		Index:   preLogIndex,
		LogTerm: preLogTerm,
		Commit:  r.RaftLog.committed,
	})
	r.log("send append to %d, index range: %d ~ %d ", to, low, high)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
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

		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		})
		if err != nil {
			return
		}
	}

	// 发送heartbeat
	if r.State == StateLeader && r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.sendHeartbeatOneRound()
	}
}

func (r *Raft) sendHeartbeatOneRound() {
	for p, _ := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.log("becomeFollower: term:" + strconv.Itoa(int(term)) + " lead:" + strconv.Itoa(int(lead)))
	}
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
	r.log("becomeCandidate")
	r.resetElectionTimeout()
	r.Term += 1
	r.State = StateCandidate
	// 给自己投票
	r.Vote = r.id
	r.voteSuccessCount = 1
	r.voteFailCount = 0
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term    ***注意：project2AA并未实现***
	// 改变Raft状态
	r.log("becomeLeader")
	r.State = StateLeader
	// 初始化nextIndex和matchIndex
	for id, pro := range r.Prs {
		if id != r.id {
			pro.Match = 0
			pro.Next = r.RaftLog.LastIndex() + 1
		}
	}
	// append一条no-op
	entry := &pb.Entry{Data: nil}
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{entry},
	})
	r.Prs[r.id].Match = entry.Index
	r.Prs[r.id].Next = entry.Index + 1
	if err != nil {
		return
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
		case pb.MessageType_MsgPropose:
			r.handleProposeForFollower(m)
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
			r.handleMsgBeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		}
	}
	return nil
}

func (r *Raft) handleMsgBeat(m pb.Message) {
	// 发送心跳
	r.heartbeatElapsed = 0
	r.sendHeartbeatOneRound()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.log("handle append from %d", m.From)
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, response)
		return
	}
	r.electionElapsed = 0
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	// 当前最新的日志索引落后
	term, err := r.RaftLog.Term(m.Index)
	if err != nil && err == ErrUnavailable {
		r.msgs = append(r.msgs, response)
		return
	}
	if r.RaftLog.LastIndex() < m.Index || (term != m.LogTerm) {
		//// preLogIndex处无日志,记录XTerm为0
		//response.LogTerm = 0
		//// 记录Index为当前的最新日志的Index
		//response.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, response)
		return
	}
	//// 若preLogIndex处的日志的term和preLogTerm不相等
	//term, err := r.RaftLog.Term(m.Index)
	//if err != nil {
	//	return
	//}
	//// preLogIndex处日志任期冲突
	//if term != m.LogTerm {
	//	// 冲突的任期
	//	response.LogTerm = term
	//	// 该冲突任期在日志中第一个位置
	//	targetTerm := r.RaftLog.FirstIndexOfTargetTerm(term)
	//	response.Index = targetTerm
	//}
	response.Reject = false
	// 追加日志
	// fmt.Printf("append: %v\n", m.Entries)
	for i, entry := range m.Entries {
		index := m.Index + uint64(i) + 1
		if index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if index < r.RaftLog.FirstIndex() {
			// 追加的日志没有被存储,即存在于快照中,则不处理
			continue
		} else {
			// 当日志冲突时
			term, _ := r.RaftLog.Term(index)
			if term != entry.Term {
				// 删除冲突日志当前和后续所有日志
				r.RaftLog.deleteEntries(index)
				// 把新日志添加进来
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
	}
	if r.RaftLog.LastIndex() >= m.Index && term == m.LogTerm && (m.Entries == nil || len(m.Entries) == 0) {
		// 当preLogIndex处日志匹配但是Entries为空,则只更新commitIndex为preLogIndex
		r.RaftLog.committed = m.Index
	} else {
		// 更新follower的commitIndex
		r.updateCommitIndexForFollower(m.Commit)
	}
	// 当成功接收日志后,Index记录为最新的日志索引
	response.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, response)
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
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, resp)
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
	r.log("handleRequestVote")
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
		// 成为follower
		r.becomeFollower(m.Term, None)
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
	lastTerm := r.RaftLog.LastTerm()
	return lastTerm == 0 || m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 发现自己Term落后
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	// 成功获得一个选票
	if m.Reject == false {
		r.voteSuccessCount++
		// 变为Leader
		if r.voteSuccessCount > len(r.Prs)/2 {
			r.becomeLeader()
			return
		}
	} else {
		r.voteFailCount++
		// 当有一半及以上的节点投了反对票,则变回follower
		if r.voteFailCount > len(r.Prs)/2 {
			r.becomeFollower(r.Term, 0)
			return
		}
	}

}

// startElection 发起选举
func (r *Raft) startElection() {
	r.log("startElection")
	// 只有一个节点
	r.Vote = r.id
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// 发送投票请求
	for p, _ := range r.Prs {
		if p != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: r.RaftLog.LastTerm(),
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

func (r *Raft) updateCommitIndexForFollower(commit uint64) {
	if commit > r.RaftLog.committed {
		r.RaftLog.committed = min(commit, r.RaftLog.LastIndex())
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	// 若返回失败
	if m.Reject {
		// 则nextIndex回退
		r.Prs[m.From].Next--
		// 重新发送
		r.sendAppend(m.From)
		return
	}
	// 若成功
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	// 检查一下是否可以更新leader的commitIndex
	r.updateCommitIndexForLeader()
}

func (r *Raft) handlePropose(m pb.Message) {
	// 处理Propose
	// 添加到本地
	r.log("handle propose")
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// 更新matchIndex, nextIndex
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	// 发送给peers
	if len(r.Prs) == 1 {
		// 更新commitIndex
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		r.sendAppendAllPeers()
	}
}

func (r *Raft) sendAppendAllPeers() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) handleProposeForFollower(m pb.Message) {
	// 当Follower收到Propose时,将该消息存下来等待发出去
	r.msgs = append(r.msgs, m)
}

func (r *Raft) updateCommitIndexForLeader() {
	//从lastLog开始
	for index := r.RaftLog.LastIndex(); index > r.RaftLog.committed; index-- {
		updateConNum := len(r.Prs) / 2
		num := 0
		for j, pro := range r.Prs {
			if j == r.id {
				continue
			}
			//若match >= index 而且log[i].Term == currentTerm则该server符合更新要求
			if term, _ := r.RaftLog.Term(index); pro.Match >= index && term == r.Term {
				num++
			}
		}
		//若过半数则更新commitIndex
		if num >= updateConNum {
			r.RaftLog.committed = index
			// 心跳通知来更新follower的commitIndex
			r.sendAppendAllPeers()
			break
		}
	}
}

const debug = true

func (r *Raft) log(format string, args ...interface{}) {
	if debug {
		outputColor := 30 + r.id
		sprintf := fmt.Sprintf("%c[0;40;%dm id[%v]state[%v]term[%v]commited[%v]applied[%v]: %c[0m",
			0x1B, outputColor, r.id, r.State.StateInfo(), r.Term, r.RaftLog.committed, r.RaftLog.applied, 0x1B)
		if len(args) != 0 {
			fmt.Printf(sprintf+format+"\n", args...)
		} else {
			fmt.Printf(sprintf + format + "\n")
		}
	}
}

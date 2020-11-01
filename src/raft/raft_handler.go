package raft

import (
	"sync"
)

func (rf *Raft) handleElectionTimeout() {
	rf.becomesCandidate()

	term := rf.getCurrentTerm()
	grantedNum := 1
	receivedNum := 1
	cond := sync.NewCond(&rf.mu)

	go rf.electionTimer()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(j int) {
			args := rf.buildRequestVoteArgs()
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(j, args, reply)

			var granted bool
			if ok {
				granted = rf.handleRequestVoteResponse(args, reply)
			}

			rf.mu.Lock()
			receivedNum++

			if granted {
				grantedNum++
			}

			cond.Signal()
			rf.mu.Unlock()

		}(i)
	}

	rf.mu.Lock()
	for receivedNum < len(rf.peers) && grantedNum < len(rf.peers)/2+1 {
		cond.Wait()
	}
	rf.mu.Unlock()

	if rf.getCurrentTerm() == term && rf.getStatus() == CANDIDATE && grantedNum > len(rf.peers)/2 {
		rf.becomesLeader()
	}
}

func (rf *Raft) handleRequestVoteResponse(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.updateTermWithoutLock(reply.Term)

	if rf.getCurrentTerm() != args.Term {
		return false
	}

	return reply.VoteGranted
}

func (rf *Raft) handleHeartBeatTimeout() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.appendEntries(i, true)
	}
}

func (rf *Raft) handleAppendEntriesResponse(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) bool {
	rf.updateTermWithoutLock(reply.Term)

	var retry bool
	var updated bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return false
	}

	if reply.Success {
		rf.nextIndex[server] = len(args.Entries) + args.PrevLogIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		//if len(args.Entries) != 0 {
		//	DPrintf("[%v] rf.nextIndex[%v] %v, rf.matchIndex[%v]: %v", rf.me, server, rf.nextIndex[server], server, rf.matchIndex[server])
		//}

		updated = rf.updateCommitIndexWithoutLock(args)
	} else {
		//rf.nextIndex[server]--
		if reply.XLen != 0 {
			rf.nextIndex[server] = reply.XLen
		} else {
			entry, ok := rf.getLastEntryByTermWithoutLock(reply.XTerm)
			if ok {
				rf.nextIndex[server] = entry.Index
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}

		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}

		//todo snapshot

		retry = true
	}

	if updated {
		rf.applyCond.Signal()
	}

	return retry
}

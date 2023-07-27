package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.leaderId = 0
	ck.clientId = time.Now().UnixNano()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.requestId = time.Now().UnixNano()
	args.clientId = ck.clientId

	var conf Config

	for {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply QueryReply
		//	ok := srv.Call("ShardMaster.Query", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return reply.Config
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)

		reply := &QueryReply{}

		ok := ck.servers[ck.leaderId].Call("ShardMaster.Query", args, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			conf = reply.Config
		}

		return conf
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.requestId = time.Now().UnixNano()
	args.clientId = ck.clientId

	for {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply JoinReply
		//	ok := srv.Call("ShardMaster.Join", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)

		reply := &JoinReply{}

		ok := ck.servers[ck.leaderId].Call("ShardMaster.Join", args, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.requestId = time.Now().UnixNano()
	args.clientId = ck.clientId

	for {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply LeaveReply
		//	ok := srv.Call("ShardMaster.Leave", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)

		reply := &LeaveReply{}

		ok := ck.servers[ck.leaderId].Call("ShardMaster.Leave", args, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.requestId = time.Now().UnixNano()
	args.clientId = ck.clientId

	for {
		// try each known server.
		//for _, srv := range ck.servers {
		//	var reply MoveReply
		//	ok := srv.Call("ShardMaster.Move", args, &reply)
		//	if ok && reply.WrongLeader == false {
		//		return
		//	}
		//}
		//time.Sleep(100 * time.Millisecond)

		reply := &MoveReply{}

		ok := ck.servers[ck.leaderId].Call("ShardMaster.Move", args, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}
}

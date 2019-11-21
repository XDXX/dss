use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, MutexGuard};

use futures::sync::mpsc::UnboundedSender;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

// use some crates
use crossbeam_channel;
use futures::future::Loop;
use futures::sync::oneshot;
use futures::{future, Future};
use rand::Rng;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// add some constants
// timeouts in original paper work fine.
const ELECTION_TIMEOUTS_LOWER_BOUND: u64 = 150;
const ELECTION_TIMEOUTS_UPPER_BOUND: u64 = 300;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(75);

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

// log entry.
#[derive(Clone)]
struct Entry {
    command: Vec<u8>,
    term: u64,
}

// change state message.
enum ServerState {
    Follower,
    Candidate,
    Leader,
}

impl Entry {
    fn new() -> Self {
        Entry {
            command: Vec::new(),
            term: 0,
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    leader_id: Option<usize>,
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<Entry>,
    commit_index: u64,
    last_applied: u64,
    apply_channel: UnboundedSender<ApplyMsg>,

    // Volatile state on leaders.
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    // Channels for thread communication
    to_follower_rx: crossbeam_channel::Receiver<bool>,
    to_follower_tx: crossbeam_channel::Sender<bool>,

    to_leader_rx: crossbeam_channel::Receiver<bool>,
    to_leader_tx: crossbeam_channel::Sender<bool>,

    log_replication: Vec<(
        crossbeam_channel::Sender<bool>,
        crossbeam_channel::Receiver<bool>,
    )>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let num_rafts = peers.len();

        // initialize channels
        let (to_follower_tx, to_follower_rx) = crossbeam_channel::unbounded();
        let (to_leader_tx, to_leader_rx) = crossbeam_channel::unbounded();

        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            leader_id: None,
            current_term: 0,
            voted_for: None,
            log: vec![Entry::new()],
            commit_index: 0,
            last_applied: 0,
            apply_channel: apply_ch,
            next_index: vec![0; num_rafts],
            match_index: vec![0; num_rafts],
            log_replication: Vec::with_capacity(num_rafts),
            to_follower_rx,
            to_follower_tx,
            to_leader_rx,
            to_leader_tx,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        let peer = &self.peers[server];
        let (tx, rx) = sync_channel(1);
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res)
                        .expect("send RpcFuture<RequestVoteReply> failed.");
                    Ok(())
                }),
        );
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
        tx: crossbeam_channel::Sender<(Result<AppendEntriesReply>, usize, usize)>,
    ) {
        let peer = &self.peers[server];

        // index of last entry in args.entries
        let last_index = args.prev_log_index as usize + args.entries.len();
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    let _ = tx.send((res, last_index, server));
                    Ok(())
                }),
        );
    }

    // if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to
    // state machine.
    fn check_and_apply_command(&mut self) {
        if self.commit_index > self.last_applied {
            for i in (self.last_applied + 1)..=self.commit_index {
                let msg = ApplyMsg {
                    command_valid: true,
                    command_index: i,
                    command: self.log[i as usize].command.clone(),
                };
                // apply command to state machine.
                self.apply_channel
                    .unbounded_send(msg)
                    .expect("apply command to state machine failed.")
            }
            self.last_applied = self.commit_index;
        }
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = self.log.len() as u64;
        let term = self.current_term;
        let is_leader = Some(self.me) == self.leader_id;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            self.log.push(Entry { command: buf, term });
            for i in 0..self.peers.len() {
                if i != self.me {
                    // notify other peers to replicate new command.
                    self.log_replication[i]
                        .0
                        .send(true)
                        .expect("send to log_replication failed.");
                }
            }
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;

        let _ = &self.last_applied;
        let _ = &self.apply_channel;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
        };

        // Start raft state machine with follower init state.
        // The state machine drive by thread pool inner Clients as a future.
        let start_state = ServerState::Follower;
        let raft_state_machine = future::loop_fn((start_state, node.clone()), |args| {
            let (state, node) = args;
            match state {
                ServerState::Follower => node.start_follower(),
                ServerState::Candidate => node.start_candidate(),
                ServerState::Leader => node.start_leader(),
            }
        });

        let raft = node.raft.lock().expect("lock raft peer failed");
        raft.peers[raft.me].spawn(raft_state_machine.map_err(|_| ()));
        drop(raft);

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let raft = self.raft.lock().unwrap();
        Some(raft.me) == raft.leader_id
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    // convert raft state to follower.
    fn start_follower(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        let raft = self.raft.lock().expect("lock raft peer failed");
        let to_follower_rx = raft.to_follower_rx.clone();
        let timeout: u64 = rand::thread_rng()
            .gen_range(ELECTION_TIMEOUTS_LOWER_BOUND, ELECTION_TIMEOUTS_UPPER_BOUND);
        let timeout = Duration::from_millis(timeout);

        // If election timeout elapses without receiving AppendEntries RPC from current leader
        // or granting vote to candidate: convert to candidate.
        thread::spawn(move || loop {
            crossbeam_channel::select! {
                recv(to_follower_rx) -> _ => continue,
                default(timeout) => {
                    let _ = sender.send(Loop::Continue((ServerState::Candidate, node)));
                    return;
                }
            }
        });

        receiver
    }

    // convert raft state to candidate
    fn start_candidate(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        // fetch some values from raft peer and do some reinitialization.
        let mut raft = self.raft.lock().expect("lock raft peer failed");
        let to_follower_rx = raft.to_follower_rx.clone();
        let to_follower_tx = raft.to_follower_tx.clone();
        let to_leader_rx = raft.to_leader_rx.clone();
        let to_leader_tx = raft.to_leader_tx.clone();
        let raft_peer_num = raft.peers.len();
        raft.current_term += 1;
        raft.voted_for = Some(raft.me);
        raft.leader_id = None;
        let current_term = raft.current_term;

        let mut request_vote_reply_vec = Vec::new();
        let request_vote_args = RequestVoteArgs {
            term: raft.current_term,
            candidate_id: raft.me as u64,
            last_log_index: (raft.log.len() - 1) as u64,
            last_log_term: raft.log.last().unwrap().term,
        };

        // send RequestVote RPCs to all other servers.
        for i in 0..raft_peer_num {
            if i != raft.me {
                request_vote_reply_vec.push(raft.send_request_vote(i, &request_vote_args));
            }
        }
        drop(raft);

        let timeout: u64 = rand::thread_rng()
            .gen_range(ELECTION_TIMEOUTS_LOWER_BOUND, ELECTION_TIMEOUTS_UPPER_BOUND);
        let timeout = Duration::from_millis(timeout);
        let (vote_tx, vote_rx) = crossbeam_channel::bounded(raft_peer_num / 2);

        // response to request vote result.
        for reply in request_vote_reply_vec {
            let vote_tx = vote_tx.clone();
            let raft = self.raft.clone();
            let to_follower_tx = to_follower_tx.clone();
            let to_leader_tx = to_leader_tx.clone();

            thread::spawn(move || {
                if let Ok(reply) = reply.recv() {
                    if let Ok(result) = reply {
                        // get a bigger term, convert to follower.
                        if result.term > current_term {
                            let mut raft = raft.lock().expect("lock raft peer failed");
                            raft.current_term = result.term;
                            raft.voted_for = None;
                            raft.leader_id = None;
                            to_follower_tx
                                .send(true)
                                .expect("send follower state faild");
                            return;
                        } else if result.vote_granted {
                            match vote_tx.send(true) {
                                Ok(_) => (),
                                Err(_) => return,
                            };
                            // receive votes from majority of servers: become leader.
                            if vote_tx.is_full() {
                                to_leader_tx.send(true).expect("send leader state failed");
                            }
                        }
                    }
                }
            });
        }

        // convert from current candidate state to other state.
        thread::spawn(move || {
            // move vote_rx to this scope to keep the vote channel connected.
            vote_rx.is_empty();

            crossbeam_channel::select! {
                recv(to_leader_rx) -> _ => sender.send(Loop::Continue((ServerState::Leader, node))),
                recv(to_follower_rx) -> _ => sender.send(Loop::Continue((ServerState::Follower, node))),
                default(timeout) => sender.send(Loop::Continue((ServerState::Candidate, node))),
            }
        });

        receiver
    }

    // convert raft state to leader
    fn start_leader(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        // fetch some values from raft peer and do some reinitialization.
        let mut raft = self.raft.lock().expect("lock raft peer failed");
        let to_follower_tx = raft.to_follower_tx.clone();
        let to_follower_rx = raft.to_follower_rx.clone();
        let raft_peer_num = raft.peers.len();
        let leader_id = raft.me;
        let current_term = raft.current_term;

        raft.leader_id = Some(raft.me);
        raft.next_index = vec![raft.log.len(); raft_peer_num];
        raft.match_index = vec![0; raft_peer_num];
        // clear the log replication channels.
        raft.log_replication.clear();
        for _ in 0..raft_peer_num {
            raft.log_replication.push(crossbeam_channel::unbounded());
        }
        let log_replication = raft.log_replication.clone();
        drop(raft);

        // send heartbeat to all other servers repeatedly.
        let (append_entries_reply_tx, append_entries_reply_rx) = crossbeam_channel::unbounded();
        let node_mv = node.clone();
        let append_entries_reply_tx_mv = append_entries_reply_tx.clone();
        thread::spawn(move || loop {
            let node = node_mv.clone();
            if node.is_leader() {
                let raft = node.raft.lock().expect("lock raft peer failed.");
                for i in 0..raft_peer_num {
                    if i != leader_id {
                        let next_index = raft.next_index[i];
                        let prev_log_term = raft.log[next_index - 1].term;
                        let prev_log_index = (next_index - 1) as u64;
                        let args = node.get_append_entries_args(
                            &raft,
                            Vec::new(),
                            (prev_log_index, prev_log_term),
                        );
                        raft.send_append_entries(i, &args, append_entries_reply_tx_mv.clone());
                    }
                }
            } else {
                return;
            }
            thread::sleep(HEARTBEAT_INTERVAL);
        });

        // response to AppendEntries RPC reply
        let raft = node.raft.clone();
        thread::spawn(move || loop {
            match append_entries_reply_rx.recv() {
                Err(_) => return,
                Ok((reply, last_index, server)) => {
                    if reply.is_err() {
                        continue;
                    }
                    let reply = reply.unwrap();
                    let mut raft = raft.lock().expect("lock raft peer failed");
                    // get a bigger term, convert to follower.
                    if reply.term > current_term {
                        raft.current_term = reply.term;
                        raft.voted_for = None;
                        raft.leader_id = None;
                        to_follower_tx
                            .send(true)
                            .expect("send follower state failed");
                        return;
                    }

                    // update nextIndex and matchIndex for follower.
                    if reply.success {
                        if raft.next_index[server] < last_index + 1 {
                            raft.next_index[server] = last_index + 1;
                            raft.match_index[server] = last_index;

                            // if there exists an N (last_index here) such that N > commitIndex,
                            // a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
                            // set commitIndex = N
                            let mut count: usize = 0;
                            for i in 0..raft_peer_num {
                                if i != leader_id && raft.match_index[i] >= last_index {
                                    count += 1;
                                    if (count >= raft_peer_num / 2)
                                        && last_index > raft.commit_index as usize
                                        && raft.log[last_index].term == current_term
                                    {
                                        raft.commit_index = last_index as u64;
                                        raft.check_and_apply_command();
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // if appendEntries fails because of log inconsistency: decrement nextIndex
                        // and retry.
                        if raft.next_index[server] > 1 {
                            raft.next_index[server] -= 1;
                        }
                        raft.log_replication[server]
                            .0
                            .send(true)
                            .expect("send to log_replication failed.");
                    }
                }
            }
        });

        // if last log index >= nextIndex for a follower: send AppendEntries RPC with log entries
        // starting at nextIndex.
        for (i, log_replication_ch) in log_replication.iter().enumerate() {
            if i == leader_id {
                continue;
            }
            let node = node.clone();
            let log_replication_rx = log_replication_ch.1.clone();
            let append_entries_reply_tx = append_entries_reply_tx.clone();
            thread::spawn(move || loop {
                if node.is_leader() {
                    if log_replication_rx.recv().is_err() {
                        return;
                    }
                    let raft = node.raft.lock().expect("lock raft peer failed.");
                    let last_log_index = raft.log.len() - 1;
                    let next_index = raft.next_index[i];
                    if last_log_index >= next_index {
                        let entries = raft.log[next_index..].to_vec();
                        let prev_log_term = raft.log[next_index - 1].term;
                        let prev_log_index = (next_index - 1) as u64;
                        let args = node.get_append_entries_args(
                            &raft,
                            entries,
                            (prev_log_index, prev_log_term),
                        );
                        raft.send_append_entries(i, &args, append_entries_reply_tx.clone());
                    }
                } else {
                    return;
                }
            });
        }

        // keep in leader state or convert to follower.
        thread::spawn(move || {
            to_follower_rx.recv().unwrap();
            let _ = sender.send(Loop::Continue((ServerState::Follower, node)));
        });

        receiver
    }

    // helper to generate append_entries_args.
    fn get_append_entries_args(
        &self,
        raft: &MutexGuard<Raft>,
        entries: Vec<Entry>,
        prev_log: (u64, u64),
    ) -> AppendEntriesArgs {
        let mut vec = Vec::with_capacity(entries.len());
        for entry in entries {
            vec.push(append_entries_args::Entry {
                command: entry.command,
                term: entry.term,
            });
        }

        AppendEntriesArgs {
            term: raft.current_term,
            leader_id: raft.me as u64,
            prev_log_index: prev_log.0,
            prev_log_term: prev_log.1,
            leader_commit: raft.commit_index,
            entries: vec,
        }
    }

    // receiver implementation of RequestVote RPC
    pub fn request_vote_handle(
        &self,
        args: RequestVoteArgs,
    ) -> oneshot::Receiver<RequestVoteReply> {
        let raft = self.raft.clone();
        let (sender, receiver) = oneshot::channel();

        // don't block the current thread.
        thread::spawn(move || {
            let mut raft = raft.lock().expect("lock raft peer failed");
            let term = raft.current_term;
            let mut vote_granted = false;

            // reply false if term < currentTerm
            if args.term < term {
                sender
                    .send(RequestVoteReply { term, vote_granted })
                    .expect("send requestVoteReply failed");
                return;
            } else if args.term > term {
                // get a bigger term, convert to follower.
                raft.current_term = args.term;

                if raft.voted_for.is_some() {
                    raft.to_follower_tx
                        .send(true)
                        .expect("send follower state failed");
                }
                raft.voted_for = None;
                raft.leader_id = None;
            }

            // check if the candidate's log is at least as up-to-date as receiver's log
            let check_log = || {
                let last_log = raft.log.last();
                match last_log {
                    None => panic!("Empty log[] in peer {}", raft.me),
                    Some(log) => {
                        log.term < args.last_log_term
                            || (log.term == args.last_log_term
                                && (raft.log.len() - 1) as u64 <= args.last_log_index)
                    }
                }
            };

            let voted_for = raft.voted_for;

            // if votedFor is null or candidateId, and candidate's log is up-to-date, grant vote
            if (voted_for.is_none() || voted_for.unwrap() as u64 == args.candidate_id)
                && check_log()
            {
                vote_granted = true;
                raft.voted_for = Some(args.candidate_id as usize);
            }

            sender
                .send(RequestVoteReply {
                    term: raft.current_term,
                    vote_granted,
                })
                .expect("send requestVoteReply failed");
        });

        receiver
    }

    // receiver implementation of AppendEntries RPC
    pub fn append_entries_handle(
        &self,
        args: AppendEntriesArgs,
    ) -> oneshot::Receiver<AppendEntriesReply> {
        let raft = self.raft.clone();
        let (sender, receiver) = oneshot::channel();

        thread::spawn(move || {
            let mut raft = raft.lock().expect("lock raft peer failed");
            let current_term = raft.current_term;

            // reply false if term < currentTerm
            if args.term < current_term {
                sender
                    .send(AppendEntriesReply {
                        term: current_term,
                        success: false,
                    })
                    .expect("send AppendEntries failed.");
                return;
            } else {
                // get a bigger term, convert to follower.
                if args.term > current_term {
                    raft.current_term = args.term;
                    raft.voted_for = None;
                }
                raft.leader_id = Some(args.leader_id as usize);
                raft.to_follower_tx
                    .send(true)
                    .expect("send follower state failed");
            }
            // reply false if log doesn't contain an entry at prevLogIndex
            // whose term matches prevLogTerm.
            let prev_log_index = args.prev_log_index as usize;
            let prev_log_term = args.prev_log_term;
            let log_length = raft.log.len();

            if log_length <= prev_log_index || raft.log[prev_log_index].term != prev_log_term {
                sender
                    .send(AppendEntriesReply {
                        term: raft.current_term,
                        success: false,
                    })
                    .expect("send AppendEntries failed.");
                return;
            }

            // if an existing entry conflicts with a new one, delete the existing
            // entry and all that follow it.
            let entries = args.entries;
            if raft.log.len() > prev_log_index + 1 {
                raft.log.truncate(prev_log_index + 1);
            }
            for entry in entries {
                raft.log.push(Entry {
                    command: entry.command,
                    term: entry.term,
                })
            }

            // if leaderCommit > commitIndex, set
            // commitIndex = min(leaderCommit, index of last new entry.
            if args.leader_commit > raft.commit_index {
                raft.commit_index = std::cmp::min(args.leader_commit, raft.log.len() as u64 - 1);

                // if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to
                // state machine.
                raft.check_and_apply_command();
            }

            sender
                .send(AppendEntriesReply {
                    term: raft.current_term,
                    success: true,
                })
                .expect("send AppendEntries failed.");
        });

        receiver
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let reply = self.request_vote_handle(args).map_err(labrpc::Error::Recv);
        Box::new(reply)
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let reply = self
            .append_entries_handle(args)
            .map_err(labrpc::Error::Recv);
        Box::new(reply)
    }
}

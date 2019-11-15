use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;

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
const ELECTION_TIMEOUTS_LOWER_BOUND: u64 = 300;
const ELECTION_TIMEOUTS_UPPER_BOUND: u64 = 500;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);

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

    to_candidate_rx: crossbeam_channel::Receiver<bool>,
    to_candidate_tx: crossbeam_channel::Sender<bool>,

    to_leader_rx: crossbeam_channel::Receiver<bool>,
    to_leader_tx: crossbeam_channel::Sender<bool>,
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
        let (to_candidate_tx, to_candidate_rx) = crossbeam_channel::unbounded();
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
            to_follower_rx,
            to_follower_tx,
            to_candidate_rx,
            to_candidate_tx,
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
        tx: crossbeam_channel::Sender<Result<AppendEntriesReply>>,
    ) {
        let peer = &self.peers[server];
        peer.spawn(
            peer.append_entries(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    let _ = tx.send(res);
                    Ok(())
                }),
        );
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
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
        let _ = &self.to_candidate_rx;
        let _ = &self.to_candidate_tx;
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

    fn start_follower(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        let mut raft = self.raft.lock().expect("lock raft peer failed");
        raft.leader_id = None;
        raft.voted_for = None;
        let to_follower_rx = raft.to_follower_rx.clone();
        let timeout: u64 = rand::thread_rng()
            .gen_range(ELECTION_TIMEOUTS_LOWER_BOUND, ELECTION_TIMEOUTS_UPPER_BOUND);
        let timeout = Duration::from_millis(timeout);

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

    fn start_candidate(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        let mut raft = self.raft.lock().expect("lock raft peer failed");
        let to_follower_rx = raft.to_follower_rx.clone();
        let to_follower_tx = raft.to_follower_tx.clone();
        let to_leader_rx = raft.to_leader_rx.clone();
        let to_leader_tx = raft.to_leader_tx.clone();
        let raft_peer_num = raft.peers.len();
        raft.current_term += 1;
        raft.voted_for = Some(raft.me);
        let current_term = raft.current_term;

        let mut request_vote_reply_vec = Vec::new();
        let request_vote_args = RequestVoteArgs {
            term: raft.current_term,
            candidate_id: raft.me as u64,
            last_log_index: raft.log.len() as u64,
            last_log_term: raft.log.last().unwrap().term,
        };
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

        for reply in request_vote_reply_vec {
            let vote_tx = vote_tx.clone();
            let raft = self.raft.clone();
            let to_follower_tx = to_follower_tx.clone();
            let to_leader_tx = to_leader_tx.clone();

            thread::spawn(move || {
                if let Ok(reply) = reply.recv() {
                    if let Ok(result) = reply {
                        if result.term > current_term {
                            let mut raft = raft.lock().expect("lock raft peer failed");
                            raft.current_term = result.term;
                            to_follower_tx
                                .send(true)
                                .expect("send follower state faild");
                        } else if result.vote_granted {
                            match vote_tx.send(true) {
                                Ok(_) => (),
                                Err(_) => return,
                            };

                            if vote_tx.is_full() {
                                to_leader_tx.send(true).expect("send leader state failed");
                            }
                        }
                    }
                }
            });
        }

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

    fn start_leader(&self) -> oneshot::Receiver<Loop<(), (ServerState, Node)>> {
        let (sender, receiver) = oneshot::channel();
        let node = self.clone();

        let mut raft = self.raft.lock().expect("lock raft peer failed");
        let to_follower_tx = raft.to_follower_tx.clone();
        let to_follower_rx = raft.to_follower_rx.clone();
        let raft_peer_num = raft.peers.len();
        let leader_id = raft.me;
        let current_term = raft.current_term;

        raft.leader_id = Some(raft.me);
        raft.next_index = vec![raft.log.len(), raft_peer_num];
        raft.match_index = vec![0, raft_peer_num];
        drop(raft);

        let (append_entries_reply_tx, append_entries_reply_rx) = crossbeam_channel::unbounded();

        for i in 0..raft_peer_num {
            if i != leader_id {
                let node = node.clone();
                let reply_tx = append_entries_reply_tx.clone();
                thread::spawn(move || loop {
                    if node.is_leader() {
                        let args = node.get_append_entries_args(Vec::new(), (0, 0));
                        node.raft
                            .lock()
                            .unwrap()
                            .send_append_entries(i, &args, reply_tx.clone());
                    } else {
                        return;
                    }
                    thread::sleep(HEARTBEAT_INTERVAL);
                });
            }
        }

        // response to AppendEntries RPC reply
        let raft = node.raft.clone();
        thread::spawn(move || loop {
            match append_entries_reply_rx.recv() {
                Err(_) => return,
                Ok(reply) => {
                    if reply.is_err() {
                        continue;
                    }
                    let reply = reply.unwrap();
                    if reply.term > current_term {
                        let mut raft = raft.lock().expect("lock raft peer failed");
                        raft.current_term = reply.term;
                        to_follower_tx
                            .send(true)
                            .expect("send follower state failed");
                        return;
                    }
                }
            }
        });
        //TODO: log replication

        thread::spawn(move || {
            to_follower_rx.recv().unwrap();
            let _ = sender.send(Loop::Continue((ServerState::Follower, node)));
        });
        receiver
    }

    fn get_append_entries_args(
        &self,
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
        let raft = self.raft.lock().expect("lock raft peer failed");
        AppendEntriesArgs {
            term: raft.current_term,
            leader_id: raft.me as u64,
            prev_log_index: prev_log.0,
            prev_log_term: prev_log.1,
            leader_commit: raft.commit_index,
            entries: vec,
        }
    }

    pub fn request_vote_handle(
        &self,
        args: RequestVoteArgs,
    ) -> oneshot::Receiver<RequestVoteReply> {
        let raft = self.raft.clone();
        let (sender, receiver) = oneshot::channel();

        thread::spawn(move || {
            let mut raft = raft.lock().expect("lock raft peer failed");
            let term = raft.current_term;
            let mut vote_granted = false;

            if args.term < term {
                sender
                    .send(RequestVoteReply { term, vote_granted })
                    .expect("send requestVoteReply failed");
                return;
            } else if args.term > term {
                raft.current_term = args.term;
                raft.to_follower_tx
                    .send(true)
                    .expect("send follower state failed");
            }

            let check_log = || {
                let last_log = raft.log.last();
                match last_log {
                    None => true,
                    Some(log) => {
                        log.term <= args.last_log_term
                            && raft.log.len() as u64 <= args.last_log_index
                    }
                }
            };

            let voted_for = raft.voted_for;
            if (voted_for.is_none() || voted_for.unwrap() as u64 == args.candidate_id)
                && check_log()
            {
                vote_granted = true;
                raft.voted_for = Some(args.candidate_id as usize);
            }

            sender
                .send(RequestVoteReply { term, vote_granted })
                .expect("send requestVoteReply failed");
        });

        receiver
    }

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
            } else if args.term >= current_term {
                raft.current_term = args.term;
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
            let mut entries = args.entries;
            for idx in 0..entries.len() {
                // skip entries already in the log.
                let idx_in_log = idx + prev_log_index + 1;
                if idx_in_log < log_length && raft.log[idx_in_log].term == entries[idx].term {
                    continue;
                } else if idx_in_log < log_length {
                    raft.log.truncate(idx_in_log);
                }

                // append entries not in log.
                for entry in entries.drain(idx..) {
                    raft.log.push(Entry {
                        command: entry.command,
                        term: entry.term,
                    });
                }
                break;
            }

            // if leaderCommit > commitIndex, set
            // commitIndex = min(leaderCommit, index of last new entry.
            if args.leader_commit > raft.commit_index {
                raft.commit_index = std::cmp::min(args.leader_commit, raft.log.len() as u64 - 1);
            }
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

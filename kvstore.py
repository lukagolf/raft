#!/usr/bin/env python3

import argparse, socket, time, json, select, struct, sys, math, os, random

END_MSG_PATTERN = b'}\n'
LEADER_ROLE = 'leader'
CANDIDATE_ROLE = 'candidate'
FOLLOWER_ROLE = 'follower'

BROADCAST = "FFFF"
SRC_KEY = "src"
DST_KEY = "dst"
KEY_KEY = "key"
LEADER_KEY = "leader"
TYPE_KEY = "type"
VALUE_KEY = "value"
MID_KEY = "MID"
CLIENT_KEY = "client"
NEW_LEADER_KEY = "new_leader"
TERM_KEY = "term"
PREV_LOG_INDEX_KEY = "prev_log_index"
PREV_LOG_TERM_KEY = "prev_term_index"
ENTRIES_KEY = 'entries'
LEADER_COMMIT_KEY = "leader_commit"
NEXT_INDEX_KEY = "next_index"
LAST_LOG_INDEX_KEY = "last_log_index"
LAST_LOG_TERM_KEY = "last_log_term"

GET_TYPE = "get"
PUT_TYPE = "put"
FAIL_TYPE = "fail"
REDIRECT_TYPE = "redirect"
OK_TYPE = "ok"
REQUEST_LEADER_ELECTION_TYPE = "request_leader_election"
ACCEPT_VOTE_TYPE = "accept_vote"
REJECT_VOTE_TYPE = "reject_vote"
APPEND_ENTRIES_TYPE = "append_entries"
ACCEPT_APPEND_TYPE = "accept_append"
REJECT_APPEND_TYPE = "reject_append"


class Replica:
    """
        Represents a replica inside a Raft distributed system.

        Attributes
        ----------
        port : int
            The port from which we will send and recieve messages from other replicas.
        id : str
            The unique identifier of this replica.
        others : str
            The IDs of the other replicas of the system.
        leader : str
            The ID of the replica believed to be the leader.
        curr_term : int
            The term that this replica is currently on.
        role : str
            The role this replica is currently acting in.
        vote_for : str
            The ID of the replica we are voting for in a round. Set to None in absence of an election.
        votes : list
            A list of IDs of the replicas who have voted for us. Is empty in absence of an election.
        election_timeout : int
            Represents the number of milliseconds we should wait for a heartbeat before starting a new election.
        heart_timeout : int 
            In the leader role, it represents the number of milliseconds we should wait before sending a heartbeat.
        append_timeout : int
            In the leader role, it represents the number of milliseconds we should wait before sending out an append RPC.
        new_put_time : int
            In the leader role, it is the time value of when we last sent out a PUT.
        last_heartbeat : int
            In the leader role, it is the tiem value of when we last sent out a heartbeat.
        last_log : int
            The time value of the last item in the log.
        db : dict
            Where we store the committed data.
        log : list
            The log of all the events in the system.
        commit_index : int
            The index of the last committed item in self.log.
        next_index : dict
            Holds the value of the next index for each of the other replicas.
        match_index : dict
            Holds the value of the matching index for each of the other replicas.
        socket : socket
            The socket object that this replica with communicate with.
    """

    def __init__(self, port, id, others):
        """
        The constructor for the Replica class. Initializes all class variables and broadcasts a hello message.
        """
        self.port = port
        self.id = id
        self.others = others
        self.leader = BROADCAST
        self.curr_term = 0
        self.role = FOLLOWER_ROLE
        self.vote_for = None
        self.votes = []
        self.election_timeout = random.randint(150,300)
        self.heartbeat_timeout = 149
        self.append_timeout = 10
        self.new_put_time = None 

        self.last_heartbeat = time.time()
        self.last_log = time.time()

        self.db={}
        self.log = [{TERM_KEY: 0}]
        self.commit_index = 0


        self.next_index = {}
        self.match_index = {}

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', 0))

        print("Replica %s starting up" % self.id, flush=True)
        hello = { "src": self.id, "dst": BROADCAST, "leader": BROADCAST, "type": "hello" }
        self.send(hello)
        print("Sent hello message: %s" % hello, flush=True)

    def send(self, message):
        """ Sends a packet with the given message to the port. """
        self.socket.sendto(json.dumps(message).encode('utf-8'), ('localhost', self.port))

    def majority(self):
        """ Calculates the number of replicas needed for a majorirty"""
        return (len(self.others)+2)//2

    def become_leader(self):
        """ Process for becoming a leader. This is called after you win an election."""
        print('Replica %s become a new leader' % self.id, flush=True)
        self.role = LEADER_ROLE
        self.leader = self.id
        self.vote_for = None
        self.votes = []
        
        self.next_index = {}
        self.match_index = {}
        for other_id in self.others:
            self.next_index[other_id] = len(self.log)
            self.match_index[other_id] = 0

    def become_follower(self, msg):
        """ Process for becoming a follower. This is called after you lose an election."""
        self.role = FOLLOWER_ROLE
        self.curr_term = msg[TERM_KEY]
        self.leader = msg[LEADER_KEY]
        self.voted_for = None
        self.votes = []

    def start_leader_election(self):
        """ Starts a new leader election. Called when the election timer times out. """
        print('Replica %s starting a new election\n' % self.id, flush=True)
        # print("Before starting elections - curr_term %s, role %s" % (self.curr_term, self.role), flush=True)
        
        self.curr_term += 1
        self.role = CANDIDATE_ROLE
        self.vote_for = self.id 
        self.votes = [self.id]
        
        for other_id in self.others:
            leader_msg = {
                SRC_KEY: self.id,
                DST_KEY: other_id,
                LEADER_KEY: self.id,
                TYPE_KEY: REQUEST_LEADER_ELECTION_TYPE,
                NEW_LEADER_KEY: self.id,
                TERM_KEY: self.curr_term,
                LAST_LOG_INDEX_KEY: len(self.log) - 1,
                LAST_LOG_TERM_KEY: self.log[len(self.log)-1][TERM_KEY]
            }
            self.send(leader_msg)

    def send_append_entries_to_id(self, other_id):
        """ Sends a AppendEntries RPC replica with the given ID. """
        if self.role == LEADER_ROLE:
            prev_log_index = self.next_index[other_id] - 1
            append_entries_msg = {
                SRC_KEY: self.id,
                DST_KEY: other_id,
                TYPE_KEY: APPEND_ENTRIES_TYPE,
                TERM_KEY: self.curr_term,
                LEADER_KEY: self.leader,
                PREV_LOG_INDEX_KEY: prev_log_index,
                PREV_LOG_TERM_KEY: self.log[prev_log_index][TERM_KEY],
                ENTRIES_KEY: self.log[self.next_index[other_id]:],
                LEADER_COMMIT_KEY: self.commit_index
            }
            #print("Replica %s append entries send msg '%s'" % (self.id, append_entries_msg), flush=True)
            self.send(append_entries_msg)

    def send_append_entries(self, immediately):
        """ Send an AppendEntries RPC if immediately is True or if enough time has passed. """
        curr_time = time.time()
        if immediately or (self.new_put_time is not None and (curr_time - self.new_put_time) * 1000 >= self.append_timeout):
            print("Replica %s append entries send msg %s" % (self.id, immediately), flush=True)
            for other_id in self.others:
                self.send_append_entries_to_id(other_id)
            self.new_put_time = None
            self.last_heartbeat = curr_time


    def handle_get_msg(self, msg):
        """ Handles the GET message based on the role of the replica. """
        if self.role == LEADER_ROLE:
            value = ""
            if msg[KEY_KEY] in self.db:
                value = self.db[msg[KEY_KEY]]
            data_msg = { 
                SRC_KEY: self.id,
                DST_KEY: msg[SRC_KEY],
                LEADER_KEY: self.leader,
                TYPE_KEY: OK_TYPE,
                MID_KEY: msg[MID_KEY],
                VALUE_KEY : value
            }
            #print("Replica %s received get msg '%s' and send answer: '%s'" % (self.id, msg, data_msg), flush=True)
            self.send(data_msg)
        else:
            redirect_msg = { 
                SRC_KEY: self.id,
                DST_KEY: msg[SRC_KEY],
                LEADER_KEY: self.leader,
                TYPE_KEY: REDIRECT_TYPE,
                MID_KEY: msg[MID_KEY] 
            }
            self.send(redirect_msg)
            #print("Replica %s sent redirect message: '%s'" % (self.id, redirect_msg), flush=True)

    def handle_put_msg(self, msg):
        if self.role == LEADER_ROLE:
            log_entry = {
                KEY_KEY: msg[KEY_KEY],
                VALUE_KEY : msg[VALUE_KEY],
                TERM_KEY: self.curr_term,
                MID_KEY: msg[MID_KEY] ,
                CLIENT_KEY: msg[SRC_KEY]
            }
            self.log.append(log_entry)
            
            #print("Replica %s received put msg: '%s'" % (self.id, msg), flush=True)
            if self.new_put_time is None:
                self.new_put_time = time.time()
            self.send_append_entries(False)
        else:
            redirect_msg = { 
                SRC_KEY: self.id,
                DST_KEY: msg[SRC_KEY],
                LEADER_KEY: self.leader,
                TYPE_KEY: REDIRECT_TYPE,
                MID_KEY: msg[MID_KEY] 
            }
            self.send(redirect_msg)
            #print("Replica %s sent redirect message: '%s'" % (self.id, redirect_msg), flush=True)

    def handle_request_leader_election_msg(self, msg):
        """ Handles the request leader message coming from a candidate replica. """
        # print ("Replica %s: vote_for %s, msg_term %s, cur_term %s" % (self.id, self.vote_for, msg[TERM_KEY], self.curr_term), flush=True)
        if msg[TERM_KEY] >= self.curr_term:
            self.role = FOLLOWER_ROLE
            self.curr_term = msg[TERM_KEY]
            self.leader = msg[LEADER_KEY];
            self.vote_for = None
            self.votes = []
        if self.role == FOLLOWER_ROLE:
            if (msg[TERM_KEY] < self.curr_term
                or ( not (self.vote_for is None or self.vote_for == msg[SRC_KEY]))
                or len(self.log) - 1 > msg[LAST_LOG_INDEX_KEY] 
                or self.log[len(self.log) - 1][TERM_KEY] > msg[LAST_LOG_TERM_KEY]):
                print("Replica %s send reject vote message for leader '%s'" % (self.id, msg[SRC_KEY]), flush=True)
            
                reject_msg = {
                    SRC_KEY: self.id,
                    DST_KEY: msg[SRC_KEY],
                    LEADER_KEY: self.leader,
                    TYPE_KEY: REJECT_VOTE_TYPE,
                    TERM_KEY: self.curr_term
                }
                self.send(reject_msg)
            else:
                self.last_log = time.time()
                self.leader = msg[LEADER_KEY]
                self.curr_term =msg[TERM_KEY]
                self.vote_for = msg[SRC_KEY]
                print("Replica %s send accept vote message for leader '%s'" % (self.id, self.vote_for), flush=True)

                accept_msg = {
                    SRC_KEY: self.id,
                    DST_KEY: msg[SRC_KEY],
                    LEADER_KEY: self.leader,
                    TYPE_KEY: ACCEPT_VOTE_TYPE,
                    TERM_KEY: self.curr_term
                }
                self.send(accept_msg)

    def handle_reject_vote_msg(self, msg):
        """ Handles the reject vote replica. Used for when you are in Candidate mode."""
        print('Replica %s receive reject vote from %s' % (self.id, msg[SRC_KEY]), flush=True) 
        if self.role == CANDIDATE_ROLE and msg[TERM_KEY] >= self.curr_term:
            self.role = FOLLOWER_ROLE 
            self.curr_term = msg[TERM_KEY]
            self.voted_for = None
            self.votes = []

    def handle_accept_vote_msg(self, msg):
        """ Handles the vote from a replica. UBut is shiuld """
        print('Replica %s receive accept vote from %s' % (self.id, msg[SRC_KEY]), flush=True)          
        if self.role ==CANDIDATE_ROLE and msg[SRC_KEY] not in self.votes:
            self.votes.append(msg[SRC_KEY])
            if len(self.votes) >= self.majority():
                self.become_leader()
                
    def handle_append_entries_msg(self, msg):
        """ Handles an AppendEntries RPC (which goulc either be a"""
        # print('Replica %s handle append entrie from %s' % (self.id, msg[SRC_KEY]), flush=True)
        self.last_log = time.time()
        # ("msg_term: %s, curr_term %s, role %s" % (msg[TERM_KEY], self.curr_term, self.role), flush=True)
        if msg[TERM_KEY] >= self.curr_term:
            self.become_follower(msg)

        if self.role == FOLLOWER_ROLE and msg[TERM_KEY] >= self.curr_term:
            #print("LOG %s pp %s" % (self.log, msg[PREV_LOG_INDEX_KEY]), flush = True)
            #print ("prev_index: %s, log_len %d, prev_term %s" % (msg[PREV_LOG_INDEX_KEY], len(self.log), msg[PREV_LOG_TERM_KEY]), flush=True)
            if len(self.log) <= msg[PREV_LOG_INDEX_KEY]:
                reject_append_msg = {
                    SRC_KEY: self.id,
                    DST_KEY: msg[SRC_KEY],
                    LEADER_KEY: self.leader,
                    TYPE_KEY: REJECT_APPEND_TYPE,
                    TERM_KEY: self.curr_term,
                    NEXT_INDEX_KEY: len(self.log)
                }
                self.send(reject_append_msg)
            elif msg[PREV_LOG_INDEX_KEY]>=0 and self.log[msg[PREV_LOG_INDEX_KEY]][TERM_KEY] != msg[PREV_LOG_TERM_KEY]:
                err_index = 0 
                while self.log[err_index][TERM_KEY] != self.log[msg[PREV_LOG_INDEX_KEY]][TERM_KEY]:
                    err_index+=1
                reject_append_msg = {
                    SRC_KEY: self.id,
                    DST_KEY: msg[SRC_KEY],
                    LEADER_KEY: self.leader,
                    TYPE_KEY: REJECT_APPEND_TYPE,
                    TERM_KEY: self.curr_term,
                    NEXT_INDEX_KEY: err_index
                }
                self.send(reject_append_msg)
            else:
                self.log = self.log[:msg[PREV_LOG_INDEX_KEY]+1]
                self.log.extend(msg[ENTRIES_KEY])   
                if msg[LEADER_COMMIT_KEY] > self.commit_index:
                    new_commit_index = min(len(self.log)-1, msg[LEADER_COMMIT_KEY])
                    while self.commit_index < new_commit_index:
                            self.commit_index += 1
                            self.db[self.log[self.commit_index][KEY_KEY]] = self.log[self.commit_index][VALUE_KEY]

                if len(msg[ENTRIES_KEY]) > 0:
                    accept_append_msg = {
                        SRC_KEY: self.id,
                        DST_KEY: msg[SRC_KEY],
                        LEADER_KEY: self.leader,
                        TYPE_KEY: ACCEPT_APPEND_TYPE,
                        TERM_KEY: self.curr_term,
                        NEXT_INDEX_KEY: len(self.log)
                    }
                    self.send(accept_append_msg)

    def handle_grant_append_msg(self, msg):
        if self.role == LEADER_ROLE:
            self.next_index[msg[SRC_KEY]] = msg[NEXT_INDEX_KEY]
            self.match_index[msg[SRC_KEY]] = msg[NEXT_INDEX_KEY] - 1
            pos = len(self.log) - 1
            while pos >= self.commit_index:
                if self.log[pos][TERM_KEY]==self.curr_term:
                    count_commits = 0
                    for other_id in self.match_index:
                        if self.match_index[other_id] >= pos:
                            count_commits +=1
                    if count_commits >= self.majority() - 1:
                        while self.commit_index < pos:
                            self.commit_index += 1
                            self.db[self.log[self.commit_index][KEY_KEY]] = self.log[self.commit_index][VALUE_KEY]
                            ok_msg = {
                                SRC_KEY: self.id,
                                DST_KEY: self.log[self.commit_index][CLIENT_KEY],
                                LEADER_KEY: self.leader,
                                TYPE_KEY: OK_TYPE,
                                MID_KEY: self.log[self.commit_index][MID_KEY]
                            }
                            #print("Replica %s send commit put data '%s'" % (self.id, ok_msg), flush=True)     
                            self.send(ok_msg)
                pos -= 1


    def handle_reject_append_msg(self, msg):
        """ Handles a rejection to an AppendEntries method. Used when this replica is the leader. """
        if self.role == LEADER_ROLE:
            self.next_index[msg[SRC_KEY]]= msg[NEXT_INDEX_KEY]
        self.send_append_entries_to_id(msg[SRC_KEY]) 

    def handle_msg(self, msg):
        """ 
        Handler function for a message recieved on our socket. Just checks the message's type and passes
        the message to the appropriate type-specific handler function.
        """
        #print("Replica %s received message '%s'" % (self.id, msg), flush=True)
        #passed = time.time() - start_time
        #print('Replica %s received at %f:\n%s\n' %
        #     (my_id, passed, json.dumps(msg, indent=2)))
        if msg[TYPE_KEY] == GET_TYPE:
            self.handle_get_msg(msg)
        elif msg[TYPE_KEY] == PUT_TYPE:
            self.handle_put_msg(msg)
        elif msg[TYPE_KEY] == REQUEST_LEADER_ELECTION_TYPE:
            self.handle_request_leader_election_msg(msg)
        elif msg[TYPE_KEY] == REJECT_VOTE_TYPE:
            self.handle_reject_vote_msg(msg)
        elif msg[TYPE_KEY] == ACCEPT_VOTE_TYPE:
            self.handle_accept_vote_msg(msg)
        elif msg[TYPE_KEY] == APPEND_ENTRIES_TYPE:
            self.handle_append_entries_msg(msg)
        elif msg[TYPE_KEY] == ACCEPT_APPEND_TYPE:
            self.handle_grant_append_msg(msg)
        elif msg[TYPE_KEY] == REJECT_APPEND_TYPE:
            self.handle_reject_append_msg(msg) 
        else: 
            print("Replica %s received ERROR message '%s'" % (self.id, msg), flush=True)

    def run(self):
        """ Main entry point for our class. Sets up the socket and handles messages from replicas accordingly."""
        while True:

           # print("Replica %s time '%d' %d" % (self.id, time.time() - self.last_msg_time, self.election_timeout), flush=True)

            socks = select.select([self.socket], [], [], 0.005)[0]
            for conn in socks:
                middle_priority_msgs = []
                low_priority_msgs = []
                try:
                    data = conn.recv(32768) +b'\n'
                    i=0
                    while END_MSG_PATTERN in data:
                        i+=1
                        end_position = data.find(END_MSG_PATTERN) + 1
                        msg =  json.loads(data[:end_position].decode('utf-8'))
                        data = data[end_position+1:]
                        if self.role == LEADER_ROLE and msg[TYPE_KEY] == ACCEPT_APPEND_TYPE:
                            self.handle_msg(msg)
                        elif self.role == FOLLOWER_ROLE and msg[TYPE_KEY] == APPEND_ENTRIES_TYPE:
                            self.handle_msg(msg)
                        elif msg[SRC_KEY] not in self.others:
                            middle_priority_msgs.append(msg)
                        else:
                            low_priority_msgs.append(msg)
                    if i> 1:
                        print(i, flush=True)
                    for msg in middle_priority_msgs:
                        self.handle_msg(msg)
                    for msg in low_priority_msgs:
                        self.handle_msg(msg)
                    
                except:
                   print("error", flush = True)
                   raise
            curr_time = time.time()

            if self.role != LEADER_ROLE and (curr_time - self.last_log)*1000 >= self.election_timeout:
                self.start_leader_election()
                self.last_log = curr_time
            elif self.role == LEADER_ROLE and (curr_time - self.last_heartbeat)*1000 >= self.heartbeat_timeout:
                #print("Replica %s send heartbeat msg" % (self.id), flush=True)     
                self.send_append_entries(True)
            elif self.role != LEADER_ROLE:
                self.send_append_entries(False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run a key-value store')
    parser.add_argument('port', type=int, help="Port number to communicate")
    parser.add_argument('id', type=str, help="ID of this replica")
    parser.add_argument('others', metavar='others', type=str, nargs='+', help="IDs of other replicas")
    args = parser.parse_args()
    replica = Replica(args.port, args.id, args.others)
    replica.run()

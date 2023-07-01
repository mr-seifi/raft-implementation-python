from abc import ABC
import random
import time
import threading
from typing import List


class Role:
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Log:
    def __init__(self) -> None:
        self.term: int = 0
        self.command: str = None


class Message(ABC):
    def __init__(self) -> None:
        pass


class VoteRequest(Message):
    def __init__(self, node_id, current_term, log_length, last_term) -> None:
        self.node_id = node_id
        self.current_term = current_term
        self.log_length = log_length
        self.last_term = last_term
        super().__init__()


class VoteResponse(Message):
    def __init__(self, node_id, current_term, granted) -> None:
        self.node_id = node_id
        self.current_term = current_term
        self.granted = granted
        super().__init__()


class AppendEntriesRequest(Message):
    def __init__(self, node_id, log: Log) -> None:
        self.node_id = node_id
        self.log = log
        super().__init__()


class Node:
    NODES = []
    UPPER_BOUND = 3

    def __init__(self) -> None:
        # START -- STORE IN A STABLE SITUATION
        # TODO: RECOVER STABLE VARS
        self.node_id = len(self.NODES) + 1
        self.current_term: int = 0
        self.voted_for: int = None
        self.log: list = []
        self.commit_length: int = 0
        # END -- STORE IN A STABLE SITUATION
        self.current_role: Role = Role.FOLLOWER
        self.current_leader: Node = None
        self.votes_received: set = set()
        self.sent_length: list = [0] * Node.UPPER_BOUND
        self.acked_length: list = [0] * Node.UPPER_BOUND
        self.election_timer_enabled: bool = True

        Node.NODES.append(self)

    def recover_from_crash(self) -> None:
        # TODO: RECOVER STABLE VARS
        self.current_role: Role = Role.FOLLOWER
        self.current_leader: Node = None
        self.votes_received: set = set()
        self.sent_length: list = []
        self.acked_length: list = []

    def start_election_timer(self, recall=False) -> None:
        self.election_timeout = self._reset_election_timeout()
        if recall:
            self._check_election_timeout()
            return

        self.timeout_thread = threading.Thread(
            target=self._check_election_timeout, daemon=True
        )
        self.timeout_thread.start()

    def _reset_election_timeout(self) -> None:
        timeout_duration = random.uniform(1, 3)
        return time.monotonic() + timeout_duration

    def _check_election_timeout(self):
        while self.election_timer_enabled:
            current_time = time.monotonic()
            if current_time >= self.election_timeout:
                print("timed out")
                self.election_timeout = self._reset_election_timeout()
                break
            time.sleep(0.01)

        if self.election_timer_enabled:
            self.recall_election_timeout()

        self.election_timer_enabled = True
        return

    def suspect_leader_failure(self, recall=False) -> None:
        self.current_term += 1
        self.current_role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received.add(self.node_id)
        self.last_term = self.log[-1].term if len(self.log) > 0 else 0
        message = VoteRequest(
            node_id=self.node_id,
            current_term=self.current_term,
            log_length=len(self.log),
            last_term=self.last_term,
        )
        self.broadcast(message=message)
        self.start_election_timer(recall=recall)

    def broadcast(self, message: Message) -> None:
        ...

    def send(self, message: Message) -> None:
        ...

    def recall_election_timeout(self):
        self.suspect_leader_failure(recall=True)

    def receive_vote_request(self, message: VoteRequest):
        my_log_term = self.log[-1].term
        log_ok = (message.last_term > my_log_term) or (
            message.last_term == my_log_term and message.log_length >= len(self.log)
        )
        term_ok = (message.current_term > self.current_term) or (
            message.current_term == self.current_term
            and self.voted_for in [message.node_id, None]
        )

        if log_ok and term_ok:
            self.current_term = message.current_term
            self.current_role = Role.FOLLOWER
            self.voted_for = message.node_id
            vote_response = VoteResponse(
                node_id=self.node_id, current_term=self.current_term, granted=True
            )

        else:
            vote_response = VoteResponse(
                node_id=self.node_id, current_term=self.current_term, granted=False
            )

        self.send(message=vote_response)

    def receive_vote_response(self, vote_response: VoteResponse):
        if (
            self.current_role == Role.CANDIDATE
            and self.current_term == vote_response.current_term
            and vote_response.granted
        ):
            self.votes_received.add(vote_response.node_id)
            if len(self.votes_received) >= (len(Node.NODES) + 1) // 2:
                self.current_role = Role.LEADER
                self.current_leader = self.node_id
                self.election_timer_enabled = False
                for node in Node.NODES:
                    if node.node_id == self.node_id:
                        continue
                    self.sent_length[node.node_id - 1] = len(self.log)
                    self.acked_length[node.node_id - 1] = 0
                    self.replicate_log(
                        leader_id=self.node_id, follower_id=vote_response.node_id
                    )

        elif vote_response.current_term > self.current_term:
            self.current_term = vote_response.current_term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
            self.election_timer_enabled = False

    def broadcast_append_entries_request(self, message: AppendEntriesRequest):
        if self.current_role == Role.LEADER:
            self.log.append(message.log)
            self.acked_length[message.node_id - 1] = len(self.log)
            self.broadcast_append_entries_request_periodically()
        else:
            # TODO: Forward request to self.current_leader
            ...

    def broadcast_append_entries_request_periodically(self):
        if self.current_role == Role.LEADER:
            for node in Node.NODES:
                if node.node_id == self.node_id:
                    continue
                # TODO: ReplicateLog(nodeId, follower)

    def replicate_log(self, leader_id, follower_id):
        prefix_length = self.sent_length[follower_id - 1]
        suffix = self.log[prefix_length:]
        prefix_term = 0
        if prefix_length > 0:
            prefix_term = self.log[-1].term
        self.send_append_entries_request(
            leader_id=leader_id,
            current_term=self.current_term,
            prefix_len=prefix_length,
            prefix_term=prefix_term,
            commit_len=self.commit_length,
            suffix=suffix,
            follower_id=follower_id,
        )

    def send_append_entries_request(
        self,
        leader_id,
        current_term,
        prefix_len,
        prefix_term,
        commit_len,
        suffix,
        follower_id,
    ):
        ...

    def receive_append_entries_request(
        self, leader_id, current_term, prefix_len, prefix_term, commit_len, suffix
    ):
        if current_term > self.current_term:
            self.current_term = current_term
            self.voted_for = None
            self.election_timer_enabled = False
        if current_term == self.current_term:
            self.current_role = Role.FOLLOWER
            self.current_leader = leader_id
        log_ok = (len(self.log) >= prefix_len) and (
            prefix_len == 0 or self.log[-1].term == current_term
        )
        if (current_term == self.current_term) and log_ok:
            # TODO: AppendEntries(prefixLen, leaderCommit, suffix)
            ack = prefix_len + len(suffix)
            self.send_append_entries_response(
                node_id=self.node_id,
                leader_id=self.current_leader.node_id,
                current_term=self.current_term,
                ack=ack,
                success=True
            )
        else:
            self.send_append_entries_response(
                node_id=self.node_id,
                leader_id=self.current_leader.node_id,
                current_term=self.current_term,
                ack=0,
                success=False
            )

    def send_append_entries_response(
        self,
        node_id,
        leader_id,
        current_term,
        ack,
        success
    ):
        ...
    
    def append_entries(self, prefix_len, leader_commit, suffix):
        if len(suffix) > 0 and len(self.log) >= prefix_len:
            index = min(len(self.log), prefix_len + len(suffix)) - 1
            if self.log[index].term != suffix[index - prefix_len].term:
                self.log = self.log[0:prefix_len]
            if prefix_len + len(suffix) > len(self.log):
                for i in range(len(self.log) - prefix_len, len(suffix)):
                    self.log.append(suffix[i])
            if leader_commit > self.commit_length:
                for i in range(self.commit_length, leader_commit):
                    self.deliver_command(suffix[i].command)
                self.commit_length = leader_commit
                    
    def deliver_command(self, command):
        ...
    
    def receive_append_entries_response(self, follower_id, current_term, ack, success):
        if self.current_term == current_term and self.current_role == Role.LEADER:
            if success and ack >= self.acked_length[follower_id - 1]:
                self.sent_length[follower_id - 1] = ack
                self.acked_length[follower_id - 1] = ack
                # TODO: CommitEntries
            elif self.sent_length[follower_id - 1] > 0:
                self.sent_length[follower_id - 1] -= 1
                self.replicate_log(
                    leader_id=self.node_id,
                    follower_id=follower_id
                )
        elif current_term > self.current_term:
            self.current_term = current_term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
            self.election_timer_enabled = False
    
    def commit_log_entries(self):
        while self.commit_length < len(self.log):
            acks = 0
            for node in Node.NODES:
                if self.acked_length[node.node_id - 1] > self.commit_length:
                    acks += 1
            
            if acks >= (len(Node.NODES) + 1) // 2:
                self.deliver_command(command=self.log[self.commit_length].command)
                self.commit_length += 1
            else:
                break
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
                print('timed out')
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
                    # TODO: ReplicateLog(node_id, follower)
        elif vote_response.current_term > self.current_term:
            self.current_term = vote_response.current_term
            self.current_role = Role.FOLLOWER
            self.voted_for = None
            self.election_timer_enabled = False


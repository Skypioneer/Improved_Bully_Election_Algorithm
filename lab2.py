import selectors
from datetime import datetime
import socket
import pickle
import sys
from enum import Enum

BUF_SZ = 1024
ASSUME_FAILURE_TIMEOUT = 1000
PEER_DIGITS = 100


class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """

    # Incoming message is pending
    QUIESCENT = 'QUIESCENT'
    WAITING_FOR_OK = 'WAIT_OK'
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'
    WAITING_FOR_ANY_MESSAGE = 'WAITING'

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)


class Node(object):
    def __init__(self, gcd_address, next_birthday, su_id):
        """
        Constructs a Lab2 object to talk ti the given Group Coordinator Daemon

        :param gcd_address: host name port of the GCD
        :param next_birthday: datetime of next birthday
        :param su_id: SratteU id number
        """
        self.state = None
        self.leader = None  # the largest node
        self.members = {}
        self.states = {}  # each peer's state
        self.pcs = []
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        """

        :return:
        """
        print('STARTING WORK for pid {} on {}'.format(self.pid, self.listener_address))
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election()
        # self.debug_connect()

        while True:
            events = self.selector.select(20000)
            # mask events
            # fileobj is a socket
            for key, mask in events:
                if key.fileobj == self.listener:
                    print("accepting peer")
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    print("receive message from peer")
                    self.receive_message(key.fileobj)
                else:
                    print("sending message to peer")
                    self.send_message(key.fileobj)
            # self.check_timeouts()

    @staticmethod
    def start_a_server():
        """
        Start a socket bound to 'localhost' at a random port

        :return: listening socket and its address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))
        listener.listen()
        listener.setblocking(False)
        return listener, listener.getsockname()

    def join_group(self):
        """
        Join the group via the GCD

        :return: if the node didn't get a reasonable response from GCD
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            message_data = (self.pid, self.listener_address)
            print('JOIN {}, {}'.format(self.gcd_address, message_data))
            gcd.connect(self.gcd_address)
            self.members = self.send(gcd, 'JOIN', message_data, wait_for_reply=True)
            if type(self.members) != dict:
                raise TypeError("Only dict is accepted")

    @classmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SZ):
        """
        Send pickled message to GCD
        :param peer: GCD
        :param message_name: requirement for joining the group
        :param message_data: pid, listener_address
        :param wait_for_reply: if needed reply
        :param buffer_size: 1024
        :return: peer information
        """
        peer.sendall(pickle.dumps((message_name, message_data)))
        if wait_for_reply:
            print('Waiting for replay...')
            return Node.receive(peer)

    @staticmethod
    def receive(peer, buffer_size=BUF_SZ):
        """
        Unpickle peer message or raise ValueError

        :param peer: GCD
        :param buffer_size: 1024
        :return: peer information
        """
        packet = peer.recv(buffer_size)
        if not packet:
            raise ValueError('socket closed')
        data = pickle.loads(packet)
        if type(data) == str:
            print('Received peer response: {}, {}'.format(data[0], data[1]))
            data = (data, None)
        return data

    def accept_peer(self):
        print("accepting peer")
        ps, address = self.listener.accept()

        print("ps is {} and address is {}".format(ps, address))
        ps.setblocking(False)

        self.selector.register(ps, selectors.EVENT_READ)
        self.pcs.append(ps)
        '''
        except KeyError:
            self.seletor.unregister(ps)
            self.seletor.register(ps, selectors.EVENT_READ)
            self.pcs.append(ps)
        print("-------------------------------")
        '''

    def send_message(self, peer):
        for ps in self.pcs:
            if ps == peer:
                if self.states[peer] == State.WAITING_FOR_OK:
                    try:
                        pass
                        # print("I am sending message to {}".format(peer))
                        # print("message is {}".format(self.state.value))
                        # self.send(peer, self.state.value, self.members)
                    except ConnectionError as err:
                        print("fail to connect: {}", err)
                    except Exception as err:
                        print("error: {}", err)

                    # if state == State.SEND_ELECTION:
                    # self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
                    print("switching selector to event write")
                    # self.selector.modify(peer, selectors.EVENT_WRITE)
                    peer.close()
                    self.pcs.remove(peer)
                    self.state = State.WAITING_FOR_ANY_MESSAGE
                    self.selector.unregister(peer)
                    # self.selector.modify(peer, selectors.EVENT_READ)
                    # else:
                    # self.set_quiescent(peer

    def receive_message(self, peer):
        for ps in self.pcs:
            if ps == peer:
                try:
                    message = self.receive(peer)
                    print("receive_message {}".format(message))
                    if self.state == State.WAITING_FOR_ANY_MESSAGE:
                        # if receive ELECTION, reply ok, update member list
                        print("data[0] is {}".format(message[0]))
                        # print("data[0].value is {}".format(data[0].value))
                        data = message[0]
                        if type(data) == str:
                            data = (data, None)
                        if data[0] == 'ELECTION':
                            # self.selector.modify(peer, selectors.EVENT_WRITE)
                            # self.state = State.SEND_OK
                            self.states[peer] = State.WAITING_FOR_OK
                            print("in receive message election")
                            # self.start_election()
                            self.send(peer, "OK")
                            print("message[0] is {}".format(message[0]))
                            print("message[1] is {}".format(message[1]))
                            for entry in message[1]:
                                print("entry is {}".format(entry))
                                print("entry[0] is {}".format(entry[0]))
                                print("entry[1] is {}".format(entry[1]))
                                print("member[entry is {}]".format(message[1][entry]))
                                if entry not in self.members:
                                    self.members[entry] = message[1][entry]

                            print("before staring a new election, my self members is {}".format(self.members))
                            self.start_election()

                        elif data[0] == 'OK':
                            self.pcs.remove(peer)
                            peer.close()
                            print("Received OK")
                            self.selector.unregister(peer)

                        # self.update_members(data[1])
                except ValueError:
                    self.pcs.remove(peer)
                    peer.close()
                    print("socket might be closed")
                    self.selector.unregister(peer)
        '''
        # if receive COORDINATOR, update leader, member list
        elif data[0] == 'COORDINATOR':
            self.set_leader(peer)
            self.update_members(data[1])
        '''

    '''
    def check_timeouts(self):
        pass
    '''

    def set_leader(self, new_leader):
        print('My new leader is {}'.format(new_leader))
        self.leader = new_leader

    def get_state(self, peer=None, detail=False):
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state):
        self.states[0] = state
        # self.states[1] = Node.pr.now()

    def debug_connect(self):
        print("in debug connect")
        print("self.member is {}".format(self.members))
        self.state = State.SEND_ELECTION
        for member in self.members:
            print('debug connect')
            peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.members[member] == self.listener_address:
                continue

            peer.connect(self.members[member])
            packet = self.send(peer, self.state.value, self.members, wait_for_reply=False)
            print(packet, peer)
            self.states[peer] = State.SEND_OK
            self.pcs.append(peer)
            peer.setblocking(False)
            self.selector.register(peer, selectors.EVENT_READ)
        self.state = State.WAITING_FOR_ANY_MESSAGE
        print("exit debug connect")
        print("my current pscs is {}".format(self.pcs))
        # self.states[peer] = State.SEND_OK

    def start_election(self):
        print("self.pid is {}".format(self.pid))
        print("self.members is {}".format(self.members))
        isMaxId = True

        print("Finding isMaxID")
        for member in self.members:
            print(member)
            if member > self.pid:
                isMaxId = False
                break

        print("Starting a election")

        if isMaxId:
            print("I have the max ID, declaring the victory")
            # send coordinator message
            self.declare_victory()
        else:
            # send message to higher peer
            isAllTimeout = True
            # change your state to election in progress
            self.set_state(State.SEND_ELECTION)
            print('self.member is {}'.format(self.members))

            for member in self.members:
                print('debug-------------------------')
                print(self.pid)
                print('member is {}'.format(member))
                if member > self.pid:
                    peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    try:
                        peer.connect(self.members[member])
                        peer.settimeout(100)
                        packet = self.send(peer, 'ELECTION', self.members)
                        print(packet, peer)
                        isAllTimeout = False
                        self.pcs.append(peer)
                        peer.setblocking(False)
                        self.selector.register(peer, selectors.EVENT_READ)
                        self.states[peer] = State.SEND_OK
                    except socket.timeout:
                        print('time out for member {}'.format(member))
                        self.pcs.remove(peer)

            if isAllTimeout:
                self.declare_victory()
            else:
                self.state = State.WAITING_FOR_ANY_MESSAGE

    def declare_victory(self):
        # self.set_state(State.SEND_VICTORY)
        self.state = State.SEND_VICTORY
        for member in self.members:
            if self.pid == member:
                continue
            else:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer:
                    peer.connect(self.members[member])
                    peer.settimeout(10)
                    print("sending COORDINATOR message to all the members")
                    self.send(peer, 'COORDINATOR', self.members)
        self.state = State.WAITING_FOR_ANY_MESSAGE

    def update_members(self, their_idea_of_membership):
        pass

    @staticmethod
    def pr_now():
        return datetime.now().strftime('%H:%M:%S.%f')

    '''
    def pr_sock(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))
    '''

    def pr_leader(self):
        return 'unknown' if self.leader is None else ('self' if self.leader == self.pid else self.leader)


if __name__ == '__main__':

    if len(sys.argv) != 7:
        print("Missing argument")
        exit(1)

    # initialize Node class
    # node = Node(("cs2.seattleu.edu", "12334"), (datetime(2022, 11, 4, 0, 0, 0)), 4121260)
    node = Node((sys.argv[1], sys.argv[2]), (datetime(int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), 0, 0, 0)),
                sys.argv[6])

    node.run()


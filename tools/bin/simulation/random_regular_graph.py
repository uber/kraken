import json
import random
import sets

import networkx as nx


PEER_COUNT = 5000
DEGREE = 5
PIECE_COUNT = 125
PIECE_TRANSMIT_LIMIT = 10  # Number of pieces can be uploaded/downloaded per "sec"


class Peer(object):
    def __init__(self, name, piece_count):
        self.name = name
        self.neighbors = sets.Set()
        self.pieces = [0]*piece_count
        self.completed = 0
        self.time = 0

        self.uploaded_current_turn = 0
        self.downloaded_current_turn = 0

    def connect(self, other):
        self.neighbors.add(other)
        other.neighbors.add(self)

    def done(self):
        return self.completed == len(self.pieces)

    def fetch_step(self, time):
        if self.done():
            return

        if self.downloaded_current_turn >= PIECE_TRANSMIT_LIMIT:
            return

        candidates = []
        for n in self.neighbors:
            if n.uploaded_current_turn >= PIECE_TRANSMIT_LIMIT:
                continue

            for i in range(0, len(self.pieces)):
                if n.uploaded_current_turn >= PIECE_TRANSMIT_LIMIT:
                    continue

                if n.pieces[i] == 1 and self.pieces[i] == 0:
                    candidates.append((n, i))

        if len(candidates) == 0:
            return

        c = random.choice(candidates)

        self.pieces[c[1]] = 1
        self.completed += 1
        self.downloaded_current_turn += 1
        c[0].uploaded_current_turn += 1

        print ('Peer %s downloaded one piece from neighbor %s. Total completed: %d.' % (self.name, c[0].name, self.completed))

        if self.completed == len(self.pieces)-1:
            self.time = time
            print ('Peer %s finished downloading at time %d.' % (self.name, time))

    def fetch_cleanup(self):
        self.uploaded_current_turn = 0
        self.downloaded_current_turn = 0

class PeerManager(object):

    def __init__(self):
        self.peers = []

        g = nx.random_regular_graph(DEGREE, PEER_COUNT)
        for n in g:
            peer = Peer(str(n), PIECE_COUNT)
            self.peers.append(peer)

        for e in g.edges():
            print ('Peer %s connected to peer %s.' % (self.peers[e[0]].name, self.peers[e[1]].name))
            self.peers[e[0]].connect(self.peers[e[1]])

        # Set peer 0 to be the seeder]
        self.peers[0].pieces = [1]*PIECE_COUNT
        self.peers[0].completed = len(self.peers[0].pieces)

    def start(self):
        time = 0
        while True:
            print ('current time: %d.' % time)
            time += 1

            plan = []
            for p in self.peers:
                if not p.done():
                    for j in range(0, PIECE_TRANSMIT_LIMIT):
                        plan.append(p)
            random.shuffle(plan)
            for p in plan:
                p.fetch_step(time)

            for p in self.peers:
                p.fetch_cleanup()

            done = True
            for p in self.peers:
                if p.completed != len(p.pieces):
                    done = False

            if done:
                break

            if time > 1000:
                break

        print ('Done. Total time: %d.' % time)


def main():
    peer_manager = PeerManager()
    peer_manager.start()

if __name__== "__main__":
     main()

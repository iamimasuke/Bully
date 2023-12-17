from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import multiprocessing 
import time

#helloこんい
#class of a node
class BullyNode:
    def __init__(self, node_id, port, processes):
        self.id = node_id
        self.port = port
        self.processes = processes
        self.leader_id = None
        self.election_timeout = 2
        self.in_election = False
        self.election_term = 1
        #RPCによるサーバーを立てる
        #これによりサーバー間で通信が可能に
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_instance(self)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()



    #Bully algorithmの選挙
    def election(self):
        print(f"Node {self.id} is starting an election.")
        print(f'今の選挙のタームは{self.election_term}です')
        self.in_election = True
        #自分よりidの大きなノードを探す。あれば選挙するように通達
        higher_nodes = [p for p in self.processes if p.id > self.id]
        election_results = set()

        for higher_node in higher_nodes:
            try:

                proxy = ServerProxy(f"http://localhost:{higher_node.port}")
                #response = proxy.send_election(self.id, self.election_term)
                #ノード1がノード2,3,4にsend_electionを送ったとき、ノード2のみ先々処理を進めていたらいけない
                #ノード2,3,4が同時に処理を進める必要がある
                p = multiprocessing.Process(target=proxy.send_election, args=(self.id, self.election_term))
                p.start()
                p.join()
                response = p.get()
                election_results.add(response)
            except Exception as e:
                print(f"Error sending election to node {higher_node.id}: {e}")
       
        print(f"選挙結果: {election_results}")
        
       
        if not election_results or self.id == max(election_results):
            self.become_leader()
        else:
            self.become_follower()

        self.in_election = False

    def send_election(self, sender_id, sender_election_term):
        self.election_term = sender_election_term + 1
        print(f"Node {self.id} received election from Node {sender_id}.")
        if self.in_election:
            return self.id
        elif sender_id > self.id:
            self.become_follower()
            self.reply_election(self.id)
        else:
            self.election()
            return self.id
        

    def reply_election(self, sender_id):
        print(f"Node {self.id} is replying to the election.")
        self.processes[sender_id].leader_id = None

        if self.in_election:
            for process in self.processes:
                if process.id > sender_id and process.id > self.id:
                    try:
                        proxy = ServerProxy(f"http://localhost:{process.port}")
                        proxy.reply_election(sender_id)
                    except Exception as e:
                        print(f"Error replying election to node {process.id}: {e}")

    def notify_leader(self, leader_id):
        print(f"Node {self.id} received leader notification from Node {leader_id}.")
        self.leader_id = leader_id

    def become_leader(self):
        print(f"Node {self.id} becomes the leader.")
        self.leader_id = self.id
        for process in self.processes:
            if process.id != self.id:
                try:
                    proxy = ServerProxy(f"http://localhost:{process.port}")
                    proxy.notify_leader(self.id)
                except Exception as e:
                    print(f"Error notifying leader to node {process.id}: {e}")

    def become_follower(self):
        print(f"Node {self.id} becomes a follower.")
        self.leader_id = None

    def run(self):
        pass

if __name__ == "__main__":
    nodes = [
        BullyNode(node_id=1, port=8001, processes=[]),
        BullyNode(node_id=2, port=8002, processes=[]),
        BullyNode(node_id=3, port=8003, processes=[]),
        BullyNode(node_id=4, port=8004, processes=[]),
    ]

    for node in nodes:
        node.processes = nodes
        
    nodes[0].election()

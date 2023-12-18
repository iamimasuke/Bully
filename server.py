from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import time

#class of a node
class BullyNode:
    def __init__(self, node_id, port, processes):
        self.id = node_id
        self.port = port
        self.processes = processes
        self.in_election = False
        self.election_term = 1
        self.nodes_replies = []
        #RPCによるサーバーを立てる
        #これによりサーバー間で通信が可能に
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_instance(self)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
    
    def stop_server(self):
        self.server.shutdown()
        self.server_thread.join()

    def __del__(self):
        self.stop_server()
        self.server_thread.join()       

    #選挙を開始する
    def election(self):
        lock = threading.Lock()
        threads = []
        self.nodes_replies = []#投票してそのレスポンスを受け取る
        print(f"Node {self.id}は選挙を開始します")
        print(f'私はNode {self.id}です。今の選挙のタームは{self.election_term}です')
        self.in_election = True
        #自分よりidの大きなノードを探す。あれば選挙するように通達
        higher_nodes = [p for p in self.processes if p.id > self.id]
        print(f'私はNode {self.id}です。私より大きいノードは{[p.id for p in higher_nodes]}です')    
        for higher_node in higher_nodes:
            thread = threading.Thread(target=self.send_election_to_node, args=(higher_node,))
            threads.append(thread)
            print(f'私はNode {self.id}です。Node{higher_node.id}のスレッドを作成しました')
            thread.start()   

        if self.id == max([p.id for p in self.processes]) and any(node.in_election for node in self.processes):
            print(f"私はNode {self.id}です。私が一番大きので退場します")
            return
        #処理パート１t1とt2の間に行いたい
        time.sleep(1)
        print(f"私はNode {self.id}です。リプライをくれたノードたちは{self.nodes_replies}です")
        if higher_nodes == self.nodes_replies.sort():  
            self.in_election = False
            print(f"Node {self.id}　選挙終了")
        if not any(node.in_election for node in self.processes):
            self.become_leader()
 
     #選挙を通知する
    def send_election_to_node(self, higher_node):
        try:
            proxy = ServerProxy(f"http://localhost:{higher_node.port}")
            proxy.send_election(self.id, self.election_term)
        except Exception as e:
            print(f"Error sending election to node {higher_node.id}: {e}")
            return None

    #選挙を送る
    def send_election(self, sender_id, sender_election_term):
        #senderにOKを返すスレッドと選挙をするスレッドを作成
        t1 = threading.Thread(target=self.send_node_ok, args=(self.id,sender_id))
        t2 = threading.Thread(target=self.election)
        t1.start()
        t1.join()
        #ここのタイミングで処理1を行いたい
        time.sleep(2)
        self.election_term = sender_election_term + 1
        print(f"Node {self.id} received election from Node {sender_id}.")
        t2.start()
        t2.join()
    
    #node2,3,4が送る
    def send_node_ok(self, self_id,sender_id):
        sender_port = 0
        for i in self.processes:
            if i.id == sender_id:
                sender_port = i.port
        proxy = ServerProxy(f"http://localhost:{sender_port}")
        proxy.recieve_ok(self.id)
    
    #node1が実行
    def recieve_ok(self, sender_id):
        print(f"Node {self.id} は node{sender_id}からOKを受け取りました")
        self.nodes_replies.append(sender_id)       

    def become_leader(self):
        print(f"【速報！！！！！】Node {self.id} is the new leader.")
        for node in self.processes:
            if node.id != self.id:
                proxy = ServerProxy(f"http://localhost:{node.port}")
                proxy.recieve_leader(self.id)
    
    def recieve_leader(self, leader_id):
        print(f"Node {self.id} received leader from Node {leader_id}.")
        self.leader_id = leader_id
        
    
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

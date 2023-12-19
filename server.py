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
        threads2 = []
        self.nodes_replies = []
        self.in_election_in_thread = True
        print(f"Node {self.id}は選挙を開始します")
        print(f'私はNode {self.id}です。今の選挙のタームは{self.election_term}です')
        self.in_election = True
        #自分よりidの大きなノードを探す。あれば選挙するように通達
        higher_nodes = [p for p in self.processes if p.id > self.id]
        print(f'私はNode {self.id}です。私より大きいノードは{[p.id for p in higher_nodes]}です')    
        for higher_node in higher_nodes:
            thread = threading.Thread(target=higher_node.reply, args=(self,))
            threads.append(thread)
            print(f'私はNode {self.id}です。Node{higher_node.id}のスレッドを作成しました')
            thread.start()   
        #hiher_nodes全てからリプライが返ってくるまで待つ
        #故障は考慮しない
        time.sleep(0.1)
        while len(higher_nodes) != len(self.nodes_replies):      
            time.sleep(0.01)

        print(f"私はNode {self.id}です。リプライをくれたノードたちは{self.nodes_replies}です")
        time.sleep(1)   
        self.in_election = False
        print(f"Node {self.id}　選挙終了")
        if not any(node.in_election for node in self.processes):
            self.become_leader()
        
        for higher_node in higher_nodes:   
            print(f'私はNode {self.id}です。Node{higher_node.id}の選挙開始用のスレッドを作成します')     
            thread2 = threading.Thread(target=higher_node.election())
            threads2.append(thread2)
            print(f'私はNode {self.id}です。Node{higher_node.id}の選挙開始用のスレッドを作成しました')
            thread2.start()   
        
    #node2,3が実行
    def reply(self,sender):
        print(f"Node {self.id} はNode {node.id}にリプライを送ります")
        self.in_election = True
        print(f"Node {self.id} は選挙中です。")
        proxy = ServerProxy(f"http://localhost:{sender.port}")
        proxy.receive_ok(self.id)

    def receive_ok(self, sender_id):
        print(f"Node {self.id} は node{sender_id}からOKを受け取りました")
        self.nodes_replies.append(sender_id)       

    def become_leader(self):
        print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!.")
        for node in self.processes:
            if node.id != self.id:
                proxy = ServerProxy(f"http://localhost:{node.port}")
                proxy.receive_leader(self.id)
    
    def receive_leader(self, leader_id):
        print(f"Node {self.id} received leader from Node {leader_id}.")
        self.leader_id = leader_id
        
    
if __name__ == "__main__":
    nodes = [
        BullyNode(node_id=1, port=8001, processes=[]),
        BullyNode(node_id=2, port=8002, processes=[]),
        BullyNode(node_id=3, port=8003, processes=[]),
        #BullyNode(node_id=4, port=8004, processes=[]),
    ]

    for node in nodes:
        node.processes = nodes
        
    nodes[0].election()

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import time

#class of a node
class BullyNode:

    election_terms = set()


    def __init__(self, node_id, port, processes):
        self.id = node_id
        self.port = port
        self.processes = processes
        self.in_election = False
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
    def election(self,self_term):
        self.election_terms.add(self_term)
        threads = []
        threads2 = []
        self.nodes_replies = []
        self.in_election_in_thread = True
        print(f"Node {self.id}は選挙を開始します。今の選挙のタームは{self_term}です")
        self.in_election = True
        #自分よりidの大きなノードを探す。あれば選挙するように通達
        higher_nodes = [p for p in self.processes if p.id > self.id]
        #print(f'私はNode {self.id}です。私より大きいノードは{[p.id for p in higher_nodes]}です')    
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
        for reply_node in self.nodes_replies:
            reply_nodes = [p for p in self.processes if p.id == reply_node][0]
            thread2 = threading.Thread(target=reply_nodes.election, args=(self_term+1,))
            threads2.append(thread2)
            print(f'私はNode {self.id}です。Node{reply_nodes.id}の選挙開始用のスレッドを作成しました')
            thread2.start()


        
        print(f"Node {self.id}　選挙終了　現在のタームは{self_term}です")
        if not any(node.in_election for node in self.processes):
            proxy = ServerProxy(f"http://localhost:{self.port}")
            proxy.become_leader(self_term)
        
        self.in_election = False
    

    #node2,3が実行
    def reply(self,sender):
        self.in_election = True
        sender.nodes_replies.append(self.id)
        print(f"Node {sender.id} はNode {self.id}からOKをもらいました")

    def send_term(self, node):
        pass
    
    def become_leader(self,self_term):
        time.sleep(1)
        print(f"self_terms: {self.election_terms}")
        if self_term == max(self.election_terms):
            print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!.")
            for node in self.processes:
                if node.id != self.id:
                    proxy = ServerProxy(f"http://localhost:{node.port}")
                    proxy.receive_leader(self.id)
        else:
            return
        
    def receive_leader(self, leader_id):
        print(f"Node {self.id} はリーダー通知を{leader_id}から受け取りました")
        self.leader_id = leader_id
        
    
if __name__ == "__main__":
    nodes = [
        BullyNode(node_id=1, port=8001, processes=[]),
        BullyNode(node_id=2, port=8002, processes=[]),
        BullyNode(node_id=3, port=8003, processes=[]),
        BullyNode(node_id=4, port=8004, processes=[]),
        BullyNode(node_id=5, port=8005, processes=[]),
        BullyNode(node_id=6, port=8006, processes=[]),

    ]

    for node in nodes:
        node.processes = nodes
        
    nodes[0].election(1)

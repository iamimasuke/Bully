from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import time
import pdb #デバック用

#class of a node
class BullyNode:

    processes = []
    processes_id = []
    proxies = []
    proxies_lock = threading.Lock()

    def __init__(self, node_id, port, is_down=False):
        self.id = node_id
        self.port = port
        self.in_election = False
        self.leader_id = None

        BullyNode.proxies.append(ServerProxy(f"http://localhost:{self.port}", allow_none=True))
        BullyNode.processes.append(self)
        BullyNode.processes_id.append(self.id)
        #RPCによるサーバーを立てる
        #これによりサーバー間で通信が可能に
        #故障Nodeはサーバーを立てない
        if not is_down:
            print(f"Node {self.id} は故障していません")
            self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
            self.server.register_instance(self)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()
        else:
            print(f"Node {self.id} は故障しています")
            

    #送信Nodeが複数のスレッドを自分の中に作成し、それぞれのスレッドで選挙を送信する
    def send_parallel_election(self):
        threads = []
        higher_nodes = [p for p in self.processes if p.id > self.id]
        for higher_node in higher_nodes:
            t=threading.Thread(target=self.send_election, args=(higher_node,))
            threads.append(t)
            t.start()
    
    def send_election(self,higher_node):
        with BullyNode.proxies_lock:
            try:
                print(f"Node {self.id} はNode {higher_node.id} に選挙を送信します")
                #electionの引数をselfなど複雑なオブジェクトにするとエラー得る
                #e: cannot marshal recursive dictionaries
                BullyNode.proxies[higher_node.id-1].election(self.id)
            except Exception as e:
                print(f"Node {higher_node.id} は故障しているので選挙を送信できません")
                print(f"エラー詳細：{e}")
                #一番大きいNodeが故障していて、自分が2番目に大きいNodeのとき自分がリーダーになる
                if higher_node.id == max([p.id for p in BullyNode.processes]):
                    nodes_ids = BullyNode.processes_id
                    nodes_ids.remove(higher_node.id)
                    if self.id == max(nodes_ids):
                        print(f"Node {self.id} はNode {higher_node.id} が故障しているのでリーダーになります")
                        self.become_leader()
                    else:
                        print(f"Node {self.id}は退場します")
                        return

   #node2,3が実行
    def election(self,from_node_id):
        #リーダーがいれば終了    
        if not self.leader_id is None:
            print(f"Node {self.id} はリーダーがいるので選挙を開始しません")
            return
        #リーダーがいなければリプ送って選挙開始
        else:
            #リプの送信
            t1 = threading.Thread(target=self.reply, args=(from_node_id,))
            t1.start()        
            #自分のidが最大ならリーダーになる
            if self.id == max([p.id for p in BullyNode.processes]):
                self.become_leader()
            else:
                t2 = threading.Thread(target=self.send_parallel_election,args=())
                t2.start()        
        
   #node2,3が別スレッドで実行
    def reply(self,from_node_id):
        if self.leader_id is None:
            print(f'Node {self.id} はNode {from_node_id} にリプライを送信しました')
            #選挙を送ったNodeにリプライを送信
            with BullyNode.proxies_lock:
                try:
                    BullyNode.proxies[from_node_id-1].receive_reply(self.id)
                except Exception as e:
                    print(f"Node {self.id} はNode {from_node_id} にリプライを送信できませんでした")
                    print(e)
    '''       
    def become_leader(self):
        if self.leader_id is None:
            self.leader_id = self.id
            print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!")
            pdb.set_trace()
            for node in BullyNode.processes:
                if node.id != self.id:
                    time.sleep(1)
                    with BullyNode.proxies_lock:
                        try:
                            print("これにてリーダー選挙を終了します")
                            print(f"リーダーはNode {self.id}です")
                            BullyNode.proxies[node.id-1].register_leader(self.id)                        
                        except Exception as e:
                            print(f"Node {self.id} は故障しているのでNode {node.id}にリーダー通知を送信できません")
                            print(f'エラー原因{e}')
        else:
            print(f"Node {self.id} はリーダーがいるのでリーダーになれません")
            return
    '''
    def become_leader(self):
        if self.leader_id is None:
            self.leader_id = self.id
            print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!")
            print("これにてリーダー選挙を終了します")
            print(f"リーダーはNode {self.id}です")
            for node in BullyNode.processes:
                if node.id != self.id:
                    try:
                        BullyNode.proxies[node.id-1].register_leader(self.id)
                    except Exception as e:
                        print(f"Node {self.id} は故障しているのでNode {node.id}にリーダー通知を送信できません")
                        print(f'エラー原因{e}')
        else:
            print(f"Node {self.id} はリーダーがいるのでリーダーになれません")
            return



    def register_leader(self,leader_id):
        self.leader_id = leader_id
        print(f"Node {self.id} はリーダー通知をNode {leader_id}から受け取りました")


    def receive_reply(self,reply_sender_id):
        print(f"Node {self.id} はNode {reply_sender_id} からリプライを受け取りました")


if __name__ == "__main__":
        
    node_1 = BullyNode(1, 8001)
    node_2 = BullyNode(2, 8002)
    node_3 = BullyNode(3, 8003, is_down=True)
        
    node_1.send_parallel_election()


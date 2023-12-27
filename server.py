from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import time
import pdb #デバック用

#class of a node
class BullyNode:

    processes = []
    processes_id = []


    def __init__(self, node_id, port, is_down=False):
        self.id = node_id
        self.port = port
        self.in_election = False
        self.leader_id = None
        replies = []
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


    #リーダーの故障に気づいたNodeが全てのNodeにリーダをリセットするRPCを送信する
    def reset_all_leader(self):
        for node_id in BullyNode.processes_id:
            if node_id != self.id:
                try:
                    node_port = BullyNode.processes[node_id-1].port
                    proxy = ServerProxy(f"http://localhost:{node_port}", allow_none=True)
                    proxy.reset_leader()
                except Exception as e:
                    print(f"Node {self.id} はNode {node_id} にreset_leaderのRPCを送信できませんでした") 
                    print(f"エラー詳細：{e}")
            else:
                self.reset_leader()
    

    def reset_leader(self):
        self.leader_id = None
        return
            

    #送信Nodeが複数のスレッドを自分の中に作成し、それぞれのスレッドで選挙を送信する
    def send_parallel_election(self):
        if self.leader_id is None:
            threads = []
            higher_nodes = [p for p in self.processes if p.id > self.id]
            for higher_node in higher_nodes:
                t=threading.Thread(target=self.send_election, args=(higher_node,))
                threads.append(t)
                t.start()
    
    #それぞれのスレッドがNodeに選挙を通知
    def send_election(self,higher_node):
        if self.leader_id is None:
            try:
                print(f"Node {self.id} はNode {higher_node.id} に選挙を送信します")
                #electionの引数をselfなど複雑なオブジェクトにするとエラー得る
                #e: cannot marshal recursive dictionaries
                node_port = BullyNode.processes[higher_node.id-1].port
                proxy = ServerProxy(f"http://localhost:{node_port}", allow_none=True)
                proxy.election(self.id)

            except Exception as e:
                print(f"Node {self.id} はNode {higher_node.id} にelectionのRPCを送信できませんでした") 
                print(f"エラー詳細：{e}")
                #一番大きいNodeが故障していて、自分が2番目に大きいNodeのとき自分がリーダーになる
                if higher_node.id == max(BullyNode.processes_id):
                    if self.id == max(BullyNode.processes_id)-1:                    
                        print(f"Node {self.id} はNode {higher_node.id} が故障しているのでリーダーになります")
                        self.become_leader()             
                    else:
                        time.sleep(2)
                        return
                     

   #選挙を受け取ったNodeが送信元にリプライを送り、自分より大きいNodeに選挙を通知
    def election(self,from_node_id):
        if self.leader_id is None:
            #リーダーがいれば終了    
            if not self.leader_id is None:
                print(f"Node {self.id} はリーダーがいるので選挙を開始しません")
                return
            #リーダーがいなければリプ送って選挙開始
            else:
                #リプの送信
                t1 = threading.Thread(target=self.reply, args=(from_node_id,))
                t1.start()
                t1.join()


                #自分のidが最大ならリーダーになる
                if self.id == max(BullyNode.processes_id):
                    self.become_leader()
                else:
                    t2 = threading.Thread(target=self.send_parallel_election,args=())
                    t2.start()        
        
   #リプライを送信
    def reply(self,from_node_id):
        if self.leader_id is None:
            print(f'Node {self.id} はNode {from_node_id} にリプライを送信しました')
            #選挙を送ったNodeにリプライを送信
            try:
                node_port = BullyNode.processes[from_node_id-1].port
                proxy = ServerProxy(f"http://localhost:{node_port}",  allow_none=True)
                proxy.receive_reply(self.id)

            except Exception as e:
                print(f"Node {self.id} はNode {from_node_id} にreceive_replyのRPCを送信できませんでした")
                print(f"エラー詳細：{e}")
                return


    #リプライを受け取る
    def receive_reply(self,reply_sender_id):
        if self.leader_id is None:
            #self.replies.append(reply_sender_id)
            print(f"Node {self.id} はNode {reply_sender_id} からリプライを受け取りました")
            return

    #リーダーになる
    #他のNodeにregister_leaderを送信
    def become_leader(self):
        if self.leader_id is None:
            self.leader_id = self.id
            print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!")
            print("これにてリーダー選挙を終了します")
            print(f"リーダーはNode {self.id}です")
            for node in BullyNode.processes:
                if node.id != self.id:
                    try:
                        node_port = BullyNode.processes[node.id-1].port
                        proxy = ServerProxy(f"http://localhost:{node_port}", allow_none=True)
                        proxy.register_leader(self.id)

                    except Exception as e:
                        print(f"Node {self.id}はNode {node.id} にregister_leaderのRPCを送信できませんでした")
                        print(f'エラー原因{e}')
                        return
        else:
            print(f"Node {self.id} はリーダーがすでにいます")
            return


    #自分のリーダーidに新しいリーダーのidを登録
    def register_leader(self,leader_id):
        if self.leader_id is None:
            self.leader_id = leader_id
            print(f"Node {self.id} はリーダー通知をNode {leader_id}から受け取りました")
            return


if __name__ == "__main__":

    node_1 = BullyNode(1, 8001)
    node_2 = BullyNode(2, 8002)
    node_3 = BullyNode(3, 8003)
    node_4 = BullyNode(4, 8004)
    #node_5 = BullyNode(5, 8005, is_down=True)

    node_1.send_parallel_election()

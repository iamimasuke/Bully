from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import threading
import time


#class of a node
class BullyNode:

    processes_id = []
    inital_port = 8000

    def __init__(self, node_id, port, is_down=False):
        self.id = node_id
        self.in_election = False
        self.leader_id = None
        self.replies = []
        BullyNode.processes_id.append(self.id)
        #これによりサーバー間で通信が可能に
        #故障Nodeはサーバーを立てない
        if not is_down:
            print(f"Node {self.id} は故障していません")
            self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
            self.server.register_instance(self)
            self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.server_thread.start()
        else:
            print(f"Node {self.id} は故障しています")


    #idからproxyを得る
    def getProxy(self, id):
        node_port = BullyNode.inital_port + id
        proxy = ServerProxy(f"http://localhost:{node_port}", allow_none=True)
        return proxy


    #リーダーの故障に気づいたNodeが全てのNodeにリーダをリセットするRPCを送信する
    def reset_all_leader(self):
        for node_id in BullyNode.processes_id:
            if node_id != self.id:
                try:
                    proxy = self.getProxy(node_id)
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
        self.replies = []
        if self.leader_id is None:
            threads = []
            higher_nodes_id = [p for p in BullyNode.processes_id if p > self.id]
            for higher_node_id in higher_nodes_id:
                t=threading.Thread(target=self.send_election, args=(higher_node_id,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            time.sleep(1)
            if self.replies == []:
                self.become_leader()
                
    #それぞれのスレッドがNodeに選挙を通知
    def send_election(self,higher_node_id):
        if self.leader_id is None:
            try:
                print(f"Node {self.id} はNode {higher_node_id} に選挙を送信します")
                proxy = self.getProxy(higher_node_id)
                reply = proxy.election()
                self.replies.append(reply)
            except Exception as e:
                print(f"Node {self.id} はNode {higher_node_id} にelectionのRPCを送信できませんでした") 
                print(f"エラー詳細：{e}")
            
            
   #選挙を受け取ったNodeが送信元にリプライを送り、自分より大きいNodeに選挙を通知
    def election(self):
        #リーダーがいれば終了    
        if not self.leader_id is None:
            print(f"Node {self.id} はリーダーがいるので選挙を開始しません")
            return
        else:
            #リプの送信
            #自分のidが最大ならリーダーになる
            if self.id == max(BullyNode.processes_id):
                self.become_leader()
            else:
                t = threading.Thread(target=self.send_parallel_election,args=(),daemon=True)   
                t.start()        
        return self.id
                    
    #リーダーになる
    #他のNodeにregister_leaderを送信
    def become_leader(self):
        if self.leader_id is None:
            self.leader_id = self.id
            print(f"【速報！！！！！】Node {self.id} がリーダーになりました!!")
            print(f"リーダーはNode {self.id}です")
            for node_id in BullyNode.processes_id:
                if node_id != self.id:
                    try:
                        proxy = self.getProxy(node_id)
                        proxy.register_leader(self.id)
                    except Exception as e:
                        print(f"Node {self.id}はNode {node_id} にregister_leaderのRPCを送信できませんでした")
                        print(f'エラー詳細：{e}')
                        #continue
            print("これにてリーダー選挙を終了します")
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
    node_4 = BullyNode(4, 8004, is_down=True)
    node_5 = BullyNode(5, 8005, is_down=True)
    

    node_1.send_parallel_election()
    time.sleep(3)
import bencoding
from .utils import generate_node_id


class KRPC:
    """
    实现了 DHT 的 KRPC 协议。
    """
    def __init__(self, node):
        self.node = node
        self.transaction_id = 0
        self.transactions = {}

    def _get_transaction_id(self):
        """
        生成一个唯一的事务ID。
        """
        self.transaction_id += 1
        return str(self.transaction_id).encode()

    def ping_query(self):
        """
        创建 ping 查询。
        """
        msg = {
            b't': self._get_transaction_id(),
            b'y': b'q',
            b'q': b'ping',
            b'a': {
                b'id': self.node.node_id
            }
        }
        return bencoding.bencode(msg)

    def find_node_query(self, target_id):
        """
        创建 find_node 查询。
        """
        msg = {
            b't': self._get_transaction_id(),
            b'y': b'q',
            b'q': b'find_node',
            b'a': {
                b'id': self.node.node_id,
                b'target': target_id
            }
        }
        return bencoding.bencode(msg)

    def get_peers_query(self, info_hash):
        """
        创建 get_peers 查询。
        """
        trans_id = self._get_transaction_id()
        self.transactions[trans_id] = {
            "info_hash": info_hash
        }
        msg = {
            b't': trans_id,
            b'y': b'q',
            b'q': b'get_peers',
            b'a': {
                b'id': self.node.node_id,
                b'info_hash': info_hash
            }
        }
        return bencoding.bencode(msg)

    def ping_response(self, trans_id, sender_id):
        """
        创建 ping 响应。
        """
        msg = {
            b't': trans_id,
            b'y': b'r',
            b'r': {
                b'id': sender_id
            }
        }
        return bencoding.bencode(msg)

    def handle_message(self, data, address):
        """
        处理收到的 KRPC 消息。
        """
        try:
            msg = bencoding.bdecode(data)
            if not isinstance(msg, dict):
                return

            msg_type = msg.get(b'y')

            if msg_type == b'r':
                self.handle_response(msg, address)
            elif msg_type == b'q':
                self.handle_query(msg, address)
            elif msg_type == b'e':
                self.handle_error(msg, address)
        except Exception:
            pass

    def handle_response(self, msg, address):
        """
        处理 KRPC 响应。
        """
        trans_id = msg.get(b't')
        if not trans_id:
            return

        transaction = self.transactions.get(trans_id)

        args = msg.get(b'r', {})
        if b'nodes' in args:
            self.node.handle_find_node_response(trans_id, args, address)
        elif b'values' in args and transaction:
            info_hash = transaction.get("info_hash")
            if info_hash:
                self.node.handle_get_peers_response(info_hash, args, address)

        if trans_id in self.transactions:
            del self.transactions[trans_id]

    def handle_query(self, msg, address):
        """
        处理 KRPC 查询。
        """
        trans_id = msg.get(b't')
        if not trans_id:
            return

        query_type = msg.get(b'q')
        args = msg.get(b'a', {})

        if query_type == b'ping':
            self.node.handle_ping_query(trans_id, args, address)
        elif query_type == b'find_node':
            self.node.handle_find_node_query(trans_id, args, address)
        elif query_type == b'get_peers':
            self.node.handle_get_peers_query(trans_id, args, address)
        elif query_type == b'announce_peer':
            self.node.handle_announce_peer_query(trans_id, args, address)

    def handle_error(self, msg, address):
        """
        处理 KRPC 错误。
        """
        pass

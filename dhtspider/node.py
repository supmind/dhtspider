import asyncio
import socket
import hashlib
from .krpc import KRPC
from .utils import generate_node_id, decode_nodes
from .fetcher import MetadataFetcher
from .storage import Storage


class Node(asyncio.DatagramProtocol):
    """
    DHT 节点类，实现了 DHT 协议的主要逻辑。
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.node_id = generate_node_id()
        self.peer_id = hashlib.sha1(self.node_id).digest()
        self.krpc = KRPC(self)
        self.transport = None
        self.routing_table = []
        self.seen_info_hashes = set()
        self.storage = Storage()

    def connection_made(self, transport):
        """
        当创建好 transport 时的回调。
        """
        self.transport = transport

    def datagram_received(self, data, addr):
        """
        当收到 UDP 数据报时的回调。
        """
        self.krpc.handle_message(data, addr)

    def error_received(self, exc):
        """
        当发生错误时的回调。
        """
        pass

    async def start(self):
        """
        启动 DHT 节点。
        """
        loop = asyncio.get_running_loop()
        try:
            self.transport, _ = await loop.create_datagram_endpoint(
                lambda: self, local_addr=(self.host, self.port)
            )
            print(f"DHT 节点正在监听 {self.host}:{self.port}")
            await self.bootstrap()
            asyncio.ensure_future(self.find_new_nodes())
        except Exception as e:
            print(f"启动节点时出错: {e}")


    def close(self):
        """
        关闭节点，保存数据。
        """
        self.storage.close()

    async def bootstrap(self):
        """
        通过连接到已知的 DHT 节点来引导节点。
        """
        bootstrap_nodes = [
            ("router.bittorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
            ("router.utorrent.com", 6881),
        ]
        for host, port in bootstrap_nodes:
            try:
                query = self.krpc.find_node_query(self.node_id)
                if self.transport:
                    self.transport.sendto(query, (host, port))
            except socket.gaierror:
                pass

    def handle_find_node_response(self, trans_id, args, address):
        """
        处理 find_node 的响应。
        """
        nodes = decode_nodes(args[b'nodes'])
        for node_id, ip, port in nodes:
            if (node_id, ip, port) not in self.routing_table:
                self.routing_table.append((node_id, ip, port))

    async def find_new_nodes(self):
        """
        持续发现新的节点。
        """
        while True:
            for node_id, ip, port in self.routing_table[:]:
                try:
                    target_id = generate_node_id()
                    query = self.krpc.find_node_query(target_id)
                    if self.transport:
                        self.transport.sendto(query, (ip, port))
                except Exception:
                    self.routing_table.remove((node_id, ip, port))
            await asyncio.sleep(1)


    def handle_ping_query(self, trans_id, args, address):
        """
        处理 ping 查询。
        """
        sender_id = args[b'id']
        response = self.krpc.ping_response(trans_id, self.node_id)
        if self.transport:
            self.transport.sendto(response, address)

    def handle_find_node_query(self, trans_id, args, address):
        """
        处理 find_node 查询。
        """
        pass

    def handle_get_peers_query(self, trans_id, args, address):
        """
        处理 get_peers 查询。
        """
        info_hash = args[b'info_hash']

        # 尝试从宣告者那里获取元数据
        try:
            fetcher = MetadataFetcher(info_hash, address, self.on_metadata_received, self.peer_id)
            asyncio.ensure_future(fetcher.fetch())
        except Exception:
            pass

        if info_hash not in self.seen_info_hashes:
            self.seen_info_hashes.add(info_hash)
            # 同时，继续向其他节点查询 peer
            for node_id, ip, port in self.routing_table:
                try:
                    query = self.krpc.get_peers_query(info_hash)
                    if self.transport:
                        self.transport.sendto(query, (ip, port))
                except Exception:
                    pass

    def handle_get_peers_response(self, info_hash, args, address):
        """
        处理 get_peers 的响应。
        """
        peers = args[b'values']
        for peer in peers:
            try:
                ip = socket.inet_ntoa(peer[:4])
                port = int.from_bytes(peer[4:], 'big')

                fetcher = MetadataFetcher(info_hash, (ip, port), self.on_metadata_received, self.peer_id)
                asyncio.ensure_future(fetcher.fetch())
            except Exception:
                pass

    async def on_metadata_received(self, info_hash, metadata):
        """
        当成功获取元数据时的回调。
        """
        try:
            name = metadata[b'name'].decode('utf-8', 'ignore')
            print(f"成功获取元数据: {name}")
            await self.storage.save(info_hash, name)
        except Exception as e:
            print(f"处理元数据时出错: {e}")

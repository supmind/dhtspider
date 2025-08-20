import asyncio
import socket
import hashlib
import random
import time
from .krpc import KRPC
from .utils import generate_node_id, decode_nodes
from .fetcher import MetadataFetcher
from .storage import Storage
from .kbucket import RoutingTable


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
        self.routing_table = RoutingTable(self.node_id)
        self.seen_info_hashes = set()
        self.storage = Storage()
        self.fetcher_semaphore = asyncio.Semaphore(100)

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
            self.routing_table.add_node((node_id, ip, port))

    async def find_new_nodes(self):
        """
        持续发现新的节点，并刷新旧的 bucket。
        """
        while True:
            # 刷新所有 bucket
            for bucket in self.routing_table.buckets:
                # 如果 bucket 在一段时间内没有更新，就刷新它
                if time.time() - bucket.last_updated > 600: # 10分钟
                    # 生成一个在 bucket 范围内的随机ID
                    target_id = random.randint(bucket.min_id, bucket.max_id - 1)
                    target_id_bytes = target_id.to_bytes(20, 'big')

                    # 向我们认识的最近的节点查询这个ID
                    closest_nodes = self.routing_table.get_closest_nodes(target_id_bytes)
                    for node_id, ip, port in closest_nodes:
                        try:
                            query = self.krpc.find_node_query(target_id_bytes)
                            if self.transport:
                                self.transport.sendto(query, (ip, port))
                        except Exception:
                            pass

            await asyncio.sleep(60) # 每分钟检查一次


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
        if info_hash not in self.seen_info_hashes:
            self.seen_info_hashes.add(info_hash)
            # 从K-Bucket中找到最近的节点并向它们查询
            closest_nodes = self.routing_table.get_closest_nodes(info_hash)
            for node_id, ip, port in closest_nodes:
                try:
                    query = self.krpc.get_peers_query(info_hash)
                    if self.transport:
                        self.transport.sendto(query, (ip, port))
                except Exception:
                    pass

    def handle_announce_peer_query(self, trans_id, args, address):
        """
        处理 announce_peer 查询。
        这是获取 info_hash 最直接的来源。
        """
        info_hash = args.get(b'info_hash')
        if not info_hash:
            return

        # 响应 announce_peer 查询
        response = self.krpc.ping_response(trans_id, self.node_id)
        if self.transport:
            self.transport.sendto(response, address)

        # 从宣告者那里获取元数据
        ip = address[0]
        implied_port = args.get(b'implied_port')
        if implied_port and implied_port != 0:
            port = address[1]
        else:
            port = args.get(b'port')

        if not port:
            return

        # 使用带并发限制的获取器
        asyncio.ensure_future(self.fetch_metadata(info_hash, (ip, port)))

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

    async def fetch_metadata(self, info_hash, address):
        """
        带并发限制地获取元数据。
        """
        async with self.fetcher_semaphore:
            fetcher = MetadataFetcher(info_hash, address, self.on_metadata_received, self.peer_id)
            await fetcher.fetch()

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

import asyncio
import socket
import hashlib
import random
import time
import logging
from .krpc import KRPC
from .utils import generate_node_id, decode_nodes
from .fetcher import MetadataFetcher
from .storage import Storage
from .kbucket import RoutingTable
from .config import (
    BOOTSTRAP_NODES,
    BLOOM_FILTER_CAPACITY,
    BLOOM_FILTER_ERROR_RATE,
    BLOOM_FILTER_FILE,
    FETCHER_SEMAPHORE_LIMIT,
    BUCKET_REFRESH_INTERVAL,
    FIND_NODES_INTERVAL,
    STATUS_REPORT_INTERVAL
)
from pybloom_live import BloomFilter


class Node(asyncio.DatagramProtocol):
    """
    DHT 节点类，实现了 DHT 协议的主要逻辑。
    """
    def __init__(self, host, port, bloom_filter_file=None):
        self.host = host
        self.port = port
        self.node_id = generate_node_id()
        self.peer_id = hashlib.sha1(self.node_id).digest()
        self.krpc = KRPC(self)
        self.transport = None
        self.routing_table = RoutingTable(self.node_id)
        self.bloom_filter_file = bloom_filter_file if bloom_filter_file is not None else BLOOM_FILTER_FILE
        self.seen_info_hashes = self._load_bloom_filter()
        self.storage = Storage()
        self.fetcher_semaphore = asyncio.Semaphore(FETCHER_SEMAPHORE_LIMIT)
        self.metadata_fetched_count = 0

    def _load_bloom_filter(self):
        """
        从文件加载布隆过滤器，如果文件不存在则创建一个新的。
        """
        try:
            with open(self.bloom_filter_file, 'rb') as f:
                bloom = BloomFilter.fromfile(f)
            logging.info("成功从 %s 加载布隆过滤器。", self.bloom_filter_file)
            return bloom
        except FileNotFoundError:
            logging.info("未找到布隆过滤器文件，将创建一个新的。")
            return BloomFilter(
                capacity=BLOOM_FILTER_CAPACITY,
                error_rate=BLOOM_FILTER_ERROR_RATE
            )

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
            logging.info("DHT 节点正在监听 %s:%s", self.host, self.port)
            await self.bootstrap()
            asyncio.ensure_future(self.find_new_nodes())
            asyncio.ensure_future(self._report_status())
        except Exception as e:
            logging.error("启动节点时出错: %s", e, exc_info=True)

    async def _report_status(self):
        """
        定期打印爬虫的状态信息。
        """
        while True:
            await asyncio.sleep(STATUS_REPORT_INTERVAL)
            logging.info(
                "[状态报告] 路由表节点: %d | 已见Infohash: %d | 已获取元数据: %d",
                len(self.routing_table),
                len(self.seen_info_hashes),
                self.metadata_fetched_count
            )

    def close(self):
        """
        关闭节点，保存数据。
        """
        # 保存布隆过滤器状态
        try:
            with open(self.bloom_filter_file, 'wb') as f:
                self.seen_info_hashes.tofile(f)
            logging.info("布隆过滤器已成功保存到 %s。", self.bloom_filter_file)
        except Exception as e:
            logging.error("保存布隆过滤器时出错: %s", e, exc_info=True)
        self.storage.close()

    async def bootstrap(self):
        """
        通过连接到已知的 DHT 节点来引导节点。
        """
        loop = asyncio.get_running_loop()
        for host, port in BOOTSTRAP_NODES:
            try:
                # 使用异步方式解析域名
                res = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_UDP)
                # res 是一个元组列表，我们取第一个结果
                # (family, type, proto, canonname, sockaddr)
                # sockaddr 是 (ip, port)
                family, _, _, _, sockaddr = res[0]

                query = self.krpc.find_node_query(self.node_id)
                if self.transport:
                    self.transport.sendto(query, sockaddr)
            except socket.gaierror:
                logging.warning("无法解析引导节点: %s:%s", host, port)
            except Exception as e:
                logging.error("引导过程中出现未知错误: %s", e, exc_info=True)

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
            # 1. 对我们自己的ID执行 find_node 查询以发现新节点
            # 这是更主动的节点发现策略
            closest_nodes = self.routing_table.get_closest_nodes(self.node_id)
            for node_id, ip, port in closest_nodes:
                try:
                    query = self.krpc.find_node_query(self.node_id)
                    if self.transport:
                        self.transport.sendto(query, (ip, port))
                except Exception:
                    pass

            # 2. 刷新所有 bucket 以保持路由表健康
            for bucket in self.routing_table.buckets:
                if time.time() - bucket.last_updated > BUCKET_REFRESH_INTERVAL:
                    # 生成一个在 bucket 范围内的随机ID
                    target_id = random.randint(bucket.min_id, bucket.max_id - 1)
                    target_id_bytes = target_id.to_bytes(20, 'big')

                    # 向 bucket 中最近的节点查询这个ID
                    # 注意：这里我们应该向 bucket 自己的节点查询，而不是全表最近
                    # 为了简单起见，我们仍然使用全表最近的节点
                    closest_to_target = self.routing_table.get_closest_nodes(target_id_bytes)
                    for node_id, ip, port in closest_to_target:
                        try:
                            query = self.krpc.find_node_query(target_id_bytes)
                            if self.transport:
                                self.transport.sendto(query, (ip, port))
                        except Exception:
                            pass

            await asyncio.sleep(FIND_NODES_INTERVAL)


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
            # 尝试解码名称用于打印，但这不再是存储的强制要求
            name = metadata.get(b'name', b'Unknown').decode('utf-8', 'ignore')
            logging.info("成功获取元数据: %s (infohash: %s)", name, info_hash.hex())
            # 将完整的元数据字典传递给存储层
            await self.storage.save(info_hash, metadata)
            self.metadata_fetched_count += 1
        except Exception as e:
            logging.error("处理或保存元数据时出错: %s", e, exc_info=True)

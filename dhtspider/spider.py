import asyncio
import logging
import hashlib

import bencoding as bencoder
from pybloom_live import BloomFilter

from .utils import generate_node_id, decode_nodes
from .fetcher import MetadataFetcher
from .storage import Storage

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)

class Spider(asyncio.DatagramProtocol):
    """
    一个单体的、maga风格的DHT爬虫实现。
    """
    def __init__(self, config, loop=None):
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.transport = None
        self.node_id = generate_node_id()
        self.peer_id = hashlib.sha1(self.node_id).digest()

        self.storage = Storage(self.config)
        self.seen_info_hashes = self._load_bloom_filter()
        self.fetcher_semaphore = asyncio.Semaphore(self.config["FETCHER_SEMAPHORE_LIMIT"])

        self.metadata_fetched_count = 0
        self.__running = False

    # --- Top-level control ---
    def start(self, port=6881):
        coro = self.loop.create_datagram_endpoint(
            lambda: self, local_addr=('0.0.0.0', port)
        )
        try:
            self.transport, _ = self.loop.run_until_complete(coro)
            logging.info("DHT Spider (maga-style) 正在监听 0.0.0.0:%d", port)
        except OSError as e:
            logging.error("无法监听在 0.0.0.0:%d - %s", port, e)
            return

        # Bootstrap
        for host, port in BOOTSTRAP_NODES:
            self.find_node(addr=(host, port))

        self.__running = True
        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        asyncio.ensure_future(self._report_status(), loop=self.loop)

    def stop(self):
        self.__running = False
        try:
            bloom_filter_file = self.config["BLOOM_FILTER_FILE"]
            with open(bloom_filter_file, 'wb') as f:
                self.seen_info_hashes.tofile(f)
            logging.info("布隆过滤器已成功保存到 %s。", bloom_filter_file)
        except Exception as e:
            logging.error("保存布隆过滤器时出错: %s", e)

    # --- Background tasks ---
    async def auto_find_nodes(self):
        while self.__running:
            for host, port in BOOTSTRAP_NODES:
                self.find_node(addr=(host, port))
            await asyncio.sleep(self.config["FIND_NODES_INTERVAL"])

    async def _report_status(self):
        while self.__running:
            logging.info(
                "[状态报告] 已见Infohash: %d | 已获取元数据: %d",
                len(self.seen_info_hashes),
                self.metadata_fetched_count
            )
            await asyncio.sleep(self.config["STATUS_REPORT_INTERVAL"])

    # --- DatagramProtocol implementation ---
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            msg = bencoder.bdecode(data)
        except Exception:
            return
        self.handle_message(msg, addr)

    # --- Message Handling ---
    def handle_message(self, msg, addr):
        msg_type = msg.get(b'y')
        if msg_type == b'r':
            self.handle_response(msg, addr)
        elif msg_type == b'q':
            asyncio.ensure_future(self.handle_query(msg, addr), loop=self.loop)

    def handle_response(self, msg, addr):
        args = msg.get(b'r', {})
        if b'nodes' in args:
            for node_id, ip, port in decode_nodes(args[b'nodes']):
                self.find_node(addr=(ip, port))

    async def handle_query(self, msg, addr):
        args = msg.get(b'a', {})
        sender_id = args.get(b'id')
        query_type = msg.get(b'q')

        logging.debug("收到来自 %s 的查询: %s", addr, query_type.decode(errors='ignore'))

        if query_type == b'get_peers':
            info_hash = args[b'info_hash']
            token = info_hash[:2]
            self.send_message({
                "t": msg[b"t"], "y": "r",
                "r": {"id": self._fake_node_id(sender_id), "nodes": "", "token": token}
            }, addr)

        elif query_type == b'announce_peer':
            info_hash = args.get(b'info_hash')
            self.send_message({
                "t": msg[b"t"], "y": "r", "r": {"id": self._fake_node_id(sender_id)}
            }, addr)

            if info_hash and info_hash not in self.seen_info_hashes:
                peer_addr = self._get_peer_addr(args, addr)
                if peer_addr:
                    await self.fetch_metadata(info_hash, peer_addr)

        elif query_type == b'find_node':
            self.send_message({
                "t": msg[b"t"], "y": "r",
                "r": {"id": self._fake_node_id(sender_id), "nodes": ""}
            }, addr)

        elif query_type == b'ping':
            self.send_message({
                "t": msg[b"t"], "y": "r", "r": {"id": self._fake_node_id(sender_id)}
            }, addr)

        self.find_node(addr=addr)

    # --- Core Logic ---
    async def fetch_metadata(self, info_hash, address):
        async with self.fetcher_semaphore:
            fetcher = MetadataFetcher(info_hash, address, self.on_metadata_received, self.peer_id)
            await fetcher.fetch()

    async def on_metadata_received(self, info_hash, metadata):
        try:
            name = metadata.get(b'name', b'Unknown').decode('utf-8', 'ignore')
            logging.info("成功获取元数据: %s (infohash: %s)", name, info_hash.hex())
            await self.storage.save(info_hash, metadata)
            self.metadata_fetched_count += 1
            self.seen_info_hashes.add(info_hash)
        except Exception as e:
            logging.error("处理或保存元数据时出错: %s", e)

    # --- Low-level methods ---
    def send_message(self, data, addr):
        data.setdefault(b"t", b"tt")
        if self.transport:
            self.transport.sendto(bencoder.bencode(data), addr)

    def find_node(self, addr, target=None):
        if not target:
            target = generate_node_id()
        self.send_message({
            "t": b"fn", "y": "q", "q": "find_node",
            "a": {"id": self.node_id, "target": target}
        }, addr)

    def _fake_node_id(self, target_id=None):
        if target_id:
            return target_id[:-1] + self.node_id[-1:]
        return self.node_id

    def _load_bloom_filter(self):
        bloom_file = self.config["BLOOM_FILTER_FILE"]
        try:
            with open(bloom_file, 'rb') as f:
                return BloomFilter.fromfile(f)
        except FileNotFoundError:
            return BloomFilter(
                capacity=self.config["BLOOM_FILTER_CAPACITY"],
                error_rate=self.config["BLOOM_FILTER_ERROR_RATE"]
            )

    def _get_peer_addr(self, args, addr):
        port = args.get(b'port') if args.get(b'implied_port', 1) == 0 else addr[1]
        return (addr[0], port) if port else None

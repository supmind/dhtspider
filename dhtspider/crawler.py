import asyncio
import socket
import hashlib
import logging

from .utils import decode_nodes, generate_node_id
from .fetcher import MetadataFetcher

class Crawler:
    """
    包含 DHT 爬虫核心逻辑的类。
    管理节点状态、K-RPC消息处理、节点发现和元数据抓取。
    """
    def __init__(self, config, node_id, storage, krpc, seen_info_hashes):
        self.config = config
        self.node_id = node_id
        self.peer_id = hashlib.sha1(self.node_id).digest()
        self.storage = storage
        self.krpc = krpc
        self.seen_info_hashes = seen_info_hashes

        self.fetcher_semaphore = asyncio.Semaphore(self.config["FETCHER_SEMAPHORE_LIMIT"])
        self.metadata_fetched_count = 0
        self.protocol = None

        # 设置从 KRPC 到自身的反向引用
        self.krpc.set_handler(self)

    def set_protocol(self, protocol):
        """
        由 Protocol 类调用，以注入对网络协议层的引用。
        """
        self.protocol = protocol

    async def start(self):
        """
        启动爬虫逻辑。
        """
        await self.bootstrap()
        asyncio.ensure_future(self.find_new_nodes())
        asyncio.ensure_future(self._report_status())

    async def _report_status(self):
        """
        定期打印爬虫的状态信息。
        """
        while True:
            await asyncio.sleep(self.config["STATUS_REPORT_INTERVAL"])
            logging.info(
                "[状态报告] 已见Infohash: %d | 已获取元数据: %d",
                len(self.seen_info_hashes),
                self.metadata_fetched_count
            )

    def close(self):
        """
        关闭爬虫，保存数据。
        """
        try:
            bloom_filter_file = self.config["BLOOM_FILTER_FILE"]
            with open(bloom_filter_file, 'wb') as f:
                self.seen_info_hashes.tofile(f)
            logging.info("布隆过滤器已成功保存到 %s。", bloom_filter_file)
        except Exception as e:
            logging.error("保存布隆过滤器时出错: %s", e, exc_info=True)
        self.storage.close()

    async def bootstrap(self):
        """
        通过连接到已知的 DHT 节点来引导节点。
        """
        loop = asyncio.get_running_loop()
        for host, port in self.config["BOOTSTRAP_NODES"]:
            try:
                res = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_UDP)
                family, _, _, _, sockaddr = res[0]
                query = self.krpc.find_node_query(self.node_id)
                if self.protocol:
                    self.protocol.sendto(query, sockaddr)
            except socket.gaierror:
                logging.warning("无法解析引导节点: %s:%s", host, port)
            except Exception as e:
                logging.error("引导过程中出现未知错误: %s", e, exc_info=True)

    def handle_find_node_response(self, trans_id, args, address):
        nodes = decode_nodes(args[b'nodes'])
        logging.debug("从 %s 收到了 %d 个新节点", address, len(nodes))
        for node_id, ip, port in nodes:
            try:
                logging.debug("向新发现的节点 %s:%s 发送 ping", ip, port)
                self.krpc.ping(addr=(ip, port), node_id=self._fake_node_id(node_id))
            except Exception:
                pass

    async def find_new_nodes(self):
        """
        持续向引导节点查询，以发现新节点。
        """
        while True:
            logging.debug("后台任务：开始新一轮的节点发现...")
            for host, port in self.config["BOOTSTRAP_NODES"]:
                try:
                    target_id = generate_node_id()
                    logging.debug("向引导节点 %s:%s 发送 find_node, 目标ID: %s", host, port, target_id.hex())
                    query = self.krpc.find_node_query(target_id)
                    if self.protocol:
                        self.protocol.sendto(query, (host, port))
                except Exception:
                    pass
            await asyncio.sleep(self.config["FIND_NODES_INTERVAL"])

    def handle_ping_query(self, trans_id, args, address):
        sender_id = args[b'id']
        response = self.krpc.ping_response(trans_id, self._fake_node_id(sender_id))
        if self.protocol:
            self.protocol.sendto(response, address)

    def handle_find_node_query(self, trans_id, args, address):
        sender_id = args[b'id']
        response = self.krpc.find_node_response(
            trans_id, self._fake_node_id(sender_id), ""
        )
        if self.protocol:
            self.protocol.sendto(response, address)

    def handle_get_peers_query(self, trans_id, args, address):
        info_hash = args[b'info_hash']
        sender_id = args[b'id']
        response = self.krpc.get_peers_response(
            trans_id, self._fake_node_id(sender_id), "some_token", nodes=""
        )
        if self.protocol:
            self.protocol.sendto(response, address)
        asyncio.ensure_future(self.on_get_peers(info_hash, address))

    def handle_announce_peer_query(self, trans_id, args, address):
        info_hash = args.get(b'info_hash')
        logging.debug("收到来自 %s 的 announce_peer 请求, infohash: %s", address, info_hash.hex() if info_hash else "N/A")
        if not info_hash or info_hash in self.seen_info_hashes:
            return

        sender_id = args[b'id']
        response = self.krpc.ping_response(trans_id, self._fake_node_id(sender_id))
        if self.protocol:
            self.protocol.sendto(response, address)

        ip = address[0]
        port = args.get(b'port') if args.get(b'implied_port', 1) == 0 else address[1]
        if not port: return

        asyncio.ensure_future(self.fetch_metadata(info_hash, (ip, port)))
        asyncio.ensure_future(self.on_announce_peer(info_hash, address, (ip, port)))

    async def on_get_peers(self, info_hash, addr): pass
    async def on_announce_peer(self, info_hash, addr, peer_addr): pass

    def handle_get_peers_response(self, info_hash, args, address):
        peers = args[b'values']
        logging.debug("为 info_hash %s 收到 %d 个 peer", info_hash.hex(), len(peers))
        for peer in peers:
            try:
                ip = socket.inet_ntoa(peer[:4])
                port = int.from_bytes(peer[4:], 'big')
                asyncio.ensure_future(self.fetch_metadata(info_hash, (ip, port)))
            except Exception:
                pass

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
            logging.error("处理或保存元数据时出错: %s", e, exc_info=True)

    def _fake_node_id(self, target_id=None):
        if target_id:
            return target_id[:-1] + self.node_id[-1:]
        return self.node_id

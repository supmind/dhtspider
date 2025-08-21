import pytest
import pytest_asyncio
import asyncio
import copy

from dhtspider.protocol import Node
from dhtspider.crawler import Crawler
from dhtspider.config import default_config
from dhtspider.krpc import KRPC
from dhtspider.storage import Storage
from dhtspider.utils import generate_node_id
from pybloom_live import BloomFilter
from unittest.mock import MagicMock, AsyncMock, patch

@pytest_asyncio.fixture
async def live_crawler_factory(tmp_path):
    """
    一个 pytest fixture 工厂，用于创建和启动一个功能完整的 Crawler 实例。
    它会监听一个真实的 UDP 端口。
    """
    crawlers = []
    transports = []

    async def _create_crawler(port):
        loop = asyncio.get_running_loop()
        config = copy.deepcopy(default_config)
        config["PORT"] = port
        # 每个爬虫使用自己的临时文件，避免冲突
        config["BLOOM_FILTER_FILE"] = str(tmp_path / f"test_{port}.bloom")

        # --- 组装组件 ---
        node_id = generate_node_id()
        storage = Storage(config=config)
        krpc = KRPC(node_id=node_id)
        # 在集成测试中，我们使用真实的布隆过滤器
        seen_info_hashes = BloomFilter(
            capacity=config["BLOOM_FILTER_CAPACITY"],
            error_rate=config["BLOOM_FILTER_ERROR_RATE"]
        )

        crawler = Crawler(
            config=config,
            node_id=node_id,
            storage=storage,
            krpc=krpc,
            seen_info_hashes=seen_info_hashes
        )

        transport, _ = await loop.create_datagram_endpoint(
            lambda: Node(crawler=crawler), local_addr=("127.0.0.1", port)
        )

        crawlers.append(crawler)
        transports.append(transport)

        return crawler

    yield _create_crawler

    # --- 清理 ---
    for crawler in crawlers:
        crawler.close()
    for transport in transports:
        transport.close()


@pytest.mark.asyncio
async def test_bootstrap_and_announce(live_crawler_factory):
    """
    测试一个节点能否成功地从另一个节点引导，并正确处理 announce_peer 请求。
    """
    # 1. 准备：创建两个实时运行的爬虫节点
    # 节点A作为引导节点
    crawler_a = await live_crawler_factory(port=16881)
    # 节点B作为新加入的节点
    crawler_b = await live_crawler_factory(port=16882)

    # 替换掉节点A的 handle_find_node_query 以便我们能感知到 bootstrap
    crawler_a.handle_find_node_query = MagicMock()

    # 2. 执行：让节点B从节点A引导
    # 修改节点B的引导列表，使其只包含节点A
    crawler_b.config["BOOTSTRAP_NODES"] = [("127.0.0.1", 16881)]
    await crawler_b.bootstrap()

    # 等待一小段时间让网络包有时间传输和处理
    await asyncio.sleep(0.1)

    # 3. 断言引导成功
    # 验证节点A是否收到了来自节点B的 find_node 查询
    assert crawler_a.handle_find_node_query.called, "引导节点未能收到 find_node 查询"

    # 4. 准备 announce_peer 测试
    # 替换掉节点B的 fetch_metadata，以便我们能验证它是否被调用
    # 这里必须用 AsyncMock，因为产品代码会用 `asyncio.ensure_future` 来调用它
    crawler_b.fetch_metadata = AsyncMock()

    # 5. 执行 announce_peer
    # 模拟一个第三方节点向节点B发送 announce_peer 请求
    trans_id = b't_announce'
    info_hash = generate_node_id() # 用随机ID作为info_hash
    sender_id = generate_node_id()
    address = ('127.0.0.1', 12345) # 模拟的第三方地址
    args = {
        b'info_hash': info_hash,
        b'id': sender_id,
            b'port': 54321,
            b'implied_port': 0
    }
    crawler_b.handle_announce_peer_query(trans_id, args, address)

    # 等待 ensure_future 中的任务被调度
    await asyncio.sleep(0.1)

    # 6. 断言 announce_peer 处理成功
    # 验证节点B是否触发了元数据抓取
    assert crawler_b.fetch_metadata.called, "收到 announce_peer 后未触发元数据抓取"
    crawler_b.fetch_metadata.assert_called_once_with(info_hash, (address[0], args[b'port']))

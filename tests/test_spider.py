import pytest
import asyncio
import os
from unittest.mock import patch, MagicMock, AsyncMock
from dhtspider.spider import Spider
from dhtspider.config import default_config

@pytest.mark.asyncio
async def test_spider_announce_peer_handling():
    """
    测试单体 Spider 类能否正确处理 announce_peer 查询并尝试获取元数据。
    这是一个集成式的单元测试，用于验证核心处理流程。
    """
    # 1. 准备
    loop = asyncio.get_event_loop()
    spider = Spider(config=default_config, loop=loop)

    # 模拟 transport 层，避免真实的网络IO
    mock_transport = MagicMock()
    spider.connection_made(mock_transport)

    # 模拟 fetch_metadata 协程，以检查它是否被调用
    spider.fetch_metadata = AsyncMock()

    # 2. 准备测试数据
    trans_id = b't1'
    info_hash = os.urandom(20)
    sender_id = os.urandom(20)
    address = ('127.0.0.1', 1234)
    args = {
        b'info_hash': info_hash,
        b'id': sender_id,
        b'port': 5678,
        b'implied_port': 0
    }
    query_message = {
        b't': trans_id,
        b'y': b'q',
        b'q': b'announce_peer',
        b'a': args
    }

    # 3. 执行
    # 直接调用查询处理器，模拟 datagram_received 的效果
    await spider.handle_query(query_message, address)
    await asyncio.sleep(0.01)  # 允许 ensure_future 中的任务被调度

    # 4. 断言
    # 验证响应是否已发送
    # (它会发送一个响应，和一个反向find_node查询)
    assert mock_transport.sendto.call_count == 2

    # 验证爬虫是否尝试获取元数据
    spider.fetch_metadata.assert_called_once_with(info_hash, (address[0], args[b'port']))

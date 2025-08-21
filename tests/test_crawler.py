# -*- coding: utf-8 -*-
import pytest
import os
import copy
from unittest.mock import MagicMock, patch
from dhtspider.crawler import Crawler
from dhtspider.config import default_config
from dhtspider.utils import generate_node_id
from pybloom_live import BloomFilter

@pytest.fixture
def crawler_components():
    """
    提供一个 Crawler 实例及其所有模拟依赖项的 pytest fixture。
    """
    config = copy.deepcopy(default_config)
    node_id = generate_node_id()
    storage = MagicMock()
    krpc = MagicMock()
    bloom_filter = MagicMock()

    crawler = Crawler(
        config=config,
        node_id=node_id,
        storage=storage,
        krpc=krpc,
        seen_info_hashes=bloom_filter
    )
    protocol = MagicMock()
    crawler.set_protocol(protocol)

    # 返回一个包含所有组件的元组，以便测试可以解包它们
    return crawler, config, storage, krpc, bloom_filter, protocol

class TestCrawler:
    @pytest.mark.asyncio
    async def test_handle_announce_peer_query_success(self, crawler_components):
        """
        测试在收到新的 info_hash 时，成功处理 announce_peer 查询的场景。
        """
        # 1. 准备
        crawler, _, _, krpc, bloom_filter, protocol = crawler_components
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
        bloom_filter.__contains__.return_value = False

        # 2. 执行
        # 使用 patch 来模拟异步方法，因为我们只关心它是否被正确调用
        with patch.object(crawler, 'fetch_metadata') as mock_fetch:
            crawler.handle_announce_peer_query(trans_id, args, address)

            # 3. 断言
            # 验证回复了 ping
            krpc.ping_response.assert_called_once()
            # 验证触发了元数据抓取
            mock_fetch.assert_called_once_with(info_hash, (address[0], args[b'port']))
            # 验证发起了反向查询
            krpc.find_node_query.assert_called_once()
            # 验证网络总共发送了两次（ping回复 + find_node查询）
            assert protocol.sendto.call_count == 2

    def test_bloom_filter_persistence(self, tmp_path):
        """
        测试 Crawler.close() 是否能正确地将布隆过滤器状态持久化。
        """
        # 1. 准备
        test_info_hash = os.urandom(20)
        # 使用 pytest 提供的 tmp_path fixture 来创建临时文件
        test_bloom_file = tmp_path / "test.bloom"

        config = copy.deepcopy(default_config)
        config['BLOOM_FILTER_FILE'] = str(test_bloom_file)

        node_id = generate_node_id()
        storage = MagicMock()
        krpc = MagicMock()
        bloom = BloomFilter(
            capacity=config["BLOOM_FILTER_CAPACITY"],
            error_rate=config["BLOOM_FILTER_ERROR_RATE"]
        )
        bloom.add(test_info_hash)

        crawler = Crawler(
            config=config,
            node_id=node_id,
            storage=storage,
            krpc=krpc,
            seen_info_hashes=bloom
        )

        # 2. 执行
        crawler.close()

        # 3. 断言
        assert os.path.exists(test_bloom_file)
        with open(test_bloom_file, 'rb') as f:
            reloaded_bloom = BloomFilter.fromfile(f)
        assert test_info_hash in reloaded_bloom

    @pytest.mark.asyncio
    async def test_handle_announce_peer_query_seen_before(self, crawler_components):
        """
        测试当 info_hash 已存在于布隆过滤器中时，不执行任何操作。
        """
        crawler, _, _, krpc, bloom_filter, protocol = crawler_components
        trans_id = b't1'
        info_hash = os.urandom(20)
        address = ('127.0.0.1', 1234)
        args = {b'info_hash': info_hash, b'id': os.urandom(20), b'port': 5678}

        # 模拟 info_hash 已存在
        bloom_filter.__contains__.return_value = True

        with patch.object(crawler, 'fetch_metadata') as mock_fetch:
            crawler.handle_announce_peer_query(trans_id, args, address)

            # 断言：不应该发送响应，也不应该抓取元数据
            # 断言：根据实现，如果 info_hash 已存在，函数会直接返回，不执行任何操作。
            krpc.ping_response.assert_not_called()
            protocol.sendto.assert_not_called()
            mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_announce_peer_query_no_port(self, crawler_components):
        """
        测试当请求中没有有效端口时，不触发元数据抓取。
        """
        crawler, _, _, krpc, bloom_filter, protocol = crawler_components
        trans_id = b't1'
        info_hash = os.urandom(20)
        address = ('127.0.0.1', 1234)
        # 为了真正地模拟无有效端口，必须设置 implied_port=0 且不提供 'port'
        args = {b'info_hash': info_hash, b'id': os.urandom(20), b'implied_port': 0}

        bloom_filter.__contains__.return_value = False

        with patch.object(crawler, 'fetch_metadata') as mock_fetch:
            crawler.handle_announce_peer_query(trans_id, args, address)

            # 虽然会回复 ping，但不应该触发抓取
            krpc.ping_response.assert_called_once()
            mock_fetch.assert_not_called()
            # 验证发起了反向查询
            krpc.find_node_query.assert_called_once()
            # 验证网络总共发送了两次（ping回复 + find_node查询）
            assert protocol.sendto.call_count == 2

    @pytest.mark.asyncio
    async def test_handle_announce_peer_query_implied_port(self, crawler_components):
        """
        测试当 implied_port 为 1 时，使用来源地址的端口。
        """
        crawler, _, _, krpc, bloom_filter, protocol = crawler_components
        trans_id = b't1'
        info_hash = os.urandom(20)
        address = ('127.0.0.1', 1234)
        args = {
            b'info_hash': info_hash,
            b'id': os.urandom(20),
            b'implied_port': 1 # port 参数会被忽略
        }

        bloom_filter.__contains__.return_value = False

        with patch.object(crawler, 'fetch_metadata') as mock_fetch:
            crawler.handle_announce_peer_query(trans_id, args, address)

            # 断言 fetch_metadata 使用了来源地址的端口 (address[1])
            mock_fetch.assert_called_once_with(info_hash, (address[0], address[1]))

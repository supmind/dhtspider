import pytest
import asyncio
import hashlib
import bencoding
from unittest.mock import Mock, patch, AsyncMock

from dhtspider.fetcher import MetadataFetcher

# Consistent test data
METADATA_DICT = {b'name': b'test.torrent', b'piece length': 262144, b'pieces': b'x'*20}
METADATA_BCODED = bencoding.bencode(METADATA_DICT)
TEST_INFO_HASH = hashlib.sha1(METADATA_BCODED).digest()
TEST_PEER_ID = b'test_peer_id' * 2
OUR_PEER_ID = b'our_peer_id' * 2

@pytest.fixture
def mock_reader():
    reader = AsyncMock(spec=asyncio.StreamReader)
    # No default side_effect. Each test must configure the mock completely.
    return reader

@pytest.fixture
def mock_writer():
    writer = AsyncMock(spec=asyncio.StreamWriter)
    return writer

@pytest.fixture
def mock_open_connection(mock_reader, mock_writer):
    """Fixture to mock asyncio.open_connection."""
    mock_open = AsyncMock(return_value=(mock_reader, mock_writer))
    with patch('asyncio.open_connection', new=mock_open) as mock:
        yield mock

@pytest.mark.asyncio
async def test_fetch_success(mock_open_connection, mock_reader, mock_writer):
    """
    Test a successful metadata fetch operation from start to finish.
    """
    on_metadata_callback = AsyncMock()
    fetcher = MetadataFetcher(TEST_INFO_HASH, ("1.2.3.4", 1234), on_metadata_callback, OUR_PEER_ID)

    # 1. Handshake
    handshake_response = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00' + TEST_INFO_HASH + TEST_PEER_ID

    # 2. Extended Handshake (from peer)
    peer_ut_metadata_id = 3
    extended_handshake_payload = {
        b'm': {b'ut_metadata': peer_ut_metadata_id},
        b'metadata_size': len(METADATA_BCODED)
    }
    bencoded_payload = bencoding.bencode(extended_handshake_payload)
    extended_handshake_msg = b'\x14\x00' + bencoded_payload
    extended_handshake_msg_with_len = len(extended_handshake_msg).to_bytes(4, 'big') + extended_handshake_msg

    # 3. Metadata pieces (from peer)
    piece_0_payload = {b'msg_type': 1, b'piece': 0}
    bencoded_piece_payload = bencoding.bencode(piece_0_payload)
    piece_msg = b'\x14' + peer_ut_metadata_id.to_bytes(1, 'big') + bencoded_piece_payload + METADATA_BCODED
    piece_msg_with_len = len(piece_msg).to_bytes(4, 'big') + piece_msg

    # Configure the mock reader to return these messages in sequence
    mock_reader.readexactly.side_effect = [
        # 1. Handshake
        handshake_response,
        # 2. Extended Handshake
        len(extended_handshake_msg).to_bytes(4, 'big'), # Length prefix
        extended_handshake_msg, # Message
        # 3. Metadata piece
        len(piece_msg).to_bytes(4, 'big'), # Length prefix
        piece_msg, # Message
        # 4. A timeout to gracefully exit the 'while True' loop
        asyncio.TimeoutError,
    ]

    await fetcher.fetch()

    # Verify handshake was sent
    mock_writer.write.assert_any_call(
        b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00' + TEST_INFO_HASH + OUR_PEER_ID
    )

    # Verify extended handshake was sent
    sent_data = b''.join(call.args[0] for call in mock_writer.write.call_args_list)
    assert b'\x14\x00' in sent_data # Extended handshake ID
    assert b'ut_metadata' in sent_data

    # Verify piece request was sent
    assert b'msg_type' in sent_data
    assert b'piece' in sent_data

    # Verify the final callback was called with the correct data
    on_metadata_callback.assert_awaited_once_with(TEST_INFO_HASH, METADATA_DICT)

@pytest.mark.asyncio
async def test_fetch_timeout_on_connect(mock_open_connection):
    """ Test timeout during the initial connection. """
    mock_open_connection.side_effect = asyncio.TimeoutError
    on_metadata_callback = AsyncMock()
    fetcher = MetadataFetcher(TEST_INFO_HASH, ("1.2.3.4", 1234), on_metadata_callback, OUR_PEER_ID)
    await fetcher.fetch()
    on_metadata_callback.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_timeout_on_read(mock_open_connection, mock_reader):
    """
    Test that the fetcher correctly handles a timeout when reading from a peer.
    """
    on_metadata_callback = AsyncMock()
    fetcher = MetadataFetcher(TEST_INFO_HASH, ("1.2.3.4", 1234), on_metadata_callback, OUR_PEER_ID)

    # Simulate a timeout during the handshake read
    mock_reader.readexactly.side_effect = asyncio.TimeoutError

    await fetcher.fetch()

    # The callback should not have been called
    on_metadata_callback.assert_not_called()

@pytest.mark.asyncio
async def test_fetch_invalid_info_hash_in_handshake(mock_open_connection, mock_reader):
    """
    Test that the fetcher aborts if the peer returns a wrong info_hash.
    """
    on_metadata_callback = AsyncMock()
    fetcher = MetadataFetcher(TEST_INFO_HASH, ("1.2.3.4", 1234), on_metadata_callback, OUR_PEER_ID)

    wrong_info_hash = b'x' * 20
    handshake_response = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00' + wrong_info_hash + TEST_PEER_ID
    mock_reader.readexactly.side_effect = [handshake_response]

    await fetcher.fetch()

    on_metadata_callback.assert_not_called()


# Integration test for bloom filter persistence
import os
from dhtspider.node import Node

@pytest.mark.asyncio
async def test_bloom_filter_prevents_refetch():
    """
    一个集成测试，验证布隆过滤器的持久化可以防止对已见过的 info_hash 重新发起抓取。
    注意：这是一个集成测试，因为它涉及到文件I/O和多个组件的交互。
    """
    test_info_hash = os.urandom(20)
    bloom_file = "test_seen_info_hashes.bloom"

    # 确保测试环境是干净的
    if os.path.exists(bloom_file):
        os.remove(bloom_file)

    try:
        # 1. 创建第一个节点，并传递测试布隆文件名
        node1 = Node(host="127.0.0.1", port=6883, bloom_filter_file=bloom_file)
        node1.seen_info_hashes.add(test_info_hash)
        # 关闭节点以保存布隆过滤器
        node1.close()

        # 2. 创建第二个节点，它应该会加载上面保存的过滤器
        node2 = Node(host="127.0.0.1", port=6884, bloom_filter_file=bloom_file)
        # 确认 info_hash 已存在
        assert test_info_hash in node2.seen_info_hashes

        # 3. 模拟 get_peers 查询，并监视 fetch_metadata 是否被调用
        with patch.object(node2, 'fetch_metadata', new_callable=AsyncMock) as mock_fetch:
            # 模拟一个 get_peers 查询
            # 在此场景下，我们不需要一个真实的 trans_id 或 address
            node2.handle_get_peers_query(b't1', {b'info_hash': test_info_hash}, ('127.0.0.1', 1234))

            # 等待一小段时间，以确保异步任务（如果有的话）有机会运行
            await asyncio.sleep(0.01)

            # 4. 验证 fetch_metadata 没有被调用，因为 info_hash 应该在布隆过滤器中
            mock_fetch.assert_not_called()

        node2.close()

    finally:
        # 清理测试文件
        if os.path.exists(bloom_file):
            os.remove(bloom_file)

import pytest
import bencoding
from unittest.mock import Mock
from dhtspider.krpc import KRPC

# 使用一个固定的、真实的 node_id 进行测试，以避免 bencoding 错误
TEST_NODE_ID = b'a' * 20

@pytest.fixture
def mock_handler():
    """提供一个模拟的处理器对象 (模仿 Crawler)。"""
    return Mock()

@pytest.fixture
def krpc(mock_handler):
    """提供一个为测试正确配置的 KRPC 实例。"""
    # 1. 使用真实的 node_id 初始化 KRPC
    instance = KRPC(node_id=TEST_NODE_ID)
    # 2. 为回调测试设置模拟处理器
    instance.set_handler(mock_handler)
    return instance

def test_ping_query(krpc):
    query = krpc.ping_query()
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'ping'
    assert decoded[b'a'][b'id'] == TEST_NODE_ID

def test_find_node_query(krpc):
    target_id = b'b' * 20
    query = krpc.find_node_query(target_id)
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'find_node'
    assert decoded[b'a'][b'target'] == target_id
    assert decoded[b'a'][b'id'] == TEST_NODE_ID

def test_get_peers_query(krpc):
    info_hash = b'c' * 20
    query = krpc.get_peers_query(info_hash)
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'get_peers'
    assert decoded[b'a'][b'info_hash'] == info_hash
    assert decoded[b'a'][b'id'] == TEST_NODE_ID
    trans_id = decoded[b't']
    assert trans_id in krpc.transactions
    assert krpc.transactions[trans_id]["info_hash"] == info_hash

def test_ping_response(krpc):
    trans_id = b't1'
    sender_id = b'd' * 20
    response = krpc.ping_response(trans_id, sender_id)
    decoded = bencoding.bdecode(response)
    assert decoded[b't'] == trans_id
    assert decoded[b'y'] == b'r'
    assert decoded[b'r'][b'id'] == sender_id

def test_handle_query_ping(krpc, mock_handler):
    address = ("1.2.3.4", 1234)
    msg = {
        b't': b't1',
        b'y': b'q',
        b'q': b'ping',
        b'a': {b'id': b'sender_id' * 2}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_handler.handle_ping_query.assert_called_once_with(b't1', msg[b'a'], address)

def test_handle_response_find_node(krpc, mock_handler):
    address = ("1.2.3.4", 1234)
    nodes_data = b'node1_data_node2_data'
    msg = {
        b't': b't2',
        b'y': b'r',
        b'r': {b'nodes': nodes_data}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_handler.handle_find_node_response.assert_called_once_with(b't2', msg[b'r'], address)

def test_handle_response_get_peers(krpc, mock_handler):
    address = ("1.2.3.4", 1234)
    info_hash = b'c' * 20
    trans_id = b't3'
    krpc.transactions[trans_id] = {"info_hash": info_hash}
    msg = {
        b't': trans_id,
        b'y': b'r',
        b'r': {b'values': [b'peer1', b'peer2']}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_handler.handle_get_peers_response.assert_called_once_with(info_hash, msg[b'r'], address)
    assert trans_id not in krpc.transactions

def test_handle_malformed_message(krpc, mock_handler):
    address = ("1.2.3.4", 1234)
    krpc.handle_message(b'not bencoded', address)
    krpc.handle_message(bencoding.bencode([1, 2, 3]), address)
    mock_handler.handle_ping_query.assert_not_called()
    mock_handler.handle_find_node_response.assert_not_called()
    mock_handler.handle_get_peers_response.assert_not_called()

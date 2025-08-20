import pytest
import bencoding
from unittest.mock import Mock, patch
from dhtspider.krpc import KRPC

# Helper to create a mock node
@pytest.fixture
def mock_node():
    node = Mock()
    node.node_id = b'a' * 20
    return node

# Helper for KRPC instance
@pytest.fixture
def krpc(mock_node):
    return KRPC(mock_node)

def test_ping_query(krpc):
    query = krpc.ping_query()
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'ping'
    assert decoded[b'a'][b'id'] == krpc.node.node_id

def test_find_node_query(krpc):
    target_id = b'b' * 20
    query = krpc.find_node_query(target_id)
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'find_node'
    assert decoded[b'a'][b'target'] == target_id

def test_get_peers_query(krpc):
    info_hash = b'c' * 20
    query = krpc.get_peers_query(info_hash)
    decoded = bencoding.bdecode(query)
    assert decoded[b'y'] == b'q'
    assert decoded[b'q'] == b'get_peers'
    assert decoded[b'a'][b'info_hash'] == info_hash
    # Check that the transaction was stored
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

def test_handle_query_ping(krpc, mock_node):
    address = ("1.2.3.4", 1234)
    msg = {
        b't': b't1',
        b'y': b'q',
        b'q': b'ping',
        b'a': {b'id': b'sender_id' * 2}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_node.handle_ping_query.assert_called_once_with(b't1', msg[b'a'], address)

def test_handle_response_find_node(krpc, mock_node):
    address = ("1.2.3.4", 1234)
    nodes_data = b'node1_data_node2_data'
    msg = {
        b't': b't2',
        b'y': b'r',
        b'r': {b'nodes': nodes_data}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_node.handle_find_node_response.assert_called_once_with(b't2', msg[b'r'], address)

def test_handle_response_get_peers(krpc, mock_node):
    address = ("1.2.3.4", 1234)
    info_hash = b'c' * 20
    trans_id = b't3'
    # Pre-register the transaction
    krpc.transactions[trans_id] = {"info_hash": info_hash}

    msg = {
        b't': trans_id,
        b'y': b'r',
        b'r': {b'values': [b'peer1', b'peer2']}
    }
    encoded_msg = bencoding.bencode(msg)
    krpc.handle_message(encoded_msg, address)
    mock_node.handle_get_peers_response.assert_called_once_with(info_hash, msg[b'r'], address)
    # Transaction should be deleted after handling
    assert trans_id not in krpc.transactions

def test_handle_malformed_message(krpc, mock_node):
    address = ("1.2.3.4", 1234)
    # Not bencoded data
    krpc.handle_message(b'not bencoded', address)
    # Bencoded, but not a dict
    krpc.handle_message(bencoding.bencode([1, 2, 3]), address)
    # No exceptions should be raised
    # And no handlers should be called
    mock_node.handle_ping_query.assert_not_called()
    mock_node.handle_find_node_response.assert_not_called()
    mock_node.handle_get_peers_response.assert_not_called()

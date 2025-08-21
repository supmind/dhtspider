import socket
import struct
from dhtspider.utils import generate_node_id, decode_nodes

def test_generate_node_id():
    """
    测试 generate_node_id 是否生成一个20字节的 bytes 对象。
    """
    node_id = generate_node_id()
    assert isinstance(node_id, bytes)
    assert len(node_id) == 20

def test_decode_nodes():
    """
    测试 decode_nodes 是否能正确解码紧凑的节点信息。
    """
    # 准备测试数据: 2个节点
    # 节点1
    node1_id = b'a' * 20
    node1_ip = '127.0.0.1'
    node1_port = 6881
    # 节点2
    node2_id = b'b' * 20
    node2_ip = '192.168.1.1'
    node2_port = 6882

    # 手动编码
    encoded_nodes = b''
    encoded_nodes += node1_id
    encoded_nodes += socket.inet_aton(node1_ip)
    encoded_nodes += struct.pack('!H', node1_port)
    encoded_nodes += node2_id
    encoded_nodes += socket.inet_aton(node2_ip)
    encoded_nodes += struct.pack('!H', node2_port)

    # 执行解码
    decoded = decode_nodes(encoded_nodes)

    # 断言
    assert len(decoded) == 2
    # 验证节点1
    assert decoded[0][0] == node1_id
    assert decoded[0][1] == node1_ip
    assert decoded[0][2] == node1_port
    # 验证节点2
    assert decoded[1][0] == node2_id
    assert decoded[1][1] == node2_ip
    assert decoded[1][2] == node2_port

def test_decode_nodes_empty():
    """
    测试 decode_nodes 在输入为空时返回空列表。
    """
    assert decode_nodes(b'') == []

def test_decode_nodes_malformed():
    """
    测试 decode_nodes 在输入数据长度不正确时的情况。
    它应该能处理这种情况而不会崩溃。
    """
    # 准备一个长度不是26倍数的字节串
    malformed_nodes = b'a' * 30
    # 期望它能处理完第一个完整的节点（如果可能），或者至少不抛出索引错误
    try:
        decode_nodes(malformed_nodes)
    except IndexError:
        pytest.fail("decode_nodes should handle malformed input gracefully")

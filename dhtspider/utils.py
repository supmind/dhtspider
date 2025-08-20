import os
import socket
import struct


def generate_node_id():
    """
    生成一个20字节的随机节点ID。
    """
    return os.urandom(20)


def decode_nodes(nodes):
    """
    解码 find_node 响应中的 "nodes" 字段。
    """
    n = []
    for i in range(0, len(nodes), 26):
        nid = nodes[i:i+20]
        ip = socket.inet_ntoa(nodes[i+20:i+24])
        port = struct.unpack('!H', nodes[i+24:i+26])[0]
        n.append((nid, ip, port))
    return n

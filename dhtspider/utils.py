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
        # 增加长度检查，确保有足够的字节来解析一个完整的节点信息
        if len(nodes) < i + 26:
            break
        try:
            nid = nodes[i:i+20]
            ip = socket.inet_ntoa(nodes[i+20:i+24])
            port = struct.unpack('!H', nodes[i+24:i+26])[0]
            n.append((nid, ip, port))
        except (OSError, struct.error):
            # 如果节点数据格式错误，则忽略该节点并继续
            continue
    return n

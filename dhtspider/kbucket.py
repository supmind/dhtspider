import time
from collections import OrderedDict

# DHT K-bucket 的大小
K = 8
# 节点ID的位数
ID_BITS = 160

class KBucket:
    """
    一个 K-Bucket，用于存储节点信息。
    使用 OrderedDict 来方便地实现LRU（最近最少使用）替换策略。
    """
    def __init__(self, min_id, max_id):
        self.min_id = min_id
        self.max_id = max_id
        self.nodes = OrderedDict()
        self.last_updated = time.time()

    def add_node(self, node):
        """
        向 bucket 中添加一个节点。
        node 是一个元组 (node_id, ip, port)。
        """
        node_id, _, _ = node
        if node_id in self.nodes:
            # 如果节点已存在，移到末尾表示最近见过
            self.nodes.move_to_end(node_id)
        elif len(self.nodes) < K:
            # 如果 bucket 未满，直接添加
            self.nodes[node_id] = node
        else:
            # 如果 bucket 已满，可以实现 ping 最老的节点来决定是否替换。
            # 在爬虫场景下，我们可以简单地忽略新节点，或直接替换最老的节点以保持信息新鲜。
            # 这里我们选择替换最老的节点（OrderedDict的第一个元素）。
            oldest_node_id = next(iter(self.nodes))
            self.nodes.pop(oldest_node_id)
            self.nodes[node_id] = node

        self.last_updated = time.time()

    def get_nodes(self):
        """
        获取 bucket 中的所有节点。
        """
        return list(self.nodes.values())

    def __len__(self):
        return len(self.nodes)


class RoutingTable:
    """
    Kademlia 路由表，管理所有的 K-Bucket。
    """
    def __init__(self, our_node_id):
        self.our_node_id = our_node_id
        self.buckets = [KBucket(0, 2**ID_BITS - 1)]

    def _get_bucket_index(self, node_id_int):
        """
        根据节点ID的整数形式，计算它应该在哪个 bucket 中。
        """
        for i, bucket in enumerate(self.buckets):
            if bucket.min_id <= node_id_int < bucket.max_id:
                return i
        return -1 # 理论上不应该发生

    def add_node(self, node):
        """
        向路由表中添加一个新节点。
        """
        node_id_bytes, _, _ = node
        if node_id_bytes == self.our_node_id:
            return

        node_id_int = int.from_bytes(node_id_bytes, 'big')
        index = self._get_bucket_index(node_id_int)
        bucket = self.buckets[index]
        bucket.add_node(node)

        # 检查是否需要分裂 bucket。只分裂包含我们自己节点的 bucket。
        our_id_int = int.from_bytes(self.our_node_id, 'big')
        if len(bucket) == K and our_id_int >= bucket.min_id and our_id_int < bucket.max_id:
            self._split_bucket(index)

    def _split_bucket(self, index):
        """
        分裂一个 bucket。
        """
        old_bucket = self.buckets[index]
        mid_point = old_bucket.min_id + (old_bucket.max_id - old_bucket.min_id) // 2

        # 创建新的 bucket
        new_bucket = KBucket(mid_point, old_bucket.max_id)
        # 调整旧 bucket 的范围
        old_bucket.max_id = mid_point

        # 插入新的 bucket
        self.buckets.insert(index + 1, new_bucket)

        # 重新分配旧 bucket 中的所有节点到新的两个 bucket 中
        all_nodes = old_bucket.get_nodes()
        old_bucket.nodes.clear()
        new_bucket.nodes.clear()

        for node in all_nodes:
            node_id_bytes, _, _ = node
            node_id_int = int.from_bytes(node_id_bytes, 'big')
            if node_id_int >= new_bucket.min_id:
                new_bucket.add_node(node)
            else:
                old_bucket.add_node(node)

    def get_closest_nodes(self, target_id, count=K):
        """
        找到离目标ID最近的 K 个节点。
        """
        if not isinstance(target_id, int):
            target_id_int = int.from_bytes(target_id, 'big')
        else:
            target_id_int = target_id

        nodes_with_distance = []
        for bucket in self.buckets:
            for node_id_bytes, ip, port in bucket.get_nodes():
                distance = target_id_int ^ int.from_bytes(node_id_bytes, 'big')
                nodes_with_distance.append(((node_id_bytes, ip, port), distance))

        # 按距离排序
        nodes_with_distance.sort(key=lambda x: x[1])

        # 返回最近的 K 个节点
        return [node for node, dist in nodes_with_distance[:count]]

    def get_all_nodes(self):
        """
        获取路由表中所有的节点，用于简单的节点发现。
        """
        all_nodes = []
        for bucket in self.buckets:
            all_nodes.extend(bucket.get_nodes())
        return all_nodes

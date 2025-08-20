# -*- coding: utf-8 -*-
import os
import asyncio
import unittest
from unittest.mock import patch
from dhtspider.node import Node

class TestNode(unittest.TestCase):
    def test_bloom_filter_persistence(self):
        """
        测试布隆过滤器的持久化是否正常。
        """
        test_info_hash = os.urandom(20)
        test_bloom_file = "test_node_seen_info_hashes.bloom"

        if os.path.exists(test_bloom_file):
            os.remove(test_bloom_file)

        # 使用 patch 来临时覆盖配置文件中的路径
        with patch('dhtspider.config.BLOOM_FILTER_FILE', test_bloom_file):
            # 1. 创建第一个节点实例并添加 info_hash
            node1 = Node(host="127.0.0.1", port=6881)
            node1.seen_info_hashes.add(test_info_hash)
            self.assertTrue(test_info_hash in node1.seen_info_hashes)

            # 2. 关闭节点，这将触发保存布隆过滤器
            node1.close()

            # 3. 创建第二个节点实例，它应该会加载已保存的布隆过滤器
            node2 = Node(host="127.0.0.1", port=6882)

            # 4. 验证 info_hash 是否存在于第二个节点的布隆过滤器中
            self.assertTrue(test_info_hash in node2.seen_info_hashes)

            # 5. 清理测试后创建的布隆过滤器文件
            node2.close()

        if os.path.exists(test_bloom_file):
            os.remove(test_bloom_file)

if __name__ == '__main__':
    unittest.main()

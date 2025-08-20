# -*- coding: utf-8 -*-

# 网络相关配置
HOST = "0.0.0.0"
PORT = 6881

# DHT 引导节点
BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
]

# 布隆过滤器相关配置
BLOOM_FILTER_CAPACITY = 100000000  # 预期存储一亿个 info_hash
BLOOM_FILTER_ERROR_RATE = 0.0001   # 万分之一的错误率
BLOOM_FILTER_FILE = "seen_info_hashes.bloom"

# 存储相关配置
STORAGE_DIR = "bt"

# 元数据抓取器配置
FETCHER_SEMAPHORE_LIMIT = 100  # 并发抓取限制

# K-Bucket 维护配置
BUCKET_REFRESH_INTERVAL = 600  # 10分钟，K-Bucket 刷新时间
FIND_NODES_INTERVAL = 60     # 1分钟，执行一次 find_new_nodes 的间隔

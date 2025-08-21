# -*- coding: utf-8 -*-

"""
默认配置设置
"""
default_config = {
    # 网络相关配置
    "HOST": "0.0.0.0",
    "PORT": 6881,

    # DHT 引导节点
    "BOOTSTRAP_NODES": [
        ("router.bittorrent.com", 6881),
        ("dht.transmissionbt.com", 6881),
        ("router.utorrent.com", 6881),
    ],

    # 布隆过滤器相关配置
    "BLOOM_FILTER_CAPACITY": 100000000,
    "BLOOM_FILTER_ERROR_RATE": 0.0001,
    "BLOOM_FILTER_FILE": "seen_info_hashes.bloom",

    # 存储相关配置
    "STORAGE_DIR": "bt",

    # 元数据抓取器配置
    "FETCHER_SEMAPHORE_LIMIT": 100,

    # 状态报告配置
    "STATUS_REPORT_INTERVAL": 30,
}

import asyncio
import sys
import logging

from dhtspider.protocol import Node
from dhtspider.crawler import Crawler
from dhtspider.config import default_config
from dhtspider.krpc import KRPC
from dhtspider.storage import Storage
from dhtspider.utils import generate_node_id
from pybloom_live import BloomFilter

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger('dhtspider.crawler').setLevel(logging.DEBUG)


# --- 事件循环配置 ---
if sys.platform != 'win32':
    try:
        import uvloop
        uvloop.install()
        logging.info("uvloop 已安装，事件循环性能已提升。")
    except ImportError:
        logging.warning("uvloop 未安装，将使用默认的 asyncio 事件循环。")
else:
    logging.info("在 Windows 平台上运行，使用默认的 asyncio 事件循环。")


def load_bloom_filter(config):
    """
    从文件加载布隆过滤器，如果文件不存在则创建一个新的。
    """
    bloom_file = config["BLOOM_FILTER_FILE"]
    try:
        with open(bloom_file, 'rb') as f:
            bloom = BloomFilter.fromfile(f)
        logging.info("成功从 %s 加载布隆过滤器。", bloom_file)
        return bloom
    except FileNotFoundError:
        logging.info("未找到布隆过滤器文件 %s，将创建一个新的。", bloom_file)
        return BloomFilter(
            capacity=config["BLOOM_FILTER_CAPACITY"],
            error_rate=config["BLOOM_FILTER_ERROR_RATE"]
        )


async def main():
    """
    主函数，负责组装和启动DHT爬虫。
    """
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    # 1. 创建所有独立的组件
    logging.info("正在初始化组件...")
    node_id = generate_node_id()
    storage = Storage(config=default_config)
    krpc = KRPC(node_id=node_id)
    seen_info_hashes = load_bloom_filter(config=default_config)

    # 2. 注入依赖，创建 Crawler (逻辑层)
    crawler = Crawler(
        config=default_config,
        node_id=node_id,
        storage=storage,
        krpc=krpc,
        seen_info_hashes=seen_info_hashes
    )

    # 3. 创建 Node (协议层), 并启动监听
    host = default_config["HOST"]
    port = default_config["PORT"]
    try:
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: Node(crawler=crawler), local_addr=(host, port)
        )
        logging.info("DHT 协议层正在监听 %s:%s", host, port)
    except OSError as e:
        logging.error("无法监听在 %s:%s - %s", host, port, e)
        return

    # 4. 启动 Crawler 的核心逻辑
    await crawler.start()

    try:
        await shutdown_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.info("收到停止信号...")
    finally:
        logging.info("正在关闭爬虫...")
        crawler.close()
        transport.close()
        logging.info("爬虫已关闭。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

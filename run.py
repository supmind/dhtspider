import asyncio
import sys
import logging
from dhtspider.node import Node
from dhtspider.config import HOST, PORT

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG, # 设置为 DEBUG 级别以查看更详细的日志
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 在非 Windows 平台上，安装 uvloop 作为默认的事件循环以提升性能
if sys.platform != 'win32':
    try:
        import uvloop
        uvloop.install()
        logging.info("uvloop 已安装，事件循环性能已提升。")
    except ImportError:
        logging.warning("uvloop 未安装，将使用默认的 asyncio 事件循环。")
else:
    logging.info("在 Windows 平台上运行，使用默认的 asyncio 事件循环。")

# 爬虫启动入口

async def main():
    """
    主函数，用于启动DHT节点。
    """
    shutdown_event = asyncio.Event()

    node = Node(host=HOST, port=PORT)
    await node.start()

    try:
        await shutdown_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        node.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("检测到 Ctrl+C，正在停止爬虫...")

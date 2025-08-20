import asyncio
import uvloop
from dhtspider.node import Node
from dhtspider.config import HOST, PORT

# 安装 uvloop 作为默认的事件循环，以提升性能
uvloop.install()

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
        print("\n爬虫已停止。")

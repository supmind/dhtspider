import asyncio
import sys
from dhtspider.node import Node
from dhtspider.config import HOST, PORT

# 在非 Windows 平台上，安装 uvloop 作为默认的事件循环以提升性能
if sys.platform != 'win32':
    import uvloop
    uvloop.install()
    print("uvloop 已安装，事件循环性能已提升。")
else:
    print("在 Windows 平台上运行，使用默认的 asyncio 事件循环。")

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

import asyncio
import logging
import signal
import sys

from dhtspider.spider import Spider
from dhtspider.config import default_config

# --- 日志配置 ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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


def main():
    """
    主函数，负责启动 Spider。
    """
    loop = asyncio.get_event_loop()

    spider = Spider(config=default_config, loop=loop)

    # 添加信号处理程序以优雅地停止爬虫
    for signame in ('SIGINT', 'SIGTERM'):
        try:
            loop.add_signal_handler(getattr(signal, signame), spider.stop)
        except NotImplementedError:
            pass  # Windows 不支持

    spider.start(port=default_config["PORT"])

    try:
        logging.info("Spider 已启动，按 Ctrl+C 停止。")
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Spider 正在关闭...")
        # 找到所有正在运行的任务并取消它们
        tasks = asyncio.all_tasks(loop=loop)
        for task in tasks:
            task.cancel()
        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
        loop.close()
        logging.info("Spider 已关闭。")

if __name__ == '__main__':
    main()

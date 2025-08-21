import asyncio
import logging

class Node(asyncio.DatagramProtocol):
    """
    一个纯粹的 DatagramProtocol 实现。
    它处理底层的UDP套接字操作，并将所有逻辑委托给 Crawler 实例。
    """
    def __init__(self, crawler):
        self.crawler = crawler
        self.transport = None

    def connection_made(self, transport):
        """
        当创建好 transport 时的回调。
        """
        self.transport = transport
        # 注入对 protocol 的引用到 crawler 中
        self.crawler.set_protocol(self)

    def datagram_received(self, data, addr):
        """
        当收到 UDP 数据报时的回调。
        将消息的处理委托给 crawler。
        """
        # 注意：这里的 self.crawler.krpc 依赖于 KRPC 在 crawler 中的实例化
        self.crawler.krpc.handle_message(data, addr)

    def error_received(self, exc):
        """
        当发生错误时的回调。
        """
        logging.warning("协议层收到错误: %s", exc)

    def connection_lost(self, exc):
        """
        当连接丢失时的回调。
        """
        logging.info("协议层连接已关闭。")

    def sendto(self, data, addr):
        """
        由 crawler 调用以发送网络数据。
        """
        if self.transport:
            self.transport.sendto(data, addr)

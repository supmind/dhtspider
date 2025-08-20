import asyncio

class Storage:
    """
    用于存储获取到的 torrent 信息。
    """
    def __init__(self, filename="torrents.txt"):
        self.filename = filename
        self.file = open(self.filename, "a", encoding="utf-8")
        self.lock = asyncio.Lock()

    async def save(self, info_hash, name):
        """
        保存 torrent 信息到文件。
        """
        async with self.lock:
            self.file.write(f"{info_hash.hex()}|{name}\n")
            self.file.flush()

    def close(self):
        """
        关闭文件。
        """
        self.file.close()

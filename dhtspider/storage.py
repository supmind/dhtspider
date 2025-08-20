import asyncio
import os
import bencoding

class Storage:
    """
    用于将获取到的元信息保存为 .torrent 文件。
    """
    def __init__(self, output_dir="bt"):
        self.output_dir = output_dir
        self.lock = asyncio.Lock()
        # 确保输出目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    async def save(self, info_hash, metadata):
        """
        将元信息保存为 .torrent 文件。
        文件名为 info_hash 的十六进制表示。
        """
        file_path = os.path.join(self.output_dir, f"{info_hash.hex()}.torrent")

        # 使用异步锁确保文件写入的原子性
        async with self.lock:
            try:
                # bencode 编码元数据
                encoded_metadata = bencoding.bencode(metadata)
                with open(file_path, "wb") as f:
                    f.write(encoded_metadata)
                print(f"成功保存种子文件: {file_path}")
            except Exception as e:
                print(f"保存种子文件时出错: {e}")

    def close(self):
        """
        这个类不再需要显式关闭。
        """
        pass

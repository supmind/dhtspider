import pytest
import os
import hashlib
import bencoding
import copy
from dhtspider.storage import Storage
from dhtspider.config import default_config

@pytest.mark.asyncio
async def test_storage_saves_torrent_file(tmp_path):
    """
    测试 Storage 类是否能将元数据正确保存为 .torrent 文件。
    """
    # 1. 设置测试环境
    test_config = copy.deepcopy(default_config)
    test_config["STORAGE_DIR"] = str(tmp_path)
    storage = Storage(config=test_config)

    # 2. 准备测试数据
    metadata = {
        b'name': b'test_torrent',
        b'piece length': 262144,
        b'pieces': b'a' * 20
    }
    # 根据元数据计算 info_hash
    info_hash = hashlib.sha1(bencoding.bencode(metadata)).digest()

    # 3. 调用被测试的方法
    await storage.save(info_hash, metadata)

    # 4. 验证结果
    # 验证文件是否已创建
    expected_file_path = tmp_path / f"{info_hash.hex()}.torrent"
    assert os.path.exists(expected_file_path)

    # 验证文件内容是否正确
    with open(expected_file_path, "rb") as f:
        file_content = f.read()

    expected_content = bencoding.bencode(metadata)
    assert file_content == expected_content

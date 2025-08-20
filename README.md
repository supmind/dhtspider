# DHT Spider

一个基于 Python `asyncio` 实现的 DHT 蜘蛛，用于在 DHT 网络中收集 `infohash` 并抓取元数据。

## 功能
- 异步网络模型，高性能
- 通过布隆过滤器去重，节省资源并支持持久化
- 可配置的参数
- 模块化设计，易于扩展

## 安装

1.  克隆本项目:
    ```bash
    git clone https://github.com/your-username/dhtspider.git
    cd dhtspider
    ```

2.  安装依赖:
    - **生产环境**:
      ```bash
      pip install -r requirements.txt
      ```
    - **开发环境** (包含测试工具):
      ```bash
      pip install -r requirements.txt -r requirements-dev.txt
      ```

## 运行

直接运行 `run.py` 即可启动爬虫:
```bash
python run.py
```

爬虫会将收集到的 `.torrent` 文件保存在 `bt/` 目录下。
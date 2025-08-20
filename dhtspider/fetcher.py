import asyncio
import hashlib
import bencoding


class MetadataFetcher:
    """
    通过 ut_metadata 扩展从 peer 获取 torrent 元数据。
    """
    def __init__(self, info_hash, peer_address, on_metadata_callback, our_peer_id):
        self.info_hash = info_hash
        self.peer_address = peer_address
        self.on_metadata_callback = on_metadata_callback
        self.our_peer_id = our_peer_id
        self.reader = None
        self.writer = None
        self.my_ut_metadata_id = 1

    async def fetch(self):
        """
        连接到 peer 并获取元数据。
        """
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.peer_address[0], self.peer_address[1]),
                timeout=5
            )
            await self._handshake()
            await self._extended_handshake_loop()
        except Exception:
            pass
        finally:
            if self.writer:
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass

    async def _handshake(self):
        """
        执行 BitTorrent 协议握手。
        """
        handshake_msg = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00' + self.info_hash + self.our_peer_id
        self.writer.write(handshake_msg)
        await self.writer.drain()
        response = await asyncio.wait_for(self.reader.readexactly(68), timeout=5)
        if response[28:48] != self.info_hash:
            raise Exception("握手响应中的 info_hash 无效")

    async def _extended_handshake_loop(self):
        """
        执行扩展握手并处理来自 peer 的消息。
        """
        extended_handshake_payload = bencoding.bencode({b"m": {b"ut_metadata": self.my_ut_metadata_id}})
        msg = b'\x14\x00' + extended_handshake_payload
        msg_len = len(msg).to_bytes(4, 'big')
        self.writer.write(msg_len + msg)
        await self.writer.drain()

        peer_ut_metadata_id = None
        metadata_size = None
        metadata_pieces = []

        while True:
            len_prefix = await asyncio.wait_for(self.reader.readexactly(4), timeout=10)
            msg_len = int.from_bytes(len_prefix, 'big')
            if msg_len == 0:
                continue

            message = await asyncio.wait_for(self.reader.readexactly(msg_len), timeout=10)
            msg_id = message[0]

            if msg_id == 20:
                extended_msg_id = message[1]
                payload = message[2:]

                if extended_msg_id == 0:
                    decoded_payload = bencoding.bdecode(payload)
                    peer_ut_metadata_id = decoded_payload.get(b'm', {}).get(b'ut_metadata')
                    metadata_size = decoded_payload.get(b'metadata_size')
                    if peer_ut_metadata_id and metadata_size:
                        num_pieces = (metadata_size + 16383) // 16384
                        metadata_pieces = [None] * num_pieces
                        for i in range(num_pieces):
                            await self._request_metadata_piece(peer_ut_metadata_id, i)

                elif peer_ut_metadata_id and extended_msg_id == peer_ut_metadata_id:
                    try:
                        bencoded_end = payload.find(b'ee') + 2
                        metadata_dict = bencoding.bdecode(payload[:bencoded_end])
                        piece_index = metadata_dict[b'piece']
                        piece_data = payload[bencoded_end:]

                        if piece_index < len(metadata_pieces):
                            metadata_pieces[piece_index] = piece_data

                        if all(p is not None for p in metadata_pieces):
                            full_metadata = b''.join(metadata_pieces)
                            if hashlib.sha1(full_metadata).digest() == self.info_hash:
                                parsed_metadata = bencoding.bdecode(full_metadata)
                                await self.on_metadata_callback(self.info_hash, parsed_metadata)
                            return
                    except Exception:
                        return

    async def _request_metadata_piece(self, peer_ut_metadata_id, piece_index):
        """
        请求一个元数据片段。
        """
        request = {b'msg_type': 0, b'piece': piece_index}
        encoded_request = bencoding.bencode(request)

        msg = b'\x14' + peer_ut_metadata_id.to_bytes(1, 'big') + encoded_request
        msg_len = len(msg).to_bytes(4, 'big')

        self.writer.write(msg_len + msg)
        await self.writer.drain()

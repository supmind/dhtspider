import pytest
from dhtspider.storage import Storage

@pytest.mark.asyncio
async def test_storage_save_and_close(tmp_path):
    """
    Tests that the Storage class can save data to a file and close it properly.
    """
    # Use a temporary file for the test
    test_file = tmp_path / "torrents.txt"

    storage = Storage(filename=str(test_file))

    # Sample data
    info_hash = b'\x12\x34\x56\x78\x90\xab\xcd\xef' * 2 + b'\x12\x34\x56\x78'
    name = "My Test Torrent"

    # Save the data
    await storage.save(info_hash, name)

    # Close the storage, which should flush the data to the file
    storage.close()

    # Read the file and verify its contents
    with open(test_file, "r", encoding="utf-8") as f:
        line = f.readline().strip()

    expected_line = f"{info_hash.hex()}|{name}"
    assert line == expected_line

@pytest.mark.asyncio
async def test_storage_multiple_saves(tmp_path):
    """
    Tests that multiple saves are written correctly.
    """
    test_file = tmp_path / "torrents.txt"
    storage = Storage(filename=str(test_file))

    info_hash1 = b'a' * 20
    name1 = "Torrent 1"
    await storage.save(info_hash1, name1)

    info_hash2 = b'b' * 20
    name2 = "Torrent 2"
    await storage.save(info_hash2, name2)

    storage.close()

    with open(test_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 2
    assert lines[0].strip() == f"{info_hash1.hex()}|{name1}"
    assert lines[1].strip() == f"{info_hash2.hex()}|{name2}"

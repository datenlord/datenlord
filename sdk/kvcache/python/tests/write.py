import asyncio
from datenlordsdk import DatenLordSDK

async def test_write():
    # Initialize the SDK with appropriate configuration
    sdk = DatenLordSDK(
        block_size=1024,
        kv_engine_address=["127.0.0.1:2379"],
        log_level="debug"
    )
    print("SDK initialized successfully")

    # Create a bytes object with size 768
    data_to_write = bytes(768)

    # Test Case: Write Operation
    # Write keys k1, k2, k3 and verify that they exist
    keys = ["k1", "k2", "k3"]
    for key in keys:
        await sdk.insert(key, data_to_write)
        matched_key, data = await sdk.try_load(key)
        assert matched_key == key, f"Key mismatch: expected {key}, got {matched_key}"
        assert len(data) == 768, f"Data size mismatch: expected 768, got {len(data)}"
    print("Write operation test passed")

if __name__ == "__main__":
    asyncio.run(test_write())

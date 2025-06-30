import asyncio
from datenlordsdk import DatenLordSDK


def convert_string_to_list_of_integers(string):
    return [ord(char) for char in string]


async def test_write():
    # Initialize the SDK with appropriate configuration
    sdk = DatenLordSDK(
        block_size=1024, kv_engine_address=["127.0.0.1:2379"], log_level="debug"
    )
    print("SDK initialized successfully")

    # Create a bytes object with size 768
    data_to_write = bytes(768)

    # Test Case: Write Operation
    # Write keys k1, k2, k3 and verify that they exist
    keys = ["k1", "k2", "k3"]
    for key in keys:
        key = convert_string_to_list_of_integers(key)
        await sdk.insert(key, data_to_write)
        matched_key, data = await sdk.try_load(key)
        assert matched_key == key, f"Key mismatch: expected {key}, got {matched_key}"
        assert (
            data.get_len() == 768
        ), f"Data size mismatch: expected 768, got {data.get_len()}"
    print("Write operation test passed")


if __name__ == "__main__":
    asyncio.run(test_write())

import asyncio
from datenlordsdk import DatenLordSDK


def convert_string_to_list_of_integers(string):
    return [ord(char) for char in string]


async def test_read():
    # Initialize the SDK with appropriate configuration
    sdk = DatenLordSDK(
        block_size=1024, kv_engine_address=["127.0.0.1:2379"], log_level="debug"
    )
    print("SDK initialized successfully")

    # Create a bytes object with size 768
    data_to_write1 = bytes([1] * 768)
    data_to_write2 = bytes([2] * 768)
    data_to_write3 = bytes([3] * 768)

    # Write keys k1, k22, k333 for testing read operation
    await sdk.insert(convert_string_to_list_of_integers("k1"), data_to_write1)
    await sdk.insert(convert_string_to_list_of_integers("k22"), data_to_write2)
    await sdk.insert(convert_string_to_list_of_integers("k333"), data_to_write3)

    # Test Case: Read Operation
    # Read a key "k" that doesn't exist
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k"))
    assert matched_key == [], f"Expected no match for key 'k', but got {matched_key}"

    # Read exact match for "k1"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k1"))
    assert matched_key == convert_string_to_list_of_integers(
        "k1"
    ), f"Expected match for key 'k1', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k1': expected 768, got {data.get_len()}"
    assert (
        memoryview(data).tobytes() == data_to_write1
    ), f"Data mismatch for key 'k1'"

    # Read partial match for "k111"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k111"))
    assert matched_key == convert_string_to_list_of_integers(
        "k1"
    ), f"Expected partial match for key 'k111' to match 'k1', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k111': expected 768, got {data.get_len()}"
    assert (
        memoryview(data).tobytes() == data_to_write1
    ), f"Data mismatch for key 'k1'"

    # Read exact match for "k22"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k22"))
    assert matched_key == convert_string_to_list_of_integers(
        "k22"
    ), f"Expected match for key 'k22', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k22': expected 768, got {data.get_len()}"
    assert (
        memoryview(data).tobytes() == data_to_write2
    ), f"Data mismatch for key 'k22'"

    # Read exact match for "k333"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k333"))
    assert matched_key == convert_string_to_list_of_integers(
        "k333"
    ), f"Expected match for key 'k333', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k333': expected 768, got {data.get_len()}"
    assert (
        memoryview(data).tobytes() == data_to_write3
    ), f"Data mismatch for key 'k333'"

    # Read a key "k4" that doesn't exist
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k4"))
    assert matched_key == [], f"Expected no match for key 'k4', but got {matched_key}"

    print("Read operation test passed")


if __name__ == "__main__":
    asyncio.run(test_read())

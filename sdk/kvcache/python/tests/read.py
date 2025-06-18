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
    data_to_write = bytes(768)

    # Write keys k1, k2, k3 for testing read operation
    await sdk.insert(convert_string_to_list_of_integers("k1"), data_to_write)
    await sdk.insert(convert_string_to_list_of_integers("k2"), data_to_write)
    await sdk.insert(convert_string_to_list_of_integers("k3"), data_to_write)

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

    # Read partial match for "k111"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k111"))
    assert matched_key == convert_string_to_list_of_integers(
        "k1"
    ), f"Expected partial match for key 'k111' to match 'k1', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k111': expected 768, got {data.get_len()}"

    # Read exact match for "k2"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k2"))
    assert matched_key == convert_string_to_list_of_integers(
        "k2"
    ), f"Expected match for key 'k2', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k2': expected 768, got {data.get_len()}"

    # Read exact match for "k3"
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k3"))
    assert matched_key == convert_string_to_list_of_integers(
        "k3"
    ), f"Expected match for key 'k3', but got {matched_key}"
    assert (
        data.get_len() == 768
    ), f"Data size mismatch for key 'k3': expected 768, got {data.get_len()}"

    # Read a key "k4" that doesn't exist
    matched_key, data = await sdk.try_load(convert_string_to_list_of_integers("k4"))
    assert matched_key == [], f"Expected no match for key 'k4', but got {matched_key}"

    print("Read operation test passed")


if __name__ == "__main__":
    asyncio.run(test_read())

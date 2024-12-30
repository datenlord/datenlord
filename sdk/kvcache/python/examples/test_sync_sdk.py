import asyncio
from datenlordsdk import DatenLordSDK


async def main():
    sdk = DatenLordSDK(
        block_size=1024,
        kv_engine_address=["127.0.0.1:2379"],
        log_level="debug"
    )
    print("SDK initialized successfully")

    # Create a bytes object
    kvcache = bytes(768)
    # Create a bytes object
    for i in range(10):
        key = f"sdk{i}"
        # convert the string to a list of integers
        key = [ord(char) for char in key]
        print(f"key: {key}")
        sdk.insert_sync(key, kvcache)

        res = sdk.match_prefix_sync(key)
        print(f"matched res: {res}")

        matched_key, data = sdk.try_load_sync(key)
        # Make sure the key is matched, not partial matched
        assert matched_key == key
        print(f"res: {matched_key}, datasize: {data.get_len()}")


if __name__  == "__main__":
    asyncio.run(main())
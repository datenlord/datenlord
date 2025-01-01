import asyncio
from datenlordsdk import DatenLordSDK


async def main():
    sdk = DatenLordSDK(
        # block_size=299167417,
        block_size=126624,
        kv_engine_address=["127.0.0.1:2379"],
        log_level="debug"
    )
    print("SDK initialized successfully")

    # Create a bytes object
    import io
    kvcache = io.BytesIO()
    kvcache.write(b"Hello World")
    kvcache.write(bytes(299167416))
    kvcache = kvcache.getbuffer()
    kvcache = kvcache[:299167416].tobytes()
    print(f"cache size: {len(kvcache)} type: {type(kvcache)}")

    # Create a bytes object
    for i in range(10):
        key = f"sdk{i}"
        # convert the string to a list of integers
        key = [ord(char) for char in key]
        print(f"key: {key}")
        sdk.insert_sync(key, kvcache)

        # res = sdk.match_prefix_sync(key)
        # print(f"matched res: {res}")

        matched_key, data = sdk.try_load_sync(key)
        # Make sure the key is matched, not partial matched
        # assert matched_key == key
        # print(f"res: {matched_key}, datasize: {data.get_len()}")
        buf = memoryview(data).tobytes()
        # print(f"buf: {str(buf)}")
        assert buf == kvcache
        # del data
        # del buf


if __name__  == "__main__":
    asyncio.run(main())
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
    # await sdk.insert("key2", kvcache)
    await sdk.insert("key111", kvcache)
    # await sdk.insert("key4", kvcache)
    # await sdk.insert("key2", kvcache)
    res = await sdk.try_load("key111")
    print(res)
    # res = await sdk.try_load("key2")
    print(res)

if __name__  == "__main__":
    asyncio.run(main())
import asyncio
from datenlordsdk import DatenLordSDK


async def main():
    sdk = DatenLordSDK("config.toml")
    print("SDK initialized successfully")

    # Create a bytes object
    kvcache = bytes(768)
    await sdk.insert("key1", kvcache)
    # await sdk.insert("key2", kvcache)
    res = await sdk.try_load("key2")
    print(res)

if __name__  == "__main__":
    asyncio.run(main())
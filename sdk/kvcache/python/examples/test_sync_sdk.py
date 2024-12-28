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
    for i in range(10):
        key = f"sdk{i}"
        # convert the string to a list of integers
        key = [ord(char) for char in key]
        print(f"key: {key}")
        res = sdk.match_prefix_sync(key)
        print(f"res: {res}")

if __name__  == "__main__":
    asyncio.run(main())
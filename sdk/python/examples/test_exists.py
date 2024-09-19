import sys
import asyncio
from datenlordsdk import DatenLordSDK

# clear the command line arguments
sys.argv = [sys.argv[0]]

async def test_exists(sdk, path):
    exists = await sdk.exists(path)
    print(f"Path '{path}' exists: {exists}")

async def main():
    sdk = DatenLordSDK("config.toml")
    print("SDK initialized successfully")

    # Check if directory exists
    test_dir = "datenlord_sdk"
    await test_exists(sdk, test_dir)

    # await sdk.deldir(test_dir, recursive=True)

    try:
        # res = await sdk.mkdir(test_dir)
        print("Directory created successfully", res)
    except Exception as e:
        handle_error(e)

    await test_exists(sdk, test_dir)

if __name__ == "__main__":
    asyncio.run(main())

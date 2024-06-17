# Uploads the .mp4 file to be available to the Function app.
# Client code that makes the HTTP post call.
import aiohttp
import asyncio

async def stream_generator(file_path):
    chunk_size = 4 * 1024 * 1024
    with open(file_path, 'rb') as file:
        while chunk := file.read(chunk_size):
            print(f"Uploading {len(chunk)} bytes")
            yield chunk

async def stream_to_server(url, file_path):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60000)) as session:
        async with session.post(url, data=stream_generator(file_path)) as resp:
            print(await resp.text())


async def main():
    # URL and file path
    url = 'http://localhost:7071/api/stream_upload'
    file_path = r'painting.mp4'
    # Stream the file to the server
    await stream_to_server(url, file_path)
    print('File uploaded successfully!')

if __name__ == "__main__":
    asyncio.run(main())
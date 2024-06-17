import logging
import math
from azurefunctions.extensions.http.fastapi import Request, StreamingResponse
import azurefunctions.extensions.bindings.blob as blob
import azure.functions as func

from fastapi import status, HTTPException

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

        
# SDK type binding for Blob
# Creates the client (without the SDK itself)
@app.route(route="stream_upload")
@app.blob_input(
    arg_name="client", path="streamingtest/painting.mp4", connection="DeferredBindingConnectionString"
) 
async def stream_upload(req: Request, client: blob.AioBlobClient) -> str:
    async with client: 
        await client.create_append_blob()
        await stream_upload(client, req) 

        return "Uploaded to blob"    

# Retrieve .mp4 file from blob
# Stream the file locally
@app.route(route="stream_download")
@app.blob_input(
    arg_name="client", path="streamingtest/painting.mp4", connection="DeferredBindingConnectionString"
) # Download file from Blob
async def stream_download(req: Request, client: blob.AioBlobClient) -> StreamingResponse:
    logging.info("Downloading video from blob")
    resp = await range_requests_response(req, client, content_type="video/mp4")
    return resp # Use HTTP streaming

async def stream_upload(blob_client, request):
    try:
        chunk_size = 2 * 1024 * 1024  # 4MB
        accumulated_chunk = b''
        accumulated_chunk_size = 0
        
        async for chunk in request.stream():
            if not chunk or len(chunk) == 0:
                break
                
            accumulated_chunk += chunk
            accumulated_chunk_size += len(chunk)
            
            if accumulated_chunk_size >= chunk_size:
                await blob_client.append_block(accumulated_chunk, accumulated_chunk_size)
                accumulated_chunk = b''
                accumulated_chunk_size = 0

        # Append the last remaining chunk
        if accumulated_chunk_size > 0:
            await blob_client.append_block(accumulated_chunk, accumulated_chunk_size)
                
        logging.info("Upload successful")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return "An error occurred during upload", 500

    

def _get_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    def _invalid_range():
        return HTTPException(
            status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail=f"Invalid request range (Range:{range_header!r})",
        )

    try:
        h = range_header.replace("bytes=", "").split("-")
        start = int(h[0]) if h[0] != "" else 0
        end = int(h[1]) if h[1] != "" else file_size - 1
    except ValueError:
        raise _invalid_range()

    if start > end or start < 0 or end > file_size - 1:
        raise _invalid_range()
    return start, end


async def range_requests_response(
    request: Request, blob_client, content_type: str
):
    """Returns StreamingResponse using Range Requests of a given file"""
    logging.info(f"Request headers {request.headers}")
    blob_props = (await blob_client.get_blob_properties())
    logging.info(f"Blob properties: {blob_props}")
    blob_size = blob_props.size
    range_header = request.headers.get("range")

    headers = {
        "content-type": content_type,
        "accept-ranges": "bytes",
        "content-encoding": "identity",
        "content-length": str(blob_size),
        "access-control-expose-headers": (
            "content-type, accept-ranges, content-length, "
            "content-range, content-encoding"
        ),
    }
    start = 0
    end = blob_size - 1
    status_code = status.HTTP_200_OK
    if range_header is not None:
        start, end = _get_range_header(range_header, blob_size)
        size = end - start + 1
        headers["content-length"] = str(size)
        headers["content-range"] = f"bytes {start}-{end}/{blob_size}"
        status_code = status.HTTP_206_PARTIAL_CONTENT

    async def chunk_generator(start, end):
        blob_chunk = await blob_client.download_blob(offset=start, length=end - start + 1, max_concurrency=4)
        async for chunk in blob_chunk.chunks():
            logging.info(f"Sent Chunk size: {len(chunk)}")
            yield chunk

    return StreamingResponse(
        chunk_generator(start, end),
        headers=headers,
        status_code=status_code,
    )
from __future__ import annotations

import asyncio
from uuid import uuid4

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import HTMLResponse

import crawlee

from .crawler import lifespan

app = FastAPI(lifespan=lifespan, title='Crawler app')


@app.get('/', response_class=HTMLResponse)
def index() -> str:
    return """
<!DOCTYPE html>
<html>
<body>
    <h1>Scraper server</h1>
        <p>To scrape some page, visit "scrape" endpoint with url parameter.
            For example:
            <a href="/scrape?url=https://www.example.com">
                /scrape?url=https://www.example.com
            </a>
        </p>
</body>
</html>
"""


@app.get('/scrape')
async def scrape_url(request: Request, url: str | None = None) -> dict:
    if not url:
        return {'url': 'missing', 'scrape result': 'no results'}

    # Generate random unique key for the request
    unique_key = str(uuid4())

    # Set the result future in the result dictionary so that it can be awaited
    request.state.requests_to_results[unique_key] = asyncio.Future[dict[str, str]]()

    # Add the request to the crawler queue
    await request.state.crawler.add_requests(
        [crawlee.Request.from_url(url, unique_key=unique_key)]
    )

    # Wait for the result future to be finished
    result = await request.state.requests_to_results[unique_key]

    # Clean the result from the result dictionary to free up memory
    request.state.requests_to_results.pop(unique_key)

    # Return the result
    return {'url': url, 'scrape result': result}

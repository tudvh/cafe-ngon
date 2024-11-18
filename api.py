from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List
import logging
import httpx
import json
import os
import random

from crawler import InstagramCrawler


class MediaData(BaseModel):
    user_id: str
    user_name: str
    resource_id: str
    resource_url: str
    created_at: datetime = Field(default_factory=datetime.now)


class Config:
    def __init__(self):
        self.is_crawl = os.getenv('IS_CRAWL').lower() == 'true'
        self.app_env = os.getenv('APP_ENV')


class DataManager:
    def __init__(self, filename: str = 'data/media_data.json'):
        self.filename = filename
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    async def load_data(self) -> List[MediaData]:
        if not os.path.exists(self.filename):
            return []
        try:
            with open(self.filename, 'r') as f:
                return [MediaData(**item) for item in json.load(f)]
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return []


class ImageService:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)

    async def fetch_image(self, url: str) -> StreamingResponse:
        try:
            response = await self.client.get(url, follow_redirects=True)
            response.raise_for_status()
            return StreamingResponse(
                response.iter_bytes(),
                media_type=response.headers.get("content-type", "image/jpeg"),
                headers={"Cache-Control": "public, max-age=3600"}
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def close(self):
        await self.client.aclose()


class APIServer:
    def __init__(self):
        self.config = Config()
        self.data_manager = DataManager()
        self.image_service = ImageService()
        self.media_cache = []
        self.app = self._create_app()

    def _create_app(self):
        app = FastAPI(lifespan=self._lifespan)
        app.add_middleware(CORSMiddleware, allow_origins=["*"])
        self._add_routes(app)
        return app

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        try:
            if self.config.is_crawl:
                crawler = InstagramCrawler()
                crawler.run()
            yield
        finally:
            await self.image_service.close()

    def _add_routes(self, app):
        @app.get("/api/cafe-ngon")
        async def get_random_media():
            if not self.media_cache:
                self.media_cache = await self.data_manager.load_data()
            if not self.media_cache:
                raise HTTPException(404, "No media")
            media = random.choice(self.media_cache)
            return {"data": media.resource_id}

        @app.get("/api/cafe-ngon/{media_id}")
        async def get_media(media_id: str):
            media = next(
                (m for m in self.media_cache if m.resource_id == media_id), None)
            if not media:
                raise HTTPException(404, "Not found")
            self.media_cache = [
                m for m in self.media_cache if m.resource_id != media_id]
            return await self.image_service.fetch_image(media.resource_url)

        @app.get("/api/media/stats")
        async def get_stats():
            data = await self.data_manager.load_data()
            users = {m.user_name for m in data}
            return {
                "total_images": len(data),
                "unique_users": len(users),
                "users": list(users)
            }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = APIServer()
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=os.getenv("APP_ENV") == "development"
    )

app = APIServer().app

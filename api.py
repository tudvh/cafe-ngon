from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from datetime import datetime
from typing import List
import logging
import httpx
import os
import random
import uvicorn
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv


class MediaData(BaseModel):
    id: str
    user_id: str
    user_name: str
    resource_id: str
    resource_url: str
    resource_type: int
    created_at: datetime


class Config:
    def __init__(self):
        load_dotenv()
        self._validate_env()

    def _validate_env(self):
        required = ['MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER',
                    'MYSQL_PASSWORD', 'MYSQL_DATABASE']
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(
                f"Missing environment variables: {', '.join(missing)}")

    @property
    def db_config(self) -> dict:
        return {
            'host': os.getenv('MYSQL_HOST'),
            'port': os.getenv('MYSQL_PORT'),
            'user': os.getenv('MYSQL_USER'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': os.getenv('MYSQL_DATABASE')
        }


class DataManager:
    def __init__(self, config: Config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config.db_config)
        except Error as e:
            logging.error(f"Database connection failed: {e}")
            raise

    async def load_data(self) -> List[MediaData]:
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect()

            cursor = self.conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM media_data")
            rows = cursor.fetchall()
            cursor.close()

            return [MediaData(**row) for row in rows]
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return []

    def remove_media(self, resource_id: str):
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect()

            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM media_data WHERE resource_id = %s",
                (resource_id,)
            )
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logging.error(f"Error removing media: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()


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
        self.data_manager = DataManager(self.config)
        self.image_service = ImageService()
        self.media_cache = []
        self.app = self._create_app()

    def _create_app(self):
        app = FastAPI(lifespan=self._lifespan)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self._add_routes(app)
        return app

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        try:
            yield
        finally:
            await self.image_service.close()
            self.data_manager.close()

    def _add_routes(self, app):
        @app.get("/api/images/random")
        async def get_random_media():
            if not self.media_cache:
                self.media_cache = await self.data_manager.load_data()
                random.shuffle(self.media_cache)
            if not self.media_cache:
                raise HTTPException(404, "No media available")
            media = random.choice(self.media_cache)
            return {"data": media.resource_id}

        @app.get("/api/images/{image_id}")
        async def get_media(image_id: str):
            if not self.media_cache:
                self.media_cache = await self.data_manager.load_data()
                random.shuffle(self.media_cache)

            media = next(
                (m for m in self.media_cache if m.resource_id == image_id),
                None
            )
            if not media:
                raise HTTPException(404, "Media not found")

            self.media_cache = [
                m for m in self.media_cache if m.resource_id != image_id
            ]

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

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=os.getenv("APP_ENV") == "development"
    )

app = APIServer().app

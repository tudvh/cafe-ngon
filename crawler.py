from instagrapi import Client
from dotenv import load_dotenv
from typing import List, Optional
from dataclasses import dataclass
import os
import logging
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import uuid


@dataclass(frozen=True)
class MediaItem:
    id: str
    user_id: str
    user_name: str
    resource_id: str
    resource_url: str
    resource_type: int


class Config:
    def __init__(self):
        load_dotenv()
        self._validate_env()

    def _validate_env(self):
        required = ['INSTAGRAM_LOGIN_USERNAME', 'INSTAGRAM_LOGIN_PASSWORD', 'INSTAGRAM_USERNAMES',
                    'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(
                f"Missing environment variables: {', '.join(missing)}")

    @property
    def username(self) -> str:
        return os.getenv('INSTAGRAM_LOGIN_USERNAME')

    @property
    def password(self) -> str:
        return os.getenv('INSTAGRAM_LOGIN_PASSWORD')

    @property
    def target_usernames(self) -> List[str]:
        return os.getenv('INSTAGRAM_USERNAMES', '').split(',')

    @property
    def new_user_post_limit(self) -> int:
        return int(os.getenv('NEW_USER_POST_LIMIT', '20'))

    @property
    def existing_user_post_limit(self) -> int:
        return int(os.getenv('EXISTING_USER_POST_LIMIT', '5'))

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
        self.setup_database()

    def setup_database(self):
        try:
            self.conn = mysql.connector.connect(**self.config.db_config)
        except Error as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def get_processed_users(self) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT user_name FROM processed_users")
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return result

    def get_existing_resource_ids(self) -> set:
        cursor = self.conn.cursor()
        cursor.execute("SELECT resource_id FROM media_data")
        result = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return result

    def save_media_items(self, items: List[MediaItem]):
        cursor = self.conn.cursor()
        query = """
            INSERT IGNORE INTO media_data (id, user_id, user_name, resource_id, resource_url, resource_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = [(item.id, item.user_id, item.user_name, item.resource_id, item.resource_url, item.resource_type)
                  for item in items]
        cursor.executemany(query, values)
        self.conn.commit()
        cursor.close()

    def add_processed_user(self, username: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT IGNORE INTO processed_users (user_name) VALUES (%s)",
            (username,)
        )
        self.conn.commit()
        cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()


class InstagramCrawler:
    def __init__(self):
        self.config = Config()
        self.client = Client()
        self.db_manager = DataManager(self.config)

    def login(self) -> bool:
        try:
            self.client.login(self.config.username, self.config.password)
            logging.info("Login successful")
            return True
        except Exception as e:
            logging.error(f"Login failed: {e}")
            return False

    def process_user(self, username: str, is_processed: bool) -> Optional[List[MediaItem]]:
        try:
            user_id = self.client.user_id_from_username(username)
            limit = (self.config.existing_user_post_limit if is_processed
                     else self.config.new_user_post_limit)

            medias = self.client.user_medias(user_id, limit)
            return [
                MediaItem(
                    id=str(uuid.uuid4()),
                    user_id=str(media.id),
                    user_name=username,
                    resource_id=str(resource.pk),
                    resource_url=str(resource.thumbnail_url),
                    resource_type=resource.media_type,
                )
                for media in medias
                for resource in media.resources
            ]
        except Exception as e:
            logging.error(f"Error processing user {username}: {e}")
            return None

    def run(self):
        if not self.login():
            return

        processed_users = self.db_manager.get_processed_users()
        existing_ids = self.db_manager.get_existing_resource_ids()

        for username in self.config.target_usernames:
            if items := self.process_user(username, username in processed_users):
                new_items = [item for item in items
                             if item.resource_id not in existing_ids]

                if new_items:
                    self.db_manager.save_media_items(new_items)
                    logging.info(
                        f"Added {len(new_items)} new media from {username}")

                if username not in processed_users:
                    self.db_manager.add_processed_user(username)

        self.db_manager.close()


def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/crawler_{datetime.now():%Y%m%d}.log'),
            logging.StreamHandler()
        ]
    )


def main():
    setup_logging()
    crawler = InstagramCrawler()
    crawler.run()


if __name__ == "__main__":
    main()

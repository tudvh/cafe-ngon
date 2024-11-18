from instagrapi import Client
from dotenv import load_dotenv
from typing import List, Dict, Optional
from dataclasses import dataclass
import os
import json
import logging
from datetime import datetime


@dataclass(frozen=True)
class MediaItem:
    user_id: str
    user_name: str
    resource_id: str
    resource_url: str

    def to_dict(self) -> Dict:
        return self.__dict__


class Config:
    def __init__(self):
        load_dotenv()
        self._validate_env()

    def _validate_env(self):
        required = ['LOGIN_USERNAME', 'LOGIN_PASSWORD', 'INSTAGRAM_USERNAMES']
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(
                f"Missing environment variables: {', '.join(missing)}")

    @property
    def username(self) -> str:
        return os.getenv('LOGIN_USERNAME')

    @property
    def password(self) -> str:
        return os.getenv('LOGIN_PASSWORD')

    @property
    def target_usernames(self) -> List[str]:
        return os.getenv('INSTAGRAM_USERNAMES', '').split(',')

    @property
    def new_user_post_limit(self) -> int:
        return int(os.getenv('NEW_USER_POST_LIMIT', '20'))

    @property
    def existing_user_post_limit(self) -> int:
        return int(os.getenv('EXISTING_USER_POST_LIMIT', '5'))


class DataManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._init_files()

    def _init_files(self):
        for filename in ['media_data.json', 'processed_users.json']:
            filepath = os.path.join(self.data_dir, filename)
            if not os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    json.dump([], f)

    def load_data(self) -> tuple[list[dict], list[str]]:
        try:
            media_data = self._read_json('media_data.json')
            processed_users = self._read_json('processed_users.json')
            return media_data, processed_users
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return [], []

    def _read_json(self, filename: str) -> list:
        with open(os.path.join(self.data_dir, filename)) as f:
            return json.load(f)

    def save_data(self, media_data: List[Dict], processed_users: List[str]):
        try:
            self._write_json('media_data.json', media_data)
            self._write_json('processed_users.json', processed_users)
            logging.info("Data saved successfully")
        except Exception as e:
            logging.error(f"Error saving data: {e}")

    def _write_json(self, filename: str, data: list):
        with open(os.path.join(self.data_dir, filename), 'w') as f:
            json.dump(data, f, indent=2)


class InstagramCrawler:
    def __init__(self):
        self.config = Config()
        self.client = Client()
        self.data_manager = DataManager()

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
            limit = self.config.existing_user_post_limit if is_processed else self.config.new_user_post_limit

            medias = self.client.user_medias(user_id, limit)

            return [
                MediaItem(
                    user_id=str(media.id),
                    user_name=username,
                    resource_id=str(resource.pk),
                    resource_url=str(resource.thumbnail_url)
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

        media_data, processed_users = self.data_manager.load_data()
        existing_ids = {item['resource_id'] for item in media_data}

        for username in self.config.target_usernames:
            if items := self.process_user(username, username in processed_users):
                new_items = [item.to_dict() for item in items
                             if item.resource_id not in existing_ids]

                if new_items:
                    media_data.extend(new_items)
                    logging.info(
                        f"Added {len(new_items)} new media from {username}")

                if username not in processed_users:
                    processed_users.append(username)

                self.data_manager.save_data(media_data, processed_users)


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

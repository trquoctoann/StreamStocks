import logging
import os
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import psycopg2
from dotenv import load_dotenv
from vnstock3 import Vnstock

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")


class BasePeriodicIngestor(ABC):
    def __init__(self, ingested_table):
        self.logger = self.setup_logger(os.getenv("LOG_DIR", "/app/logs"), f"{self.__class__.__name__}.log")
        self.source_data = self.setup_vnstock()
        self.ingested_table = ingested_table

    def setup_logger(self, log_dir, log_file_name):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file_path = os.path.join(log_dir, log_file_name)
        if not os.path.exists(log_file_path):
            with open(log_file_path, "w") as f:
                f.write("")

        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(log_file_path)
        file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_formatter = logging.Formatter("%(asctime)s - %(message)s")
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        return logger

    def setup_vnstock(self):
        return Vnstock().stock(symbol="ACB", source="VCI")

    def build_save_query(self, line_data: Dict[str, Any]) -> str:
        columns = ", ".join(line_data.keys())
        placeholders = ", ".join(["%s"] * len(line_data))

        query = f"INSERT INTO {self.ingested_table} ({columns}) VALUES ({placeholders});"
        return query

    def save_data(self, conn, data: List[Dict[str, Any]]) -> None:
        cursor = conn.cursor()
        try:
            for line_data in data:
                query = self.build_save_query(line_data)
                values = tuple(line_data.values())
                cursor.execute(query, values)
            conn.commit()
        finally:
            cursor.close()

    def periodic_run(self):
        raw_data = self.fetch_data()
        processed_data = self.process_data(raw_data)

        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
        try:
            new_data = self.check_data_existence(conn, processed_data)

            if new_data:
                self.save_data(conn, new_data)

            self.remove_stale_data(conn, processed_data)
        finally:
            conn.close()

    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def check_data_existence(self, conn, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def remove_stale_data(self, conn, data: List[Dict[str, Any]]) -> None:
        pass

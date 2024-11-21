from typing import Any, Dict, List

from base_periodic_ingestor import BasePeriodicIngestor


class ListedStockIngestor(BasePeriodicIngestor):
    def fetch_data(self) -> List[Dict[str, Any]]:
        return self.source_data.listing.symbols_by_industries().to_dict(orient="records")

    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed_data = []

        for record in raw_data:
            if not record.get("symbol"):
                continue
            processed_data.append(record)

        return processed_data

    def check_data_existence(self, conn, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cursor = conn.cursor()
        try:
            new_data = []
            for line_data in data:
                unique_key = line_data.get("symbol")
                if unique_key is None:
                    continue

                query = f"SELECT COUNT(*) FROM {self.ingested_table} WHERE symbol = %s"
                cursor.execute(query, (unique_key,))
                if cursor.fetchone()[0] == 0:
                    new_data.append(line_data)
        finally:
            cursor.close()
        return new_data

    def remove_stale_data(self, conn, source_data: List[Dict[str, Any]]) -> None:
        source_symbols = [item["symbol"] for item in source_data]
        cursor = conn.cursor()
        try:
            query = f"""
                DELETE FROM {self.ingested_table}
                WHERE symbol NOT IN %s;
            """
            cursor.execute(query, (tuple(source_symbols),))
            conn.commit()
        finally:
            cursor.close()

import json
import asyncio
import asyncpg
import time
from datetime import datetime
from . import task1, task2, task3, task4

# Thông tin kết nối PostgreSQL
DB_CONFIG = {
    "database": "postgres",
    "user": "runner_vesta",
    "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
    "host": "118.69.83.26",
    "port": "5444",
}

async def fetch_data(date_key):
    """Sử dụng connection pool để chạy tất cả truy vấn đồng thời"""
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        results = await asyncio.gather(
            task2.query_top_5_advance_stocks(pool, date_key),
            task2.query_top_5_decline_stocks(pool, date_key),
            task2.highest_volume_stocks(pool, date_key),
            task2.highest_volatility_stock(pool, date_key),
            task1.query_index_data(pool, date_key),
            task3.fetch_stock_data(pool, date_key),
            task4.fetch_foreign_trading(pool, date_key),
            task4.fetch_incountry_trading(pool, date_key),
        )
        return results

async def create_new_evening_api(date_key):
    """Trả về báo cáo dưới dạng JSON"""
    start_time = time.time()

    # Lấy dữ liệu từ PostgreSQL
    advance_result, decline_result, stock_volumn, highest_volatility, query_index_data, stock_data_task3, foreign_trading, incountry_trading = await fetch_data(date_key)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)

    # Định dạng JSON
    report_data = {
        "date": date_key,
        "Tổng kết thị trường": json.loads(query_index_data),
        "Biến động cổ phiếu nổi bật": {
            "Top 5 tăng mạnh": json.loads(advance_result),
            "Top 5 giảm mạnh": json.loads(decline_result),
            "Cổ phiếu có thanh khoản cao nhất": json.loads(stock_volumn),
            "Cổ phiếu có biên độ dao động mạnh nhất": json.loads(highest_volatility)
        },
        "Diễn biến ngành nghề": json.loads(stock_data_task3),
        "Giao Dịch Khối Ngoại và Tổ Chức": {
            "Khối ngoại": json.loads(foreign_trading),
            "Tổ chức trong nước": json.loads(incountry_trading)
        },
        "elapsed_time": f"{elapsed_time} giây",
        "timestamp": datetime.now().isoformat()
    }

    return json.dumps(report_data, ensure_ascii=False, indent=4)


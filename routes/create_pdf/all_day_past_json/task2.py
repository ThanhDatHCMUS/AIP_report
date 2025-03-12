import asyncio
import asyncpg
from datetime import datetime
import json
# 🟢 Hàm truy vấn top 5 cổ phiếu tăng mạnh nhất
async def query_top_5_advance_stocks(pool, date_key):
    async with pool.acquire() as conn:
        try:
            query = f"""
SELECT 
    f1.symbol,
    f1.priceclose - f2.priceclose AS close_diff,
    (f1.priceclose - f2.priceclose) / f2.priceclose * 100 AS advance_percent
FROM basement.aip_report f1
JOIN basement.aip_report f2 
    ON f1.symbol = f2.symbol 
    AND f2.date::date = 
        CASE 
            WHEN EXTRACT(DOW FROM f1.date::date) = 1 THEN f1.date::date - INTERVAL '3 days'   
            ELSE f1.date::date - INTERVAL '1 day'   
        END
WHERE f1.date::date = '{date_key}'
  AND f1.priceclose IS NOT NULL
ORDER BY advance_percent DESC
LIMIT 5;
            """
            rows = await conn.fetch(query)
            result = {"Top 5 cổ phiếu tăng mạnh nhất": [
                {
                    "rank": i+1, 
                    "symbol": row["symbol"], 
                    "advance_percent": round(row["advance_percent"], 2), 
                    "close_diff": round(row["close_diff"], 2)
                }
                for i, row in enumerate(rows)
            ]}
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return {"error": f"Lỗi khi chạy truy vấn tăng mạnh nhất: {str(e)}"}

# 🟢 Hàm truy vấn top 5 cổ phiếu giảm mạnh nhất
async def query_top_5_decline_stocks(pool, date_key):
    async with pool.acquire() as conn:
        try:
            query = f"""
SELECT 
    f1.symbol,
    f1.priceclose - f2.priceclose AS close_diff,
    (f1.priceclose - f2.priceclose) / f2.priceclose * 100 AS advance_percent
FROM basement.aip_report f1
JOIN basement.aip_report f2 
    ON f1.symbol = f2.symbol 
    AND f2.date::date = 
        CASE 
            WHEN EXTRACT(DOW FROM f1.date::date) = 1 THEN f1.date::date - INTERVAL '3 days'   
            ELSE f1.date::date - INTERVAL '1 day'   
        END
WHERE f1.date::date = '{date_key}'
  AND f1.priceclose IS NOT NULL
ORDER BY advance_percent 
LIMIT 5;
            """
            rows = await conn.fetch(query)
            result = {"top_5_decline": [
                {"rank": i+1, "symbol": row["symbol"], "decline_percent": round(abs(row["advance_percent"]), 2), "close_diff": round(row["close_diff"], 2)}
                for i, row in enumerate(rows)
            ]}
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return {"error": f"Lỗi khi chạy truy vấn giảm mạnh nhất: {str(e)}"}

# Cổ phiếu có thanh khoản cao nhất
async def highest_volume_stocks(pool, date_key):
    async with pool.acquire() as conn:
        try:
            query = f"""
SELECT symbol, totalvolume 
FROM basement.aip_report 
WHERE date::date = '{date_key}'  
ORDER BY totalvolume DESC 
LIMIT 1;
            """
            row = await conn.fetchrow(query)
            result =  {"highest_volume_stock": {"symbol": row["symbol"], "total_volume": row["totalvolume"]} if row else None}
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return {"error": f"Lỗi khi truy vấn khối lượng giao dịch cao nhất: {str(e)}"}

# Cổ phiếu có biên độ dao động mạnh nhất
async def highest_volatility_stock(pool, date_key):
    async with pool.acquire() as conn:
        try:
            query = f"""
SELECT 
    f1.symbol,
    (f1.pricehigh - f1.pricelow) / f2.priceclose AS advance_percent,
    (f1.pricehigh - f1.pricelow) AS advance_grade
FROM basement.aip_report f1
JOIN basement.aip_report f2 
    ON f1.symbol = f2.symbol 
    AND f2.date::date =  
        CASE 
            WHEN EXTRACT(DOW FROM f1.date::date) = 1 THEN f1.date::date - INTERVAL '3 days'   
            ELSE f1.date::date - INTERVAL '1 day'   
        END 
WHERE f1.date::date = '{date_key}' 
ORDER BY advance_percent DESC
LIMIT 1;
            """
            row = await conn.fetchrow(query)
            result = {"highest_volatility_stock": {"symbol": row["symbol"], "volatility_percent": round(row["advance_percent"], 2), "volatility_grade": round(row["advance_grade"], 2)} if row else None}
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": f"Lỗi khi truy vấn biên độ dao động mạnh nhất: {str(e)}"}, ensure_ascii=False)

# import asyncio
# import asyncpg
# from datetime import datetime

# async def main():
#     # Thông tin kết nối PostgreSQL
#     DB_CONFIG = {
#         "database": "postgres",
#         "user": "runner_vesta",
#         "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
#         "host": "118.69.83.26",
#         "port": "5444",
#     }

#     # Ngày cần truy vấn (định dạng YYYY-MM-DD)
#     date_key = "2022-12-13"

#     # Kết nối tới database
#     pool = await asyncpg.create_pool(**DB_CONFIG)

#     try:
#         # Chạy các hàm truy vấn
#         top_5_advance = await query_top_5_advance_stocks(pool, date_key)
#         top_5_decline = await query_top_5_decline_stocks(pool, date_key)
#         highest_volume = await highest_volume_stocks(pool, date_key)
#         highest_volatility = await highest_volatility_stock(pool, date_key)

#         # In kết quả
#         print(top_5_advance)
#         print(top_5_decline)
#         print(highest_volume)
#         print(highest_volatility)

#     finally:
#         await pool.close()  # Đóng kết nối khi hoàn thành

# # Chạy chương trình
# asyncio.run(main())

import asyncio
import asyncpg
import pandas as pd
import psycopg2
from datetime import datetime
import json
async def fetch_foreign_trading(pool, date):
    """
    Hàm lấy dữ liệu giao dịch của khối ngoại từ PostgreSQL, trả về JSON.
    """
    async with pool.acquire() as conn:
        queries = {
            "ban_rong": """
                SELECT SUM(sellforeignvalue) - SUM(buyforeignvalue) AS ban_rong 
                FROM basement.aip_report_aft WHERE date = $1
            """,
            "mua_rong": """
                SELECT SUM(buyforeignvalue) - SUM(sellforeignvalue) AS mua_rong
                FROM basement.aip_report_aft WHERE date::date = $1
            """,
            "ban_rong_cao_nhat": """
                SELECT symbol, sellforeignvalue - buyforeignvalue AS ban_rong_cao_nhat 
                FROM basement.aip_report_aft WHERE date::date = $1
                ORDER BY ban_rong_cao_nhat DESC LIMIT 1
            """,
            "mua_rong_cao_nhat": """
                SELECT symbol, buyforeignvalue - sellforeignvalue AS mua_rong_cao_nhat 
                FROM basement.aip_report_aft WHERE date::date = $1
                ORDER BY mua_rong_cao_nhat DESC LIMIT 1
            """
        }

        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
            date = date_obj

            ban_rong_row = await conn.fetchrow(queries["ban_rong"], date)
            mua_rong_row = await conn.fetchrow(queries["mua_rong"], date)
            ban_rong_cao_nhat_row = await conn.fetchrow(queries["ban_rong_cao_nhat"], date)
            mua_rong_cao_nhat_row = await conn.fetchrow(queries["mua_rong_cao_nhat"], date)

            # Lấy giá trị từ kết quả
            ban_rong = ban_rong_row["ban_rong"] if ban_rong_row and ban_rong_row["ban_rong"] is not None else 0
            mua_rong = mua_rong_row["mua_rong"] if mua_rong_row and mua_rong_row["mua_rong"] is not None else 0
            
            symbol_ban_rong = ban_rong_cao_nhat_row["symbol"] if ban_rong_cao_nhat_row else "Không có dữ liệu"
            value_ban_rong = ban_rong_cao_nhat_row["ban_rong_cao_nhat"] if ban_rong_cao_nhat_row else 0

            symbol_mua_rong = mua_rong_cao_nhat_row["symbol"] if mua_rong_cao_nhat_row else "Không có dữ liệu"
            value_mua_rong = mua_rong_cao_nhat_row["mua_rong_cao_nhat"] if mua_rong_cao_nhat_row else 0

            # Định dạng kết quả JSON
            result_json = {
                "Nhà Đầu tư nước ngoài": {
                    "Tổng giá trị bán ròng": round(ban_rong / 1_000_000_000, 2),
                    "Tổng giá trị mua ròng": round(mua_rong / 1_000_000_000, 2),
                    "Cổ phiếu bị bán ròng nhiều nhất": {
                        "symbol": symbol_ban_rong,
                        "value": round(value_ban_rong / 1_000_000_000, 2) if value_ban_rong else 0
                    },
                    "Cổ phiếu được mua ròng nhiều nhất": {
                        "symbol": symbol_mua_rong,
                        "value": round(value_mua_rong / 1_000_000_000, 2) if value_mua_rong else 0
                    }
                },
                "timestamp": datetime.now().isoformat()
            }

            return json.dumps(result_json, ensure_ascii=False, indent=4)

        except Exception as e:
            return json.dumps({
                "error": f"Lỗi khi truy vấn dữ liệu: {str(e)}"
            }, ensure_ascii=False, indent=4)

async def fetch_incountry_trading(pool, date):
    """
    Hàm lấy dữ liệu giao dịch của tổ chức trong nước từ PostgreSQL, trả về JSON.
    """
    async with pool.acquire() as conn:
        queries = {
            "mua_rong": """
                SELECT SUM(proptradingnetvalue) AS mua_rong
                FROM basement.aip_report WHERE date::date = $1
            """,
            "ban_rong": """
                SELECT SUM(proptradingnetvalue) AS ban_rong
                FROM basement.aip_report WHERE date::date = $1
            """,
            "mua_rong_lon_nhat": """
                SELECT symbol, proptradingnetvalue AS mua_rong_lon_nhat
                FROM basement.aip_report WHERE date::date = $1
                ORDER BY mua_rong_lon_nhat DESC LIMIT 1
            """,
            "ban_rong_lon_nhat": """
                SELECT symbol, proptradingnetvalue AS ban_rong_lon_nhat
                FROM basement.aip_report WHERE date::date = $1
                ORDER BY ban_rong_lon_nhat ASC LIMIT 1
            """
        }

        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
            date = date_obj

            mua_rong_row = await conn.fetchrow(queries["mua_rong"], date)
            ban_rong_row = await conn.fetchrow(queries["ban_rong"], date)
            mua_rong_lon_nhat_row = await conn.fetchrow(queries["mua_rong_lon_nhat"], date)
            ban_rong_lon_nhat_row = await conn.fetchrow(queries["ban_rong_lon_nhat"], date)

            # Lấy giá trị từ kết quả
            mua_rong = mua_rong_row["mua_rong"] if mua_rong_row and mua_rong_row["mua_rong"] is not None else 0
            ban_rong = ban_rong_row["ban_rong"] if ban_rong_row and ban_rong_row["ban_rong"] is not None else 0

            symbol_mua_rong = mua_rong_lon_nhat_row["symbol"] if mua_rong_lon_nhat_row else "Không có dữ liệu"
            value_mua_rong = mua_rong_lon_nhat_row["mua_rong_lon_nhat"] if mua_rong_lon_nhat_row else 0

            symbol_ban_rong = ban_rong_lon_nhat_row["symbol"] if ban_rong_lon_nhat_row else "Không có dữ liệu"
            value_ban_rong = ban_rong_lon_nhat_row["ban_rong_lon_nhat"] if ban_rong_lon_nhat_row else 0

            # Định dạng kết quả JSON
            result_json = {
                "Tổ chức trong nước": {
                    "Tổng mua ròng": round(mua_rong / 1_000_000_000, 2),
                    "Tổng bán ròng": round(ban_rong / 1_000_000_000, 2),
                    "Cổ phiếu được mua ròng mạnh nhất": {
                        "symbol": symbol_mua_rong,
                        "value": round(value_mua_rong / 1_000_000_000, 2) if value_mua_rong else 0
                    },
                    "Cổ phiếu bị bán ròng mạnh nhất": {
                        "symbol": symbol_ban_rong,
                        "value": round(value_ban_rong / 1_000_000_000, 2) if value_ban_rong else 0
                    }
                },
                "timestamp": datetime.now().isoformat()
            }

            print("Xong Task 4 lúc: ", datetime.now())
            return json.dumps(result_json, ensure_ascii=False, indent=4)

        except Exception as e:
            return json.dumps({
                "error": f"Lỗi khi truy vấn dữ liệu: {str(e)}"
            }, ensure_ascii=False, indent=4)
        
        
import asyncio
import asyncpg

# Cấu hình kết nối cơ sở dữ liệu
DB_CONFIG = {
    "database": "postgres",
    "user": "runner_vesta",
    "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
    "host": "118.69.83.26",
    "port": "5444",
}

# async def main():
#     try:
#         # Tạo kết nối pool
#         pool = await asyncpg.create_pool(**DB_CONFIG)
#         print("✅ Kết nối đến PostgreSQL thành công!")

#         date_key = "2022-12-13"  # Ngày cần truy vấn
#         # Thử truy vấn kiểm tra
#         async with pool.acquire() as conn:
#             row = await conn.fetchrow("SELECT NOW() AS current_time;")
#             print(f"🕒 Thời gian hiện tại trong DB: {row['current_time']}")

#         # Đóng pool sau khi hoàn tất

#         output = await fetch_incountry_trading(pool, date_key)

#     # In kết quả
#         print(output)
#         await pool.close()
    
#     except Exception as e:
#         print(f"❌ Lỗi kết nối PostgreSQL: {e}")

# if __name__ == "__main__":
#     asyncio.run(main())

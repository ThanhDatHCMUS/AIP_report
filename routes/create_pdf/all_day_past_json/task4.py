import asyncio
import asyncpg
import json
from datetime import datetime

async def fetch_foreign_trading(pool, date):
    async with pool.acquire() as conn:
        queries = {
            "ban_rong": """
                SELECT SUM(sellforeignvalue) - SUM(buyforeignvalue) AS ban_rong
                FROM basement.aip_report WHERE date::date = $1
            """,
            "mua_rong": """
                SELECT SUM(buyforeignvalue) - SUM(sellforeignvalue) AS mua_rong
                FROM basement.aip_report WHERE date::date = $1
            """,
            "ban_rong_cao_nhat": """
                SELECT symbol, sellforeignvalue - buyforeignvalue AS ban_rong 
                FROM basement.aip_report WHERE date::date = $1
                ORDER BY ban_rong DESC LIMIT 1
            """,
            "mua_rong_cao_nhat": """
                SELECT symbol, buyforeignvalue - sellforeignvalue AS mua_rong 
                FROM basement.aip_report WHERE date::date = $1
                ORDER BY mua_rong DESC LIMIT 1
            """
        }

        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
            ban_rong_row = await conn.fetchrow(queries["ban_rong"], date_obj)
            mua_rong_row = await conn.fetchrow(queries["mua_rong"], date_obj)
            ban_rong_cao_nhat_row = await conn.fetchrow(queries["ban_rong_cao_nhat"], date_obj)
            mua_rong_cao_nhat_row = await conn.fetchrow(queries["mua_rong_cao_nhat"], date_obj)

            result = {
                "nha_dau_tu_nuoc_ngoai": {
                    "tong_ban_rong": ban_rong_row["ban_rong"] if ban_rong_row else 0,
                    "tong_mua_rong": mua_rong_row["mua_rong"] if mua_rong_row else 0,
                    "ban_rong_cao_nhat": {
                        "symbol": ban_rong_cao_nhat_row["symbol"] if ban_rong_cao_nhat_row else "Không có dữ liệu",
                        "value": ban_rong_cao_nhat_row["ban_rong"] if ban_rong_cao_nhat_row else 0
                    },
                    "mua_rong_cao_nhat": {
                        "symbol": mua_rong_cao_nhat_row["symbol"] if mua_rong_cao_nhat_row else "Không có dữ liệu",
                        "value": mua_rong_cao_nhat_row["mua_rong"] if mua_rong_cao_nhat_row else 0
                    }
                }
            }
            return json.dumps(result, indent=4, ensure_ascii=False)

        except Exception as e:
            print(f"Lỗi khi truy vấn dữ liệu: {e}")
            return json.dumps({"error": str(e)})


async def fetch_incountry_trading(pool, date):
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
            mua_rong_row = await conn.fetchrow(queries["mua_rong"], date_obj)
            ban_rong_row = await conn.fetchrow(queries["ban_rong"], date_obj)
            mua_rong_lon_nhat_row = await conn.fetchrow(queries["mua_rong_lon_nhat"], date_obj)
            ban_rong_lon_nhat_row = await conn.fetchrow(queries["ban_rong_lon_nhat"], date_obj)

            result = {
                "to_chuc_trong_nuoc": {
                    "tong_mua_rong": mua_rong_row["mua_rong"] if mua_rong_row else 0,
                    "tong_ban_rong": ban_rong_row["ban_rong"] if ban_rong_row else 0,
                    "mua_rong_lon_nhat": {
                        "symbol": mua_rong_lon_nhat_row["symbol"] if mua_rong_lon_nhat_row else "Không có dữ liệu",
                        "value": round(mua_rong_lon_nhat_row["mua_rong_lon_nhat"] / 1000000000, 2) if mua_rong_lon_nhat_row else 0
                    },
                    "ban_rong_lon_nhat": {
                        "symbol": ban_rong_lon_nhat_row["symbol"] if ban_rong_lon_nhat_row else "Không có dữ liệu",
                        "value": round(ban_rong_lon_nhat_row["ban_rong_lon_nhat"] / 1000000000, 2) if ban_rong_lon_nhat_row else 0
                    }
                }
            }
            return json.dumps(result, indent=4, ensure_ascii=False)

        except Exception as e:
            print(f"Lỗi khi truy vấn dữ liệu: {e}")
            return json.dumps({"error": str(e)})

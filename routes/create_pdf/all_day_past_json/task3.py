import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from vnstock import Vnstock
import json
async def fetch_stock_data(pool, date_key):
    async with pool.acquire() as conn:
        try:
            print(date_key)
            query = """
            SELECT b1.symbol as ticker, b1.priceclose close1, b2.priceclose close2 
            FROM basement.aip_report b1 
            LEFT join basement.aip_report b2 
            ON b1.symbol = b2.symbol 
            AND b2.date::date =
                CASE 
                    WHEN EXTRACT(DOW FROM b1.date::date) = 1 
                    THEN b1.date::date - INTERVAL '3 days'   
                    ELSE b1.date::date - INTERVAL '1 day'   
                END 
            WHERE b1.date::date = $1
            """

            date_obj = datetime.strptime(date_key, "%Y-%m-%d").date()
            date_key = date_obj
            rows = await conn.fetch(query, date_key)

            if not rows:
                return json.dumps({
                    "message": "Không có dữ liệu chứng khoán cho ngày này."
                }, ensure_ascii=False)

            data_in_pg = pd.DataFrame(rows, columns=["ticker", "close1", "close2"])

            stock = Vnstock().stock(source='VCI')
            company_industry = stock.listing.symbols_by_industries()

            df_merged_pg = pd.merge(data_in_pg, company_industry, left_on='ticker', right_on='symbol', how='left')
            df_merged_pg['increase'] = df_merged_pg['close1'] - df_merged_pg['close2']

            df_grouped = df_merged_pg.groupby('icb_name2')[['increase', 'close2']].sum().reset_index()
            df_grouped['increase_percent'] = df_grouped['increase'] / df_grouped['close2']
            df_sorted = df_grouped.sort_values(by='increase_percent', ascending=False)

            df_high_5 = df_sorted.head(1)
            df_bottom = df_sorted.tail(1)

            try:
                increase_bank = df_grouped[df_grouped['icb_name2'] == 'Ngân hàng']['increase_percent'].iloc[0]
                increase_bds = df_grouped[df_grouped['icb_name2'] == 'Bất động sản']['increase_percent'].iloc[0]
                increase_dk = df_grouped[df_grouped['icb_name2'] == 'Dầu khí']['increase_percent'].iloc[0]
            except IndexError:
                increase_bank = increase_bds = increase_dk = None

            result_json = {
                "top_increase_sector": {
                    "name": df_high_5['icb_name2'].values[0],
                    "percent": round(df_high_5['increase_percent'].values[0] * 100, 2)
                },
                "top_decrease_sector": {
                    "name": df_bottom['icb_name2'].values[0],
                    "percent": round(df_bottom['increase_percent'].values[0] * 100, 2)
                },
                "key_sectors": {
                    "banking": {"change": "increase" if increase_bank >= 0 else "decrease", "percent": round(increase_bank * 100, 2) if increase_bank is not None else None},
                    "real_estate": {"change": "increase" if increase_bds >= 0 else "decrease", "percent": round(increase_bds * 100, 2) if increase_bds is not None else None},
                    "oil_gas": {"change": "increase" if increase_dk >= 0 else "decrease", "percent": round(increase_dk * 100, 2) if increase_dk is not None else None}
                }
            }

            print("Xong task 3 lúc: ", datetime.now())
            return json.dumps(result_json, ensure_ascii=False)

        except Exception as e:
            print(f"Lỗi khi truy vấn dữ liệu task 3: {e}")
            return json.dumps({"error": "Không thể truy vấn dữ liệu."}, ensure_ascii=False)

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from vnstock import Vnstock
import json
# Cấu hình Database
DB_CONFIG = {
    "database": "postgres",
    "user": "runner_vesta",
    "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
    "host": "118.69.83.26",
    "port": "5444",
}

async def fetch_stock_data(pool, date_key):
    """
    Hàm lấy dữ liệu chứng khoán từ PostgreSQL, ghép nối với ngành công nghiệp,
    tính toán mức tăng giảm và nhóm dữ liệu theo ngành, trả về JSON.
    """
    async with pool.acquire() as conn:
        try:
            print(date_key)

            query = """
            SELECT b1.symbol as ticker, b1.priceclose close1, b2.priceclose close2 
            FROM basement.aip_report_aft b1 
            LEFT join basement.aip_report_aft b2 
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
            rows = await conn.fetch(query, date_obj)

            if not rows:
                return json.dumps({
                    "message": "Không có dữ liệu chứng khoán cho ngày này."
                }, ensure_ascii=False, indent=4)

            # Chuyển dữ liệu thành DataFrame
            data_in_pg = pd.DataFrame(rows, columns=["ticker", "close1", "close2"])

            # Lấy dữ liệu ngành
            stock = Vnstock().stock(source='VCI')
            company_industry = stock.listing.symbols_by_industries()

            # Merge dữ liệu ngành
            df_merged_pg = pd.merge(data_in_pg, company_industry, left_on='ticker', right_on='symbol', how='left')

            # Tính toán mức tăng/giảm
            df_merged_pg['increase'] = df_merged_pg['close1'] - df_merged_pg['close2']

            # Nhóm theo ngành
            df_grouped = df_merged_pg.groupby('icb_name2')[['increase', 'close2']].sum().reset_index()
            df_grouped['increase_percent'] = df_grouped['increase'] / df_grouped['close2']
            df_sorted = df_grouped.sort_values(by='increase_percent', ascending=False)

            # Lấy nhóm ngành tăng mạnh nhất và giảm mạnh nhất
            df_high_5 = df_sorted.head(1)
            df_bottom = df_sorted.tail(1)

            # Kiểm tra ngành cụ thể
            try:
                increase_bank = df_grouped[df_grouped['icb_name2'] == 'Ngân hàng']['increase_percent'].iloc[0]
                increase_bds = df_grouped[df_grouped['icb_name2'] == 'Bất động sản']['increase_percent'].iloc[0]
                increase_dk = df_grouped[df_grouped['icb_name2'] == 'Dầu khí']['increase_percent'].iloc[0]
            except IndexError:
                increase_bank = increase_bds = increase_dk = None

            # Tạo kết quả JSON
            result_json = {
                "Nhóm ngành tăng mạnh nhất": {
                    "name": df_high_5['icb_name2'].values[0] if not df_high_5.empty else "Không có dữ liệu",
                    "increase_percent": round(df_high_5['increase_percent'].values[0] * 100, 2) if not df_high_5.empty else 0
                },
                "Nhóm ngành giảm mạnh nhất": {
                    "name": df_bottom['icb_name2'].values[0] if not df_bottom.empty else "Không có dữ liệu",
                    "increase_percent": round(df_bottom['increase_percent'].values[0] * 100, 2) if not df_bottom.empty else 0
                },
                "Hiệu suất một số nhóm ngành chính": {
                    "Ngân hàng": {
                        "status": "Tăng" if increase_bank is not None and increase_bank >= 0 else "Giảm",
                        "percent": round(increase_bank * 100, 2) if increase_bank is not None else None
                    },
                    "Bất động sản": {
                        "status": "Tăng" if increase_bds is not None and increase_bds >= 0 else "Giảm",
                        "percent": round(increase_bds * 100, 2) if increase_bds is not None else None
                    },
                    "Dầu khí": {
                        "status": "Tăng" if increase_dk is not None and increase_dk >= 0 else "Giảm",
                        "percent": round(increase_dk * 100, 2) if increase_dk is not None else None
                    }
                },
                "timestamp": datetime.now().isoformat()
            }

            print("Xong task 3 lúc: ", datetime.now())
            return json.dumps(result_json, ensure_ascii=False, indent=4)

        except Exception as e:
            print(f"Lỗi khi truy vấn dữ liệu task 3: {e}")
            return json.dumps({
                "error": "Không thể truy vấn dữ liệu."
            }, ensure_ascii=False, indent=4)
import asyncio
import asyncpg
import pandas as pd
from datetime import datetime
from vnstock import Vnstock

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
    tính toán mức tăng giảm và nhóm dữ liệu theo ngành.
    """
    async with pool.acquire() as conn:
        try:
            # Chuyển đổi date_key từ int (YYYYMMDD) sang str ("YYYY-MM-DD")

            print(date_key)
            # Query dữ liệu từ PostgreSQL
            query = """
                SELECT b1.symbol as ticker, b1.priceclose close1, b2.priceclose close2 
                FROM basement.aip_report b1 
                LEFT join basement.aip_report_aft b2 
                ON b1.symbol = b2.symbol 
                AND b2.date = b1.date
                WHERE b1.date::date = $1 
            """

            date_obj = datetime.strptime(date_key, "%Y-%m-%d").date()
            date_key = date_obj
            rows = await conn.fetch(query, date_key)

            if not rows:
                return "Không có dữ liệu chứng khoán cho ngày này."

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

            # Tạo kết quả
            result_str = f"""
Nhóm ngành tăng mạnh nhất: {df_high_5['icb_name2'].values[0]} ({df_high_5['increase_percent'].values[0]:.2%})
Nhóm ngành giảm mạnh nhất: {df_bottom['icb_name2'].values[0]} ({df_bottom['increase_percent'].values[0]:.2%})
Hiệu suất một số nhóm ngành chính:
- Ngân hàng: {'Tăng' if increase_bank >= 0 else 'Giảm'} {increase_bank:.2%}
- Bất động sản: {'Tăng' if increase_bds >= 0 else 'Giảm'} {increase_bds:.2%}
- Dầu khí: {'Tăng' if increase_dk >= 0 else 'Giảm'} {increase_dk:.2%}
            """
            print("Xong task 3 lúc: ", datetime.now())
            return result_str.strip()

        except Exception as e:
            print(f"Lỗi khi truy vấn dữ liệu task 3: {e}")
            return "Không thể truy vấn dữ liệu."


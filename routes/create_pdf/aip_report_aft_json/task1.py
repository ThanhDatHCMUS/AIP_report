import asyncio
import asyncpg
import json
from datetime import datetime
# Thông tin kết nối
DB_CONFIG = {
    "database": "postgres",
    "user": "runner_vesta",
    "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
    "host": "118.69.83.26",
    "port": "5444",
}

def get_last_friday(date):
    import datetime
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    if date.weekday() == 0:
        return (date - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    else:
        return (date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# 🟢 Hàm 1: Truy vấn và in kết quả trực tiếp

import json
from datetime import datetime

async def query_index_data(pool, date_key):
    async with pool.acquire() as conn:
        predate = str(get_last_friday(date_key))
        print('loại dữ liệu predate: ', type(predate))
        date_key_convert = date_key.replace("-", "")
        date_key = date_key_convert
        index_codes = ['VNINDEX', 'HNXINDEX', 'UPINDEX']

        # 🟢 Lấy dữ liệu chỉ số chính
        index_results = []
        for index in index_codes:
            print("Dữ liệu có dạng là: ", type(date_key))
            query = f"""
SELECT 
    f1.symbol,
    f1.priceclose - f2.priceclose AS change,
    (f1.priceclose - f2.priceclose) / f2.priceclose * 100 AS percent
FROM basement.aip_report_index_aft f1
JOIN basement.aip_report_index f2 
    ON f1.symbol = f2.symbol 
    AND f2.date = 
        CASE 
            WHEN EXTRACT(DOW FROM f1.date) = 1 THEN f1.date - INTERVAL '3 days'   
            ELSE f1.date - INTERVAL '1 day'   
        END
WHERE f1.date = '{date_key}'
  AND f1.symbol = '{index}'
            """
            row = await conn.fetchrow(query)
            if row:
                trend = "Tăng" if row["change"] > 0 else "Giảm"
                index_results.append({
                    "index": index,
                    "symbol": row["symbol"],
                    "trend": trend,
                    "change": round(abs(row["change"]), 2),
                    "percent": round(abs(row["percent"]), 2)
                })

        if not index_results:
            index_results = [{"index": "Không có dữ liệu"}]

        # 🟢 Lấy dữ liệu thanh khoản thị trường
        query_total_value = f"SELECT SUM(totalvalue) FROM basement.aip_report_aft WHERE date::date = '{date_key}'"
        total_value = await conn.fetchval(query_total_value) or 0

        query_total_share = f"SELECT SUM(totalvolume) FROM basement.aip_report_aft WHERE date::date = '{date_key}'"
        total_share = await conn.fetchval(query_total_share) or 0

        # 🟢 Lấy dữ liệu số lượng mã cổ phiếu
        count_stock = f"SELECT COUNT(*) FROM basement.aip_report_aft WHERE date::date = '{date_key}'"
        query_advance = f"""
            SELECT COUNT(*) FROM basement.aip_report_aft f1
            JOIN basement.aip_report_aft f2 ON f1.symbol = f2.symbol
            AND f2.date::date = '{predate}'
            WHERE f1.date::date = '{date_key}' AND f1.priceclose > f2.priceclose
        """
        query_no_change = f"""
            SELECT COUNT(*) FROM basement.aip_report_aft f1
            JOIN basement.aip_report_aft f2 ON f1.symbol = f2.symbol
            AND f2.date::date =
            CASE
                WHEN EXTRACT(DOW FROM f1.date::date) = 1 THEN f1.date::date - INTERVAL '3 days'
                ELSE f1.date::date - INTERVAL '1 day'
            END
            WHERE f1.date::date = '{date_key}' AND f1.priceclose = f2.priceclose
        """

        count_stock = await conn.fetchval(count_stock) or 0
        advance = await conn.fetchval(query_advance) or 0
        no_change = await conn.fetchval(query_no_change) or 0
        decline = count_stock - advance - no_change

        await conn.close()

        # 🟢 Format kết quả trả về JSON
        result_json = {
            "Chỉ số chính": index_results,
            "Thanh khoản thị trường": {
                "Tổng giá trị giao dịch": round(float(total_value) / 1_000_000_000, 2),
                "Tổng khối lượng giao dịch": round(float(total_share) / 1_000_000, 2)
            },
            "Số lượng mã cổ phiếu": {
                "Tăng giá": advance,
                "Giảm giá": decline,
                "Đứng giá": no_change
            },
            "timestamp": datetime.now().isoformat()
        }

        print("Xong task 1 lúc: ", datetime.now())
        return json.dumps(result_json, ensure_ascii=False, indent=4)


# DB_CONFIG = {
#     "database": "postgres",
#     "user": "runner_vesta",
#     "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
#     "host": "118.69.83.26",
#     "port": "5444",
# }
# async def main():
#     try:
#         # Tạo kết nối pool
#         pool = await asyncpg.create_pool(**DB_CONFIG)
#         print("✅ Kết nối đến PostgreSQL thành công!")

#         date_key = "2022-12-13"  # Ngày cần truy vấn
#         # Thử truy vấn kiểm tra
#         print("Bắt đầu task 1 lúc: ", datetime.now())
#         async with pool.acquire() as conn:
#             row = await conn.fetchrow("SELECT NOW() AS current_time;")


#         # Đóng pool sau khi hoàn tất

#         output = await query_index_data(pool, date_key)

#     # In kết quả
#         print(output)
#         print("Xong task 1 lúc: ", datetime.now())
#         await pool.close()
    
#     except Exception as e:
#         print(f"❌ Lỗi kết nối PostgreSQL: {e}")

# if __name__ == "__main__":
#     asyncio.run(main())
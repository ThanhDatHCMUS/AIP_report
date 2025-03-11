import asyncio
import asyncpg
import time
from . import task1
from . import task2
from . import task3
from . import task4


from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from datetime import datetime
# Thông tin kết nối PostgreSQL
DB_CONFIG = {
    "database": "postgres",
    "user": "runner_vesta",
    "password": "93pFCR0ALmu2aIVrRk4vIIfkT2055WBFDC94cKNus85FItqy8jfIDnmD6aV8rD4K",
    "host": "118.69.83.26",
    "port": "5444",
}

# Hàm tạo PDF báo cáo
def create_pdf_report(date_key, query_index_data, advance_result, decline_result, stock_volumn, highest_volatility, stock_data_task3, foreign_trading, incountry_trading, elapsed_time):
    pdf_path = f"{date_key}-morning.pdf"
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4

    # Đăng ký font hỗ trợ tiếng Việt
    pdfmetrics.registerFont(TTFont('DejaVu', './ttf/DejaVuSans.ttf'))

    # Cài đặt font tiêu đề
    c.setFont("DejaVu", 12)

    c.drawCentredString(width / 2, height - 20, f"BÁO CÁO KẾT THÚC PHIÊN GIAO DỊCH BUỔI SÁNG: {date_key}")

    y_position = height - 120  # Điều chỉnh vị trí xuống dưới

    # Cài đặt font nội dung
    c.setFont("DejaVu", 12)
    y_position = height - 50

    # Ghi nội dung vào PDF
    sections = [
        ("1. Tổng kết thị trường", query_index_data + "\n"),
        ("2. Biến động cổ phiếu nổi bật", advance_result + "\n" + decline_result + "\n" + stock_volumn + "\n" + highest_volatility + "\n"),
        ("3. Diễn biến ngành nghề", stock_data_task3 + "\n"),
        ("4. Giao Dịch Khối Ngoại và Tổ Chức", foreign_trading + "\n" + incountry_trading + "\n"),
    ]

    for title, content in sections:
        c.setFont("DejaVu", 12)
        c.drawString(50, y_position, title)
        y_position -= 20
        c.setFont("DejaVu", 12)
        for line in content.split("\n"):
            c.drawString(50, y_position, line)
            y_position -= 15
            if y_position < 0:  # Xuống trang mới nếu hết trang
                c.showPage()
                c.setFont("DejaVu", 12)
                y_position = height - 50

    # Lưu PDF
    c.save()
    print(f"File báo cáo đã được tạo: {pdf_path}")

async def fetch_data(date_key):
    """Sử dụng connection pool để chạy tất cả truy vấn đồng thời"""
    async with asyncpg.create_pool(**DB_CONFIG) as pool:

            # Chạy tất cả truy vấn song song
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

async def create_new_afternoon(date_key):


    # Lấy dữ liệu từ PostgreSQL
    advance_result, decline_result, stock_volumn, highest_volatility, query_index_data, stock_data_task3, foreign_trading, incountry_trading = await fetch_data(date_key)

    end_time = time.time()

    elapsed_time = end_time
    # Gọi hàm tạo PDF
    create_pdf_report(date_key, query_index_data, advance_result, decline_result, stock_volumn, highest_volatility, stock_data_task3, foreign_trading, incountry_trading, elapsed_time)

# if __name__ == "__main__":
#     date_key = "2022-12-13"  # Ngày cần truy vấn
#     start_time = time.time()
#     print(f"\n🕒 Đang chạy truy vấn lúc: {time.strftime('%Y-%m-%d %H:%M:%S')}")
#     asyncio.run(main(date_key))

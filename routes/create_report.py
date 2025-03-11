from flask import Blueprint, jsonify, request, send_file
from models import Users, db
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
import re
from .create_pdf.all_day_past.total import create_new
from .create_pdf.aip_report_aft.total import create_new_afternoon
from .create_pdf.aip_report_evening.total import create_new_evening
from datetime import datetime
import asyncio


report_bp = Blueprint('report', __name__)

@report_bp.route('/create-report/<string:day>', methods=['GET'])
def create_report_day(day):
    date_key = day  # Ngày cần truy vấn
    pdf_path = "{date_key}-allday.pdf".format(date_key=date_key)
    a = asyncio.run(create_new(date_key))
    # Trả về file PDF
    return send_file(pdf_path, as_attachment=True)

@report_bp.route('/create-report/sa/<string:day>', methods=['GET'])
def create_report_afternoon(day):
    
    date_key = day  # Ngày cần truy vấn
    pdf_path = "{date_key}-morning.pdf".format(date_key=date_key)
    a = asyncio.run(create_new_afternoon(date_key))
    # Trả về file PDF
    return send_file(pdf_path, as_attachment=True)

@report_bp.route('/create-report/ch/<string:day>', methods=['GET'])
def create_report_evening(day):
    
    date_key = day  # Ngày cần truy vấn
    pdf_path = "{date_key}-evening.pdf".format(date_key=date_key)
    a = asyncio.run(create_new_evening(date_key))
    # Trả về file PDF
    return send_file(pdf_path, as_attachment=True)
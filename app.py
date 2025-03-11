from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager
from routes.authentication import auth_bp
from routes.create_report import report_bp
from models import db
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

# Khởi tạo SQLAlchemy
db.init_app(app)

# Khởi tạo JWTManager
jwt = JWTManager(app)

# Đăng ký Blueprint
app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(report_bp, url_prefix='/report')

# Tạo database khi chạy ứng dụng
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True,host = "0.0.0.0",port=5678)

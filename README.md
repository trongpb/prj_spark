# prj_spark
Tạo project sử dụng kafka để streaming dữ liệu từ server , dùng spark để xử lý và lưu vào postgres. Dùng superset để tạo dashboard
1/Start hadoop:
-Vào thư mục hadoop chạy lệnh: docker compose up -d
-Kiểm tra :The Namenode UI can be accessed at http://localhost:9870/ and the ResourceManager UI can be accessed at http://localhost:8088/
2/Start postgres:
-Vào thư mục postgres chạy lệnh: docker compose up -d
-Kiểm tra: http://localhost:8380
-Tạo DB postgres:

3/

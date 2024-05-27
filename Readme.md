# Nhóm 7
# Thu thập và xử lý dữ liệu kinh tế

### 1. Miêu tả code
**1.1 download data:**
- get_calendar.py: Dùng để crawl những dữ liệu về lịch cuộc họp của FOMC
- get_data.py: Dùng để tải các dữ liệu văn bản từ những cuộc họp của FOMC
- get_quandl.py: Dùng để tải các dữ liệu về kinh tế của FOMC
**1.2 Xử lý data:**
- Preprocess_data.py: Tiền xử lý các dữ liệu văn bản của cuộc họp FOMC
- Preprocess_quandl.py: Tiền xử lý các dữ liệu kinh tế của FOMC
- Process_nonText.py: Sau khi tiền xử lý dữ liệu kinh tế tiếp theo sẽ xử lý lại các dữ liệu này
**1.3 Huấn luyện mô hình:**
- baseLine.py: Huấn luyện mô hình dựa trên dữ liệu kinh tế
### 2. Pipeline:
- pipe_Line_CK_DE_2.py: Chứa mô hình PipeLine bao gồm việc tải $\RightArrow$ xử lý $\RightArrow$ Huấn luyện mô hình
### 3. fomc_get_data:
- Là tệp chứa các đoạn code hổ trợ tải các văn bản từ cuộc họp FOMC xử lý và dump thành file pickle.


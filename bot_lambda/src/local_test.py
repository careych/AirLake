import os
import time
import requests

# Lấy Token từ máy tính
TOKEN = os.environ.get("TELEGRAM_TOKEN")
if not TOKEN:
    print("Lỗi: Chưa tìm thấy TELEGRAM_TOKEN!")
    exit()

URL = f"https://api.telegram.org/bot{TOKEN}"
print("Bot đang chạy trên máy tính của bạn... Hãy mở Telegram và nhắn tin cho bot!")

last_update_id = 0

while True:
    try:
        # Lên Telegram hỏi xem có tin nhắn mới không
        res = requests.get(f"{URL}/getUpdates?offset={last_update_id}&timeout=10").json()
        
        # Bắt lỗi nếu sai Token
        if not res.get("ok"):
            print("Lỗi từ Telegram:", res)
            time.sleep(5)
            continue
            
        for update in res.get("result", []):
            last_update_id = update["update_id"] + 1
            
            if "message" in update and "text" in update["message"]:
                chat_id = update["message"]["chat"]["id"]
                text = update["message"]["text"].strip()
                print(f"-> Nhận được tin nhắn: {text}")

                # Xử lý lệnh
                if text == '/start':
                    requests.post(f"{URL}/sendMessage", json={
                        "chat_id": chat_id, 
                        "text": "Chào mừng đến với AirLake! Mình đang chạy thử nghiệm trên máy tính của Thịnh."
                    })
                elif text.startswith('/subscribe'):
                    requests.post(f"{URL}/sendMessage", json={
                        "chat_id": chat_id, 
                        "text": "Đăng ký thành công! (Hiện tại dữ liệu chỉ lưu tạm trên RAM, chưa gọi AWS DynamoDB)."
                    })
                else:
                    requests.post(f"{URL}/sendMessage", json={
                        "chat_id": chat_id, 
                        "text": "Lệnh không hợp lệ. Hãy thử /start hoặc /subscribe."
                    })
        time.sleep(1)
    except Exception as e:
        print("Lỗi hệ thống:", e)
        time.sleep(5)
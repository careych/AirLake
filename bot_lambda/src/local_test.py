import os
import time
import requests
import boto3
from botocore.exceptions import ClientError

# Lấy Token từ máy tính
TOKEN = os.environ.get("TELEGRAM_TOKEN")
if not TOKEN:
    print("Lỗi: Chưa tìm thấy TELEGRAM_TOKEN!")
    exit()

URL = f"https://api.telegram.org/bot{TOKEN}"

# Kết nối trực tiếp đến DynamoDB tại vùng Sydney (ap-southeast-2)
dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
table = dynamodb.Table('AirLake_Subscriptions')

print("Bot đang chạy... ĐÃ KẾT NỐI VỚI AWS DYNAMODB TẠI SYDNEY!")
print("Hãy mở Telegram và nhắn tin cho bot!")

last_update_id = 0

while True:
    try:
        # Lên Telegram hỏi xem có tin nhắn mới không
        res = requests.get(f"{URL}/getUpdates?offset={last_update_id}&timeout=10").json()
        
        if not res.get("ok"):
            print("Lỗi từ Telegram:", res)
            time.sleep(5)
            continue
            
        for update in res.get("result", []):
            last_update_id = update["update_id"] + 1
            
            if "message" in update and "text" in update["message"]:
                # Ép kiểu chat_id thành chuỗi (String) để khớp với KeyType của DynamoDB
                chat_id = str(update["message"]["chat"]["id"])
                text = update["message"]["text"].strip()
                print(f"-> Nhận được tin nhắn: {text} từ {chat_id}")

                # Xử lý lệnh
                if text == '/start':
                    requests.post(f"{URL}/sendMessage", json={
                        "chat_id": chat_id, 
                        "text": "Chào mừng đến với AirLake! Bot đã chính thức kết nối với AWS."
                    })
                elif text.startswith('/subscribe'):
                    try:
                        # Đẩy dữ liệu lên AWS DynamoDB, chỉ thêm nếu chat_id chưa tồn tại
                        table.put_item(
                            Item={'chat_id': chat_id},
                            ConditionExpression='attribute_not_exists(chat_id)'
                        )
                        reply_text = "Đăng ký thành công! Dữ liệu của bạn đã được lưu lên đám mây AWS DynamoDB."
                        print(f"[AWS] Đã lưu thành công {chat_id} vào database.")
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                            reply_text = "Bạn đã đăng ký nhận cảnh báo từ trước rồi nhé!"
                            print(f"[AWS] Bỏ qua, {chat_id} đã tồn tại trong database.")
                        else:
                            reply_text = "Có lỗi xảy ra khi kết nối với AWS."
                            print(f"[AWS Error] {e}")
                            
                    requests.post(f"{URL}/sendMessage", json={
                        "chat_id": chat_id, 
                        "text": reply_text
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
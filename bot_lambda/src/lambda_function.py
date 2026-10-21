import json
import os
import urllib.request
import boto3
from botocore.exceptions import ClientError

TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
table = dynamodb.Table('AirLake_Subscriptions')

def send_telegram_message(chat_id, text):
    data = json.dumps({"chat_id": chat_id, "text": text}).encode('utf-8')
    req = urllib.request.Request(TELEGRAM_API_URL, data=data, headers={'Content-Type': 'application/json'})
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"Lỗi gửi tin nhắn: {e}")

def lambda_handler(event, context):
    try:
        # LUỒNG 1: Xử lý cảnh báo từ SQS (do hệ thống SNS gửi tới)
        if 'Records' in event:
            for record in event['Records']:
                body = json.loads(record['body'])
                # Nội dung cảnh báo thật từ SNS nằm trong trường 'Message'
                alert_text = body.get('Message', str(body))

                # Quét lấy toàn bộ ID người dùng đã đăng ký trong DynamoDB
                response = table.scan()
                for item in response.get('Items', []):
                    chat_id = item['chat_id']
                    send_telegram_message(chat_id, f"⚠️ CẢNH BÁO AIRLAKE:\n{alert_text}")
            
            return {"statusCode": 200, "body": "Đã xử lý SQS"}

        # LUỒNG 2: Xử lý chat từ Telegram (qua API Gateway)
        body = json.loads(event.get('body', '{}'))
        if "message" in body and "text" in body["message"]:
            chat_id = str(body["message"]["chat"]["id"])
            text = body["message"]["text"].strip()
            
            if text.startswith('/subscribe'):
                try:
                    table.put_item(Item={'chat_id': chat_id}, ConditionExpression='attribute_not_exists(chat_id)')
                    send_telegram_message(chat_id, "Bạn đã đăng ký nhận cảnh báo thành công!")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                        send_telegram_message(chat_id, "Bạn đã đăng ký rồi!")
            else:
                send_telegram_message(chat_id, "Hệ thống AWS: Gõ /subscribe để nhận cảnh báo.")
                
        return {"statusCode": 200, "body": json.dumps({"status": "OK"})}

    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
        return {"statusCode": 200, "body": "Error"}
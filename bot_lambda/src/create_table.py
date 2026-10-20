import boto3

def create_subscription_table():
    # Khởi tạo kết nối DynamoDB
    dynamodb = boto3.client('dynamodb', region_name='ap-southeast-1')
    
    try:
        response = dynamodb.create_table(
            TableName='AirLake_Subscriptions',
            KeySchema=[
                {'AttributeName': 'chat_id', 'KeyType': 'HASH'}  # Khóa chính
            ],
            AttributeDefinitions=[
                {'AttributeName': 'chat_id', 'AttributeType': 'S'} # Kiểu chuỗi
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("Đang tạo bảng... Trạng thái:", response['TableDescription']['TableStatus'])
    except dynamodb.exceptions.ResourceInUseException:
        print("Bảng AirLake_Subscriptions đã tồn tại!")
    except Exception as e:
        print("Lỗi khi tạo bảng:", e)

if __name__ == '__main__':
    create_subscription_table()
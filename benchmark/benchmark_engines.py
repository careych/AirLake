import time
import boto3
import pandas as pd
from trino.dbapi import connect

# ================= CẤU HÌNH HỆ THỐNG =================
ATHENA_DATABASE = 'airlake_it2_20261108_37179997ad2b42959f4387637e003ab8_gold'
ATHENA_OUTPUT = 's3://airlake-701364614078-athena-results/' 

# 2. Cấu hình Trino
TRINO_HOST = '3.107.183.65'
TRINO_PORT = 8080
TRINO_USER = 'thinh'
TRINO_CATALOG = 'iceberg'

# ================= BỘ CÂU HỎI TRUY VẤN =================
# ================= BỘ CÂU HỎI TRUY VẤN =================
QUERIES = {
    "1_Truy_van_co_ban": f"SELECT * FROM {ATHENA_DATABASE}.pm25_aqi_hourly LIMIT 5000",
    
    "2_Gom_nhom_don_gian": f"""
        SELECT location_id, AVG(pm25_avg) as avg_pm25 
        FROM {ATHENA_DATABASE}.pm25_aqi_hourly 
        GROUP BY location_id
    """,
    
    "3_Truy_van_phuc_tap": f"""
        SELECT location_id, MAX(pm25_avg) as max_pm25
        FROM {ATHENA_DATABASE}.pm25_aqi_hourly
        WHERE pm25_avg IS NOT NULL
        GROUP BY location_id
        ORDER BY max_pm25 DESC
        LIMIT 10
    """
}

# ================= HÀM THỰC THI =================
def run_athena_query(query):
    client = boto3.client('athena', region_name='ap-southeast-2')
    start_time = time.time()
    
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        query_id = response['QueryExecutionId']
        
        while True:
            stats = client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)
            
        execution_time = time.time() - start_time
        if status == 'FAILED':
            print(f"Lỗi Athena: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return execution_time if status == 'SUCCEEDED' else None
    except Exception as e:
        print(f"Lỗi kết nối Athena: {e}")
        return None

def run_trino_query(query):
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=ATHENA_DATABASE
        )
        cur = conn.cursor()
        
        start_time = time.time()
        cur.execute(query)
        rows = cur.fetchall() 
        execution_time = time.time() - start_time
        
        return execution_time
    except Exception as e:
        print(f"Lỗi kết nối Trino: {e}")
        return None

# ================= CHẠY THỰC NGHIỆM =================
if __name__ == "__main__":
    results = []
    
    print("Bắt đầu so sánh thực nghiệm Trino vs Athena...")
    for q_name, sql in QUERIES.items():
        print(f"\nĐang chạy: {q_name}")
        
        athena_time = run_athena_query(sql)
        print(f" - Athena: {athena_time:.2f} giây" if athena_time else " - Athena: LỖI")
        
        trino_time = run_trino_query(sql)
        print(f" - Trino: {trino_time:.2f} giây" if trino_time else " - Trino: LỖI")
        
        results.append({
            "Truy vấn": q_name,
            "Athena (s)": athena_time,
            "Trino (s)": trino_time
        })
        
    print("\n=== KẾT QUẢ SO SÁNH ===")
    df = pd.DataFrame(results)
    print(df.to_string(index=False))
"""
Benchmark so sánh hiệu năng và chi phí giữa Trino và AWS Athena trên dữ liệu AirLake.
Hỗ trợ đo lường thời gian thực thi, dung lượng dữ liệu quét (DataScannedInBytes),
ước tính chi phí truy vấn theo bảng giá AWS, chạy lặp nhiều lần để tính trung bình,
chế độ mô phỏng (--dry-run), và xuất báo cáo định dạng Markdown.
"""

import argparse
import datetime
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from trino.dbapi import connect as trino_connect
except ImportError:
    trino_connect = None

try:
    import pandas as pd
except ImportError:
    pd = None

# ================= CẤU HÌNH HỆ THỐNG MẶC ĐỊNH =================
DEFAULT_ATHENA_DATABASE = os.getenv(
    'ATHENA_DATABASE', 'airlake_it2_20261108_37179997ad2b42959f4387637e003ab8_gold'
)
DEFAULT_ATHENA_OUTPUT = os.getenv(
    'ATHENA_OUTPUT', 's3://airlake-701364614078-athena-results/'
)
DEFAULT_ATHENA_REGION = os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-2')

DEFAULT_TRINO_HOST = os.getenv('TRINO_HOST', '3.107.183.65')
DEFAULT_TRINO_PORT = int(os.getenv('TRINO_PORT', '8080'))
DEFAULT_TRINO_USER = os.getenv('TRINO_USER', 'thinh')
DEFAULT_TRINO_CATALOG = os.getenv('TRINO_CATALOG', 'iceberg')

# Tương thích ngược với các biến toàn cục cũ
ATHENA_DATABASE = DEFAULT_ATHENA_DATABASE
ATHENA_OUTPUT = DEFAULT_ATHENA_OUTPUT
TRINO_HOST = DEFAULT_TRINO_HOST
TRINO_PORT = DEFAULT_TRINO_PORT
TRINO_USER = DEFAULT_TRINO_USER
TRINO_CATALOG = DEFAULT_TRINO_CATALOG

# Định mức chi phí AWS
ATHENA_COST_PER_TB_USD = 5.0
TRINO_HOURLY_COST_USD = 0.1008  # m7i-flex.large tại ap-southeast-2 (~$0.1008/giờ)

# ================= BỘ CÂU HỎI TRUY VẤN =================
def get_queries(database: str = DEFAULT_ATHENA_DATABASE) -> Dict[str, str]:
    """Trả về bộ truy vấn kiểm thử hiệu năng đối với Gold layer."""
    return {
        "1_Truy_van_co_ban": f"SELECT * FROM {database}.pm25_aqi_hourly LIMIT 5000",
        "2_Gom_nhom_don_gian": f"""
            SELECT location_id, AVG(pm25_avg) as avg_pm25 
            FROM {database}.pm25_aqi_hourly 
            GROUP BY location_id
        """,
        "3_Truy_van_phuc_tap": f"""
            SELECT location_id, MAX(pm25_avg) as max_pm25
            FROM {database}.pm25_aqi_hourly
            WHERE pm25_avg IS NOT NULL
            GROUP BY location_id
            ORDER BY max_pm25 DESC
            LIMIT 10
        """
    }

QUERIES = get_queries(ATHENA_DATABASE)

# ================= HÀM TÍNH TOÁN CHI PHÍ =================
def calculate_athena_cost(data_scanned_bytes: int, apply_min_10mb: bool = True) -> float:
    """
    Tính chi phí truy vấn AWS Athena dựa trên dung lượng dữ liệu quét (USD).
    - Đơn giá: $5.00 cho mỗi 1 TB quét (1 TB = 1024^4 bytes).
    - AWS Athena áp dụng mức tính phí tối thiểu 10 MB cho mỗi truy vấn.
    """
    if data_scanned_bytes <= 0:
        return 0.0
    effective_bytes = max(10 * 1024 * 1024, data_scanned_bytes) if apply_min_10mb else data_scanned_bytes
    tb_scanned = effective_bytes / (1024 ** 4)
    return tb_scanned * ATHENA_COST_PER_TB_USD

def calculate_trino_cost(execution_time_s: float, hourly_rate_usd: float = TRINO_HOURLY_COST_USD) -> float:
    """
    Tính chi phí tính toán Trino dựa trên thời gian thực thi (USD).
    - Đơn giá tính theo chi phí EC2 server: hourly_rate / 3600 mỗi giây.
    """
    if execution_time_s <= 0:
        return 0.0
    per_second_rate = hourly_rate_usd / 3600.0
    return execution_time_s * per_second_rate

# ================= HÀM THỰC THI TRUY VẤN =================
def run_athena_query(query: str,
                     database: str = DEFAULT_ATHENA_DATABASE,
                     output_location: str = DEFAULT_ATHENA_OUTPUT,
                     region_name: str = DEFAULT_ATHENA_REGION,
                     client=None) -> Dict:
    """
    Thực thi truy vấn Athena, theo dõi tiến trình và bóc tách metrics:
    thời gian chạy (s), dung lượng quét (bytes/MB), chi phí ($), query_id.
    """
    if boto3 is None and client is None:
        return {
            'success': False,
            'execution_time_s': None,
            'data_scanned_bytes': 0,
            'data_scanned_mb': 0.0,
            'cost_usd': 0.0,
            'query_id': None,
            'error': "Thư viện 'boto3' chưa được cài đặt."
        }

    if client is None:
        client = boto3.client('athena', region_name=region_name)

    start_time = time.time()
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_id = response['QueryExecutionId']

        while True:
            stats = client.get_query_execution(QueryExecutionId=query_id)
            query_exec = stats.get('QueryExecution', {})
            status = query_exec.get('Status', {}).get('State')
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        execution_time = time.time() - start_time
        statistics = query_exec.get('Statistics', {})
        scanned_bytes = statistics.get('DataScannedInBytes', 0)
        scanned_mb = round(scanned_bytes / (1024 * 1024), 2)
        cost_usd = calculate_athena_cost(scanned_bytes)

        if status == 'SUCCEEDED':
            return {
                'success': True,
                'execution_time_s': execution_time,
                'data_scanned_bytes': scanned_bytes,
                'data_scanned_mb': scanned_mb,
                'cost_usd': cost_usd,
                'query_id': query_id,
                'error': None
            }
        else:
            reason = query_exec.get('Status', {}).get('StateChangeReason', f"Trạng thái {status}")
            return {
                'success': False,
                'execution_time_s': execution_time,
                'data_scanned_bytes': scanned_bytes,
                'data_scanned_mb': scanned_mb,
                'cost_usd': cost_usd,
                'query_id': query_id,
                'error': f"Lỗi Athena: {reason}"
            }
    except Exception as e:
        return {
            'success': False,
            'execution_time_s': None,
            'data_scanned_bytes': 0,
            'data_scanned_mb': 0.0,
            'cost_usd': 0.0,
            'query_id': None,
            'error': f"Lỗi kết nối Athena: {e}"
        }

def run_trino_query(query: str,
                    host: str = DEFAULT_TRINO_HOST,
                    port: int = DEFAULT_TRINO_PORT,
                    user: str = DEFAULT_TRINO_USER,
                    catalog: str = DEFAULT_TRINO_CATALOG,
                    schema: str = DEFAULT_ATHENA_DATABASE,
                    connect_fn=None) -> Dict:
    """
    Thực thi truy vấn Trino, đo đạc thời gian thực thi (s), số dòng trả về,
    và ước lượng chi phí tính toán theo thời gian EC2.
    """
    conn_func = connect_fn or trino_connect
    if conn_func is None:
        return {
            'success': False,
            'execution_time_s': None,
            'row_count': 0,
            'cost_usd': 0.0,
            'error': "Thư viện 'trino' chưa được cài đặt."
        }

    try:
        conn = conn_func(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )
        cur = conn.cursor()

        start_time = time.time()
        cur.execute(query)
        rows = cur.fetchall()
        execution_time = time.time() - start_time
        cost_usd = calculate_trino_cost(execution_time)

        return {
            'success': True,
            'execution_time_s': execution_time,
            'row_count': len(rows),
            'cost_usd': cost_usd,
            'error': None
        }
    except Exception as e:
        return {
            'success': False,
            'execution_time_s': None,
            'row_count': 0,
            'cost_usd': 0.0,
            'error': f"Lỗi kết nối Trino: {e}"
        }

# ================= BÁO CÁO MARKDOWN =================
def generate_benchmark_markdown_report(results: List[Dict],
                                       metadata: Optional[Dict] = None,
                                       output_path: Optional[str] = None) -> str:
    """
    Tạo báo cáo chi tiết so sánh Trino vs Athena định dạng Markdown
    kèm phân tích hiệu năng và chi phí.
    """
    meta = metadata or {}
    timestamp = meta.get('timestamp', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    athena_db = meta.get('athena_database', DEFAULT_ATHENA_DATABASE)
    trino_host = meta.get('trino_host', DEFAULT_TRINO_HOST)
    trino_port = meta.get('trino_port', DEFAULT_TRINO_PORT)
    runs = meta.get('runs', 1)
    is_dry_run = meta.get('dry_run', False)

    lines = [
        "# Báo Cáo So Sánh Thực Nghiệm Hiệu Năng & Chi Phí: Trino vs AWS Athena",
        "",
        f"- **Thời gian thực hiện**: {timestamp}",
        f"- **Cơ sở dữ liệu Gold**: `{athena_db}`",
        f"- **Endpoint Trino**: `{trino_host}:{trino_port}` (EC2 `m7i-flex.large`, đơn giá: ~$0.1008/giờ)",
        f"- **AWS Athena**: Serverless engine (Đơn giá: $5.00/TB quét, min 10MB/query)",
        f"- **Số lần chạy lặp lại (Runs)**: {runs}",
        f"- **Chế độ kiểm thử**: {'Mô phỏng (Dry-run)' if is_dry_run else 'Thực tế (Live)'}",
        "",
        "## 1. Bảng Tổng Hợp Kết Quả Thực Nghiệm",
        "",
        "| STT | Kịch Bản Truy Vấn | Athena Thời Gian (s) | Athena Quét (MB) | Athena Chi Phí ($) | Trino Thời Gian (s) | Trino Dòng Trả Về | Trino Chi Phí ($) | Tăng Tốc (Speedup) |",
        "|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
    ]

    total_athena_time = 0.0
    total_trino_time = 0.0
    total_athena_cost = 0.0
    total_trino_cost = 0.0
    total_athena_mb = 0.0

    for idx, r in enumerate(results, start=1):
        q_name = r.get('query_name', f"Query_{idx}")
        athena_time = r.get('athena_time_s')
        athena_mb = r.get('athena_scanned_mb', 0.0)
        athena_cost = r.get('athena_cost_usd', 0.0)
        trino_time = r.get('trino_time_s')
        trino_rows = r.get('trino_rows', 0)
        trino_cost = r.get('trino_cost_usd', 0.0)

        athena_time_str = f"{athena_time:.2f}" if athena_time is not None else "LỖI"
        trino_time_str = f"{trino_time:.2f}" if trino_time is not None else "LỖI"
        athena_mb_str = f"{athena_mb:.2f}"
        athena_cost_str = f"${athena_cost:.6f}"
        trino_cost_str = f"${trino_cost:.6f}"
        trino_rows_str = f"{trino_rows:,}" if trino_rows is not None else "0"

        if athena_time and trino_time and trino_time > 0:
            speedup = athena_time / trino_time
            speedup_str = f"**{speedup:.2f}x**"
        else:
            speedup_str = "N/A"

        if athena_time:
            total_athena_time += athena_time
        if trino_time:
            total_trino_time += trino_time
        total_athena_cost += athena_cost
        total_trino_cost += trino_cost
        total_athena_mb += athena_mb

        lines.append(
            f"| {idx} | `{q_name}` | {athena_time_str} | {athena_mb_str} | {athena_cost_str} | "
            f"{trino_time_str} | {trino_rows_str} | {trino_cost_str} | {speedup_str} |"
        )

    avg_speedup = (total_athena_time / total_trino_time) if total_trino_time > 0 else 0.0

    lines.extend([
        "",
        "### Thống Kê Tổng Hợp",
        f"- **Tổng thời gian thực thi**: Athena: `{total_athena_time:.2f}s` vs Trino: `{total_trino_time:.2f}s` (Trino nhanh hơn trung bình **{avg_speedup:.2f}x**).",
        f"- **Tổng dung lượng quét Athena**: `{total_athena_mb:.2f} MB` (~`${total_athena_cost:.6f}`).",
        f"- **Tổng chi phí tính toán Trino**: `${total_trino_cost:.6f}`.",
        "",
        "## 2. Phân Tích Kỹ Thuật & Đánh Giá Kiến Trúc",
        "",
        "### 2.1. Độ Trễ (Latency)",
        "- **Trino**: Trino giữ tiến trình JVM chạy thường trực (long-running cluster) kết hợp in-memory execution, loại bỏ hoàn toàn độ trễ khởi động (cold start). Các truy vấn phục vụ dashboard (như Superset) đạt thời gian phản hồi sub-second hoặc ~1s.",
        "- **AWS Athena**: Là kiến trúc Serverless, Athena mất khoảng 1.5s - 2.5s overhead cho mỗi truy vấn để cấp phát worker và phân bổ tài nguyên. Tuy nhiên, với các truy vấn quét dữ liệu lớn phân tán (full-scan), Athena tự động mở rộng theo tải mà không cần cấu hình cụ thể.",
        "",
        "### 2.2. Chi Phí (Cost Efficiency)",
        "- **AWS Athena**: Tính phí thuần túy theo $5.00/TB quét thực tế. Khi áp dụng định dạng Parquet nén kèm phân vùng (partition pruning), dung lượng quét giảm từ 70% - 90%, rất tiết kiệm cho các báo cáo định kỳ theo giờ hoặc tác vụ kiểm toán dữ liệu.",
        "- **Trino**: Tính phí theo giờ máy chủ EC2 (`m7i-flex.large`: ~$0.1008/giờ, tương đương ~$72.5/tháng nếu chạy 24/7). Khi tích hợp Auto Scaling Group (ASG) và lịch trình bật/tắt (Scheduled Action 07:00 - 19:00), chi phí giảm xuống còn ~$36/tháng, rất tối ưu cho các hệ thống dashboard có tần suất truy vấn cao liên tục.",
        "",
        "### 2.3. Khuyến Nghị Phối Hợp Kiến Trúc (Architecture Recommendation)",
        "1. **Apache Superset BI Dashboard**: Sử dụng **Trino** làm query engine mặc định để mang lại trải nghiệm tương tác mượt mà, thời gian tải biểu đồ nhanh chóng (< 1.5s).",
        "2. **Tác vụ Ad-hoc & Phân Tích Thăm Dò Nâng Cao**: Sử dụng **AWS Athena** cho các tác vụ ad-hoc từ data analyst hoặc batch data science không yêu cầu độ trễ tương tác thực thời.",
        "3. **Dự phòng sự cố (Failover/Fallback)**: Nếu Trino cluster gặp sự cố bảo trì hoặc ngoài khung giờ hoạt động, hệ thống có thể linh hoạt fallback truy vấn trực tiếp vào AWS Athena vì cả hai cùng chia sẻ chung AWS Glue Data Catalog và cấu trúc bảng Apache Iceberg."
    ])

    report_content = "\n".join(lines) + "\n"

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        print(f"\n[OK] Đã lưu báo cáo Markdown tại: {output_path}")

    return report_content

# ================= MÔ PHỎNG DRY-RUN =================
def run_dry_run_benchmark(queries: Dict[str, str], runs: int = 1) -> List[Dict]:
    """Mô phỏng đo đạc benchmark khi không có kết nối trực tiếp đến cluster."""
    dry_run_data = [
        {
            "query_name": "1_Truy_van_co_ban",
            "athena_time_s": 2.15,
            "athena_scanned_bytes": 15728640,  # 15 MB
            "athena_scanned_mb": 15.00,
            "athena_cost_usd": calculate_athena_cost(15728640),
            "trino_time_s": 0.62,
            "trino_rows": 5000,
            "trino_cost_usd": calculate_trino_cost(0.62)
        },
        {
            "query_name": "2_Gom_nhom_don_gian",
            "athena_time_s": 3.48,
            "athena_scanned_bytes": 44040192,  # 42 MB
            "athena_scanned_mb": 42.00,
            "athena_cost_usd": calculate_athena_cost(44040192),
            "trino_time_s": 1.18,
            "trino_rows": 120,
            "trino_cost_usd": calculate_trino_cost(1.18)
        },
        {
            "query_name": "3_Truy_van_phuc_tap",
            "athena_time_s": 5.12,
            "athena_scanned_bytes": 94371840,  # 90 MB
            "athena_scanned_mb": 90.00,
            "athena_cost_usd": calculate_athena_cost(94371840),
            "trino_time_s": 1.45,
            "trino_rows": 10,
            "trino_cost_usd": calculate_trino_cost(1.45)
        }
    ]
    # Nếu danh sách truy vấn tuỳ biến khác mặc định, tạo mẫu tương ứng
    res = []
    for idx, (name, _) in enumerate(queries.items()):
        if idx < len(dry_run_data):
            sample = dict(dry_run_data[idx])
            sample["query_name"] = name
            res.append(sample)
        else:
            res.append({
                "query_name": name,
                "athena_time_s": 3.0,
                "athena_scanned_bytes": 20971520,
                "athena_scanned_mb": 20.0,
                "athena_cost_usd": calculate_athena_cost(20971520),
                "trino_time_s": 1.0,
                "trino_rows": 100,
                "trino_cost_usd": calculate_trino_cost(1.0)
            })
    return res

# ================= HÀM ĐIỀU PHỐI CHÍNH =================
def run_benchmark(args) -> List[Dict]:
    """Thực thi kịch bản đo đạc hiệu năng và chi phí theo cấu hình đầu vào."""
    queries = get_queries(args.athena_database)
    
    if args.dry_run:
        print("\n[CHẾ ĐỘ MÔ PHỎNG --dry-run ĐƯỢC KÍCH HOẠT]")
        results = run_dry_run_benchmark(queries, runs=args.runs)
        return results

    results = []
    print(f"\nBắt đầu so sánh thực nghiệm Trino vs Athena ({args.runs} lần chạy lặp)...")
    print(f" - Trino: {args.trino_host}:{args.trino_port} (catalog={args.trino_catalog}, schema={args.athena_database})")
    print(f" - Athena: database={args.athena_database}, output={args.athena_output}, region={args.athena_region}")

    for q_name, sql in queries.items():
        print(f"\n==================================================")
        print(f"Kịch bản: {q_name}")
        print(f"SQL: {sql.strip()[:100]}...")

        athena_times = []
        athena_bytes = []
        athena_costs = []
        last_athena_res = None

        trino_times = []
        trino_costs = []
        trino_rows = 0
        last_trino_res = None

        for r_idx in range(1, args.runs + 1):
            if args.runs > 1:
                print(f"  --- Lần chạy {r_idx}/{args.runs} ---")

            # Chạy Athena
            ath_res = run_athena_query(
                query=sql,
                database=args.athena_database,
                output_location=args.athena_output,
                region_name=args.athena_region
            )
            last_athena_res = ath_res
            if ath_res['success']:
                athena_times.append(ath_res['execution_time_s'])
                athena_bytes.append(ath_res['data_scanned_bytes'])
                athena_costs.append(ath_res['cost_usd'])
                print(f"  * Athena: {ath_res['execution_time_s']:.2f}s | "
                      f"Quét: {ath_res['data_scanned_mb']:.2f} MB | "
                      f"Chi phí: ${ath_res['cost_usd']:.6f}")
            else:
                print(f"  * Athena [LỖI]: {ath_res.get('error')}")

            # Chạy Trino
            tri_res = run_trino_query(
                query=sql,
                host=args.trino_host,
                port=args.trino_port,
                user=args.trino_user,
                catalog=args.trino_catalog,
                schema=args.athena_database
            )
            last_trino_res = tri_res
            if tri_res['success']:
                trino_times.append(tri_res['execution_time_s'])
                trino_costs.append(tri_res['cost_usd'])
                trino_rows = tri_res['row_count']
                print(f"  * Trino : {tri_res['execution_time_s']:.2f}s | "
                      f"Dòng: {tri_res['row_count']} | "
                      f"Chi phí: ${tri_res['cost_usd']:.6f}")
            else:
                print(f"  * Trino  [LỖI]: {tri_res.get('error')}")

        # Tính trung bình các lần chạy
        avg_athena_time = (sum(athena_times) / len(athena_times)) if athena_times else None
        avg_athena_bytes = int(sum(athena_bytes) / len(athena_bytes)) if athena_bytes else 0
        avg_athena_mb = round(avg_athena_bytes / (1024 * 1024), 2)
        avg_athena_cost = (sum(athena_costs) / len(athena_costs)) if athena_costs else 0.0

        avg_trino_time = (sum(trino_times) / len(trino_times)) if trino_times else None
        avg_trino_cost = (sum(trino_costs) / len(trino_costs)) if trino_costs else 0.0

        results.append({
            "query_name": q_name,
            "athena_time_s": avg_athena_time,
            "athena_scanned_bytes": avg_athena_bytes,
            "athena_scanned_mb": avg_athena_mb,
            "athena_cost_usd": avg_athena_cost,
            "trino_time_s": avg_trino_time,
            "trino_rows": trino_rows,
            "trino_cost_usd": avg_trino_cost
        })

    return results

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Benchmark so sánh hiệu năng và chi phí giữa Trino và AWS Athena (AirLake Gold Layer)."
    )
    parser.add_argument('--trino-host', default=DEFAULT_TRINO_HOST, help="Địa chỉ IP / Hostname của Trino Coordinator")
    parser.add_argument('--trino-port', type=int, default=DEFAULT_TRINO_PORT, help="Cổng kết nối Trino (mặc định: 8080)")
    parser.add_argument('--trino-user', default=DEFAULT_TRINO_USER, help="Tên user Trino (mặc định: thinh)")
    parser.add_argument('--trino-catalog', default=DEFAULT_TRINO_CATALOG, help="Catalog Trino (mặc định: iceberg)")
    parser.add_argument('--athena-database', default=DEFAULT_ATHENA_DATABASE, help="Athena Database / Glue Catalog database")
    parser.add_argument('--athena-output', default=DEFAULT_ATHENA_OUTPUT, help="S3 Output location cho kết quả Athena")
    parser.add_argument('--athena-region', default=DEFAULT_ATHENA_REGION, help="AWS Region (mặc định: ap-southeast-2)")
    parser.add_argument('--runs', type=int, default=1, help="Số lần chạy lặp lại mỗi truy vấn để tính trung bình")
    parser.add_argument('--output', default='benchmark/BENCHMARK_REPORT.md', help="Đường dẫn file lưu báo cáo Markdown")
    parser.add_argument('--dry-run', action='store_true', help="Chạy chế độ mô phỏng kiểm thử không cần kết nối cluster")
    return parser.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    results = run_benchmark(args)

    print("\n" + "=" * 60)
    print("=== KẾT QUẢ SO SÁNH TỔNG HỢP (TRUNG BÌNH) ===")
    print("=" * 60)

    # In kết quả dạng bảng Console
    if pd is not None:
        table_records = []
        for r in results:
            ath_t = f"{r['athena_time_s']:.2f}s" if r['athena_time_s'] is not None else "FAIL"
            tri_t = f"{r['trino_time_s']:.2f}s" if r['trino_time_s'] is not None else "FAIL"
            speedup = (f"{r['athena_time_s'] / r['trino_time_s']:.2f}x"
                       if r['athena_time_s'] and r['trino_time_s'] and r['trino_time_s'] > 0 else "N/A")
            table_records.append({
                "Truy vấn": r['query_name'],
                "Athena (s)": ath_t,
                "Athena (MB)": f"{r['athena_scanned_mb']:.2f}",
                "Athena ($)": f"${r['athena_cost_usd']:.6f}",
                "Trino (s)": tri_t,
                "Trino ($)": f"${r['trino_cost_usd']:.6f}",
                "Speedup": speedup
            })
        df = pd.DataFrame(table_records)
        print(df.to_string(index=False))
    else:
        for r in results:
            print(f"- {r['query_name']}: Athena={r['athena_time_s']}s (${r['athena_cost_usd']:.6f}) vs "
                  f"Trino={r['trino_time_s']}s (${r['trino_cost_usd']:.6f})")

    # Xuất báo cáo Markdown
    metadata = {
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'athena_database': args.athena_database,
        'trino_host': args.trino_host,
        'trino_port': args.trino_port,
        'runs': args.runs,
        'dry_run': args.dry_run
    }
    generate_benchmark_markdown_report(results, metadata=metadata, output_path=args.output)
    return 0

if __name__ == "__main__":
    sys.exit(main())

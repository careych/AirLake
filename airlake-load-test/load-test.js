import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Định nghĩa custom metric để theo dõi tỷ lệ lỗi
const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '30s', target: 10 }, // Ramp-up: Tăng từ 0 lên 10 Virtual Users (VUs) trong 30 giây
    { duration: '1m', target: 50 },  // Steady state: Duy trì 50 VUs trong 1 phút để test tải
    { duration: '30s', target: 0 },  // Ramp-down: Giảm tải về 0 VUs
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% số lượng request phải phản hồi dưới 500ms
    errors: ['rate<0.2'],            // Cho phép tỷ lệ lỗi nằm trong tầm kiểm soát dưới 20%
  },
};

// Thay đổi URL mục tiêu thành ALB hoặc API Gateway của bạn
const BASE_URL = __ENV.TARGET_URL || 'http://your-alb-dns-name.amazonaws.com';

export default function () {
  // --- KỊCH BẢN 1: Gửi request hợp lệ (Kiểm tra hiệu năng thông thường) ---

  // --- KỊCH BẢN 2: Gây lỗi chủ động (Error Injection) ---
  // Cố tình gửi payload sai định dạng hoặc dữ liệu rác để test cơ chế validation và xử lý lỗi của hệ thống
  let invalidPayload = JSON.stringify({
    station_id: "INVALID_STATION_XYZ",
    pm25: -999.99, // Dữ liệu cảm biến không hợp lệ
    timestamp: "not-a-valid-date"
  });

  let params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  let resError = http.post(`${BASE_URL}/AirLake_Bot_API`, invalidPayload, params);
    console.log(`Mã trạng thái trả về: ${resError.status}`);
// Kỳ vọng hệ thống phải bắt được lỗi...
  let checkErrorHandling = check(resError, {
    'error handled correctly (4xx/5xx)': (r) => r.status === 400 || r.status === 422 || r.status === 500,
  });
  
  // Nếu hệ thống trả về đúng mã lỗi chủ động thì tính là pass bài kiểm tra xử lý lỗi
  errorRate.add(!checkErrorHandling);

  sleep(1); // Nghỉ 1 giây giữa các request của mỗi VU
}
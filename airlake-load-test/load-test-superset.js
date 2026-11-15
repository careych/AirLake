import http from 'k6/http';
import { check, sleep } from 'k6';

// Copy đoạn DNS name của Superset ALB trên AWS dán vào đây
const BASE_URL = __ENV.TARGET_URL || 'http://airlake-superset-alb-XXXXXX.ap-southeast-2.elb.amazonaws.com';

export const options = {
    stages: [
        { duration: '30s', target: 50 },  // Tăng dần lên 50 user trong 30s
        { duration: '1m', target: 100 },  // Duy trì 100 user liên tục trong 1 phút
        { duration: '30s', target: 0 },   // Giảm dần về 0
    ],
    thresholds: {
        http_req_duration: ['p(95)<1000'], // 95% request phải phản hồi dưới 1 giây
        http_req_failed: ['rate<0.01'],    // Tỷ lệ lỗi (VD: 503 Service Unavailable) phải dưới 1%
    },
};

export default function () {
    // Gửi request GET vào đường dẫn health (hoặc đường dẫn trang login /login/)
    let res = http.get(`${BASE_URL}/health`); 
    
    check(res, {
        'status is 200': (r) => r.status === 200,
    });
    
    sleep(1); 
}
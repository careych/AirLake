import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.TARGET_URL || 'https://rsvjj5fff3.execute-api.ap-southeast-2.amazonaws.com/default/AirLake_Bot_API';

export default function () {
    const params = { headers: { 'Content-Type': 'application/json' } };

    // Trường hợp 1: Cảm biến gửi THỪA trường dữ liệu (thêm nhiệt độ, độ ẩm)
    const payloadExtra = JSON.stringify({
        station_id: "ST-01",
        timestamp: "2026-11-04T10:00:00Z",
        pm25: 45.5,
        temperature: 30.2, 
        humidity: 75       
    });

    // Trường hợp 2: Cảm biến gửi THIẾU trường dữ liệu (mất ID trạm)
    const payloadMissing = JSON.stringify({
        timestamp: "2026-11-04T10:05:00Z",
        pm25: 50.1
    });

    // Bắn thử Trường hợp 1
    let resExtra = http.post(BASE_URL, payloadExtra, params);
    check(resExtra, { 'Xử lý tốt dữ liệu thừa (200)': (r) => r.status === 200 });

    // Bắn thử Trường hợp 2
    let resMissing = http.post(BASE_URL, payloadMissing, params);
    check(resMissing, { 'Xử lý tốt dữ liệu thiếu (200)': (r) => r.status === 200 });

    sleep(1);
}
"""Vietnamese bot texts and inline keyboards. No internal terms, no safety promises."""

from datetime import timedelta

from ingestion.openaq import utc

SITE = "https://public.dhyvgpdsnj4zi.amplifyapp.com/"
VIETNAM = timedelta(hours=7)
STATIONS = {
    6123215: "Vinhomes Ocean Park, Gia Lâm (cảm biến Dragon)",
    99385: "Khu vực Tây Hồ (cảm biến cộng đồng #99385)",
}
THRESHOLDS = (25, 35, 50, 75, 100)


def vn_time(value):
    t = utc(value) + VIETNAM
    return t.strftime("%H:%M %d/%m")


def age(value, now):
    minutes = int((utc(now) - utc(value)).total_seconds() // 60)
    return (
        f"{minutes} phút trước"
        if minutes < 60
        else f"{minutes // 60} giờ {minutes % 60} phút trước"
    )


def number(v):
    return f"{v:.1f}".replace(".", ",")


def keyboard(rows):
    return {
        "inline_keyboard": [
            [{"text": t, "callback_data": d} for t, d in row] for row in rows
        ]
    }


MAIN_MENU = keyboard(
    [
        [("🔔 Đăng ký cảnh báo", "sub"), ("📍 Số đo mới nhất", "latest")],
        [("🕘 Giờ thường ít bụi hơn", "hours"), ("⚙️ Cảnh báo của tôi", "status")],
    ]
)


def welcome():
    return (
        "Chào bạn! Đây là bot AirLake, theo dõi bụi mịn PM2.5 ở Hà Nội.\n\n"
        "Bạn có thể:\n"
        "• xem số đo mới nhất tại các trạm AirLake được phép chia sẻ,\n"
        "• nhận tin nhắn khi bụi mịn tại trạm bạn chọn vượt ngưỡng bạn đặt.\n\n"
        "Hiện AirLake có 2 điểm đo, chưa phủ toàn thành phố. Chọn một mục bên dưới để bắt đầu."
    )


HELP = (
    "Các lệnh:\n"
    "/latest – số đo mới nhất\n"
    "/subscribe – đăng ký cảnh báo\n"
    "/status – xem cảnh báo bạn đã đăng ký\n"
    "/settings – đổi trạm, ngưỡng hoặc tạm dừng\n"
    "/hours – giờ thường ít bụi hơn (theo dữ liệu đã có)\n"
    "/unsubscribe – hủy cảnh báo\n\n"
    "Bot chỉ nhắn khi số đo thật vượt ngưỡng; không dùng dự báo để cảnh báo."
)


def choose_station():
    return "Bạn muốn theo dõi trạm nào?", keyboard(
        [[(name, f"st:{sid}")] for sid, name in STATIONS.items()]
    )


def choose_threshold(station_id):
    return (
        (
            f"Trạm: {STATIONS[station_id]}\n\nNhắn cho bạn khi PM2.5 đạt từ bao nhiêu µg/m³?\n"
            "Để tham khảo: WHO khuyến nghị trung bình 24 giờ không quá 15 µg/m³."
        ),
        keyboard(
            [
                [(f"{t}", f"th:{station_id}:{t}") for t in THRESHOLDS],
                [("Nhập số khác", f"custom:{station_id}")],
            ]
        ),
    )


def confirm(station_id, threshold):
    return (
        (
            f"Xác nhận đăng ký:\nTrạm: {STATIONS[station_id]}\nNgưỡng: {threshold} µg/m³\n\n"
            "Bot sẽ nhắn một lần khi bụi vượt ngưỡng và chỉ nhắn lại sau khi mức bụi đã giảm xuống."
        ),
        keyboard(
            [[("✅ Xác nhận", f"ok:{station_id}:{threshold}"), ("Hủy", "cancel")]]
        ),
    )


def subscribed(station_id, threshold):
    return f"Đã bật cảnh báo cho {STATIONS[station_id]} ở ngưỡng {threshold} µg/m³. Xem lại bất cứ lúc nào bằng /status."


def reading_line(o, now):
    if o.get("observed_at") is None or o.get("pm25") is None:
        return f"• {STATIONS[o['station_id']]}: chưa có số đo."
    old = "" if o.get("freshness", {}).get("status") == "recent" else " (số đo đã cũ)"
    return f"• {STATIONS[o['station_id']]}: {number(o['pm25'])} µg/m³, đo lúc {vn_time(o['observed_at'])} ({age(o['observed_at'], now)}){old}"


def latest(projection, now):
    lines = ["Số đo mới nhất (PM2.5):"]
    lines += [reading_line(o, now) for o in projection["observations"]]
    m = projection["model"]["current"]
    lines += [
        "",
        f"Mô hình CAMS ước tính cho cả vùng trung tâm (~45 km): {number(m['pm25'])} µg/m³ lúc {vn_time(m['valid_at'])}. Đây là ước tính, không phải số đo.",
        "",
        f"Chi tiết: {SITE}",
    ]
    return "\n".join(lines)


def status(sub, projection, now):
    if not sub:
        return "Bạn chưa đăng ký cảnh báo. Gõ /subscribe để bắt đầu.", None
    obs = next(
        (o for o in projection["observations"] if o["station_id"] == sub["station_id"]),
        None,
    )
    state = "đang bật" if sub["active"] else "đang tạm dừng"
    last = sub.get("state", {}).get("last_alert_at")
    text = (
        f"Cảnh báo của bạn ({state}):\nTrạm: {STATIONS[sub['station_id']]}\nNgưỡng: {sub['threshold']} µg/m³\n"
        f"Tin cảnh báo gần nhất: {vn_time(last) if last else 'chưa có'}\n\n"
        + (reading_line(obs, now) if obs else "")
    )
    return text, settings_menu(sub)


def settings_menu(sub):
    toggle = ("⏸ Tạm dừng", "pause") if sub["active"] else ("▶️ Bật lại", "resume")
    return keyboard(
        [
            [("Đổi trạm", "sub"), ("Đổi ngưỡng", f"st:{sub['station_id']}")],
            [toggle, ("Hủy cảnh báo", "unsub")],
        ]
    )


def unsubscribe_confirm():
    return "Bạn chắc chắn muốn hủy cảnh báo? Thông tin đăng ký sẽ bị xóa.", keyboard(
        [[("Hủy cảnh báo", "unsub_ok"), ("Giữ lại", "cancel")]]
    )


def alert(station_id, value, threshold, observed_at, now):
    return (
        "⚠️ Bụi mịn tại trạm bạn theo dõi đang cao\n\n"
        f"Trạm: {STATIONS[station_id]}\n"
        f"PM2.5: {number(value)} µg/m³\n"
        f"Ngưỡng bạn chọn: {threshold} µg/m³\n"
        f"Đo lúc: {vn_time(observed_at)} ({age(observed_at, now)})\n\n"
        "Số đo thật tại trạm đã đạt ngưỡng bạn đăng ký. Bạn có thể cân nhắc giảm hoạt động gắng sức ngoài trời.\n\n"
        f"Xem chi tiết: {SITE}\n"
        "Đổi cài đặt: /settings · Tắt cảnh báo: /unsubscribe"
    )


def hours(pattern):
    rows = sorted(
        (h for h in pattern["hours"] if h["pm25"] is not None), key=lambda h: h["pm25"]
    )
    fmt = lambda h: f"{h['hour']:02d}:00 (~{number(h['pm25'])} µg/m³)"
    return (
        f"Theo số đo tại {STATIONS[pattern['station_id']]} trong {pattern['period']}:\n"
        f"• Thường ít bụi hơn: {', '.join(fmt(h) for h in rows[:3])}\n"
        f"• Thường nhiều bụi hơn: {', '.join(fmt(h) for h in rows[-3:])}\n\n"
        "Đây là trung bình của dữ liệu đã có ở một trạm, không phải dự báo cho hôm nay. Dự báo xem tại: "
        + SITE
    )

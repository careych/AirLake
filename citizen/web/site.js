'use strict';
(() => {
  const cfg = window.AIRLAKE_CONFIG || {};
  const API = typeof cfg.apiBase === 'string' && /^https:\/\/[a-z0-9]+\.lambda-url\.ap-southeast-2\.on\.aws$/.test(cfg.apiBase) ? cfg.apiBase : null;
  const BOT = typeof cfg.botUrl === 'string' && /^https:\/\/t\.me\/[A-Za-z0-9_]{5,32}$/.test(cfg.botUrl) ? cfg.botUrl : null;
  const H = 3600000, D = 86400000, LOCAL = 7 * H; // Vietnam: UTC+7, no daylight saving time
  const FIRST = Date.parse('2026-08-31T17:00:00Z'); // 2026-09-01 00:00 Vietnam time
  const NEAR_KM = 4; // neighbourhood scale (0.5-4 km) of US EPA monitor siting
  const HANOI = {s: 20.56, n: 21.39, w: 105.28, e: 106.02};
  const ORDER = [6123215, 99385];
  const STATIONS = {
    6123215: {id: 6123215, name: 'Gia Lâm · Ocean Park', area: 'xã Gia Lâm', lat: 20.9933, lon: 105.9441, exact: true, hourly: true,
      sensor: 'Cảm biến giá rẻ AirGradient (tên trạm “Dragon”), số liệu qua OpenAQ', licence: 'CC BY 4.0', credit: 'AirGradient via OpenAQ', short: 'Gia Lâm'},
    99385: {id: 99385, name: 'Tây Hồ · cảm biến cộng đồng', area: 'phường Tây Hồ', lat: 21.070, lon: 105.824, exact: false, hourly: false,
      sensor: 'Cảm biến giá rẻ SDS011 của mạng Sensor.Community, vị trí do mạng làm tròn', licence: 'ODbL 1.0', credit: 'Contains Sensor.Community data', short: 'Tây Hồ'}
  };
  const POLICY = {6123215: [5400, 10800], 99385: [1500, 3600]};
  const STATE_TEXT = {fresh: 'Mới', delayed: 'Chậm cập nhật', stale: 'Dữ liệu cũ', none: 'Chưa có dữ liệu'};
  const S = {now: null, nowAt: 0, areas: [], place: null, focus: null, picking: false, maps: {}, cache: new Map(), h: {station: null, range: '7d'}};

  // ---- small helpers ------------------------------------------------------
  const $ = id => document.getElementById(id);
  function el(tag, attrs = {}, ...kids) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === 'class') n.className = v; else if (k === 'text') n.textContent = v; else if (k.startsWith('on')) n.addEventListener(k.slice(2), v); else n.setAttribute(k, v === true ? '' : v);
    }
    for (const c of kids.flat()) if (c !== null && c !== undefined && c !== false) n.append(c.nodeType ? c : document.createTextNode(String(c)));
    return n;
  }
  function svg(tag, attrs = {}, text) {
    const n = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
    if (text !== undefined) n.textContent = text;
    return n;
  }
  const pad = n => String(n).padStart(2, '0');
  const num = (v, d = 1) => Number.isFinite(v) ? new Intl.NumberFormat('vi-VN', {maximumFractionDigits: d, minimumFractionDigits: 0}).format(v) : '—';
  const local = ms => new Date(ms + LOCAL);
  const hm = ms => { const d = local(ms); return pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()); };
  const dm = ms => { const d = local(ms); return pad(d.getUTCDate()) + '/' + pad(d.getUTCMonth() + 1); };
  const dmy = ms => dm(ms) + '/' + local(ms).getUTCFullYear();
  const when = ms => hm(ms) + ', ' + dm(ms);
  const dateKey = ms => local(ms).toISOString().slice(0, 10);
  const midnight = key => Date.parse(key + 'T00:00:00Z') - LOCAL;
  const iso = ms => new Date(ms).toISOString().slice(0, 13) + ':00:00Z';
  const floorHour = ms => Math.floor(ms / H) * H;
  const fold = s => s.normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/đ/g, 'd').replace(/Đ/g, 'D').toLowerCase();
  const store = {get(k) { try { return JSON.parse(localStorage.getItem(k)); } catch { return null; } }, set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* private mode */ } }};
  function km(a, b) {
    const r = Math.PI / 180, x = Math.sin((b.lat - a.lat) * r / 2) ** 2 + Math.cos(a.lat * r) * Math.cos(b.lat * r) * Math.sin((b.lon - a.lon) * r / 2) ** 2;
    return 12742 * Math.asin(Math.sqrt(x));
  }
  const distText = (d, exact) => (exact ? 'cách khoảng ' : 'cách khoảng ') + (d < 1 ? 'dưới 1 km' : num(d, 0) + ' km') + (exact ? '' : ' (vị trí cảm biến gần đúng)');
  function notice(text) { const n = $('notice'); n.textContent = text || ''; n.hidden = !text; }

  async function getJSON(url, ttl = 0) {
    const hit = S.cache.get(url);
    if (hit && Date.now() - hit.at < ttl) return hit.value;
    const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), 15000);
    try {
      const r = await fetch(url, {cache: 'no-cache', credentials: 'omit', redirect: 'error', signal: ctl.signal});
      if (!r.ok) throw Error('http ' + r.status);
      const value = await r.json();
      S.cache.set(url, {at: Date.now(), value});
      return value;
    } finally { clearTimeout(timer); }
  }
  async function history(sid, start, end, granularity = 'hourly') {
    if (!API) throw Error('no api');
    let url = `${API}/v1/history?station_id=${sid}&start=${iso(start)}&end=${iso(end)}&granularity=${granularity}`;
    const first = await getJSON(url, 300000), rows = [...first.rows];
    let next = first.next, pages = 1;
    while (next && pages < 4) { const more = await getJSON(API + next, 300000); rows.push(...more.rows); next = more.next; pages++; }
    return {...first, rows};
  }

  // ---- live projection ----------------------------------------------------
  const obs = id => S.now?.observations?.find(o => o.station_id === id) || null;
  function station(id) {
    const o = obs(id), s = STATIONS[id];
    return o && Number.isFinite(o.latitude) ? {...s, lat: o.latitude, lon: o.longitude} : s;
  }
  function freshness(id, o = obs(id), now = Date.now()) {
    if (!o || !o.observed_at || !Number.isFinite(o.pm25)) return {state: 'none', text: 'Chưa có số đo hợp lệ từ điểm này.'};
    const age = (now - Date.parse(o.observed_at)) / 1000, [fresh, stale] = [o.freshness?.fresh_seconds || POLICY[id][0], o.freshness?.stale_seconds || POLICY[id][1]];
    if (!Number.isFinite(age) || age < -300) return {state: 'none', text: 'Thời điểm đo không hợp lệ nên số này không được dùng.'};
    const ago = age < 3600 ? `${Math.max(0, Math.round(age / 60))} phút` : age < 2 * D / 1000 ? `${Math.floor(age / 3600)} giờ ${Math.round(age % 3600 / 60)} phút` : `${Math.floor(age / 86400)} ngày`;
    if (age <= fresh) return {state: 'fresh', text: `Số đo cách đây ${ago}, đúng nhịp gửi của điểm đo.`};
    if (age <= stale) return {state: 'delayed', text: `Số đo cách đây ${ago}; điểm đo gửi chậm hơn thường lệ.`};
    return {state: 'stale', text: `Số đo cách đây ${ago}, đã quá cũ để nói về hiện tại. Điểm đo có thể đang tạm mất kết nối.`};
  }
  function measured(id, o = obs(id)) {
    if (!o?.observed_at) return 'Chưa có số đo';
    const t = Date.parse(o.observed_at);
    return STATIONS[id].hourly ? `Trung bình ${hm(t - H)}–${hm(t)}, ${dm(t)}` : `Đo lúc ${when(t)}`;
  }
  const badge = f => el('span', {class: 'badge ' + f.state, text: STATE_TEXT[f.state]});

  async function loadNow() {
    if (!cfg.nowUrl) return;
    try {
      S.now = await getJSON(cfg.nowUrl);
      S.nowAt = Date.now();
      const age = Date.now() - Date.parse(S.now.published_at);
      notice(age > 45 * 60000 ? `AirLake chưa cập nhật được từ ${when(Date.parse(S.now.published_at))}. Các số dưới đây có thể đã cũ.` : '');
    } catch {
      notice(S.now ? 'Không tải lại được dữ liệu mới; đang hiển thị lần tải trước.' : 'Chưa tải được dữ liệu hiện tại. Vui lòng thử lại sau ít phút.');
    }
    render();
  }

  // ---- location -----------------------------------------------------------
  function nearestArea(p) {
    let best = null, bd = Infinity;
    for (const a of S.areas) { const d = km(p, a); if (d < bd) { bd = d; best = a; } }
    return best;
  }
  const areaName = a => (a.kind === 'phường' ? 'Phường ' : 'Xã ') + a.name;
  function setPlace(place, save = true) {
    S.place = place; S.focus = null;
    if (save) store.set('airlake.place.v1', place.type === 'area' ? {type: 'area', osm: place.osm} : {type: 'pin', lat: place.lat, lon: place.lon, how: place.how});
    if (!S.h.station) S.h.station = nearestStations()[0].id;
    $('locate-status').textContent = '';
    render();
  }
  function pin(lat, lon, how) {
    const p = {lat: Math.round(lat * 100) / 100, lon: Math.round(lon * 100) / 100};
    if (p.lat < HANOI.s || p.lat > HANOI.n || p.lon < HANOI.w || p.lon > HANOI.e) {
      $('locate-status').textContent = 'Vị trí này nằm ngoài Hà Nội. AirLake hiện chỉ có dữ liệu cho Hà Nội.';
      return;
    }
    setPlace({type: 'pin', how, ...p, near: nearestArea(p)});
  }
  function initSearch() {
    const input = $('area-input'), list = $('area-list');
    let items = [], active = -1;
    const close = () => { list.hidden = true; input.setAttribute('aria-expanded', 'false'); active = -1; };
    const choose = a => { input.value = areaName(a); close(); setPlace({type: 'area', ...a}); };
    function show() {
      const q = fold(input.value.trim().replace(/^(phuong|xa)\s+/i, ''));
      items = q ? S.areas.filter(a => fold(a.name).includes(fold(q.replace(/^(phuong|xa)\s+/, '')))).sort((a, b) => fold(a.name).indexOf(q) - fold(b.name).indexOf(q)).slice(0, 8) : [];
      list.replaceChildren(...items.map((a, i) => el('li', {role: 'option', id: 'area-opt-' + i, 'aria-selected': i === active ? 'true' : 'false', onmousedown: e => { e.preventDefault(); choose(a); }}, areaName(a))));
      if (q && !items.length) list.append(el('li', {class: 'empty', text: 'Không thấy phường, xã này. Thử tên khác hoặc chọn trên bản đồ.'}));
      list.hidden = !q; input.setAttribute('aria-expanded', q ? 'true' : 'false');
    }
    input.addEventListener('input', () => { active = -1; show(); });
    input.addEventListener('keydown', e => {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); if (!items.length) return; active = (active + (e.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length; show(); input.setAttribute('aria-activedescendant', 'area-opt-' + active); }
      else if (e.key === 'Enter' && items.length) { e.preventDefault(); choose(items[Math.max(0, active)]); }
      else if (e.key === 'Escape') close();
    });
    input.addEventListener('blur', () => setTimeout(close, 100));
    $('use-location').addEventListener('click', () => {
      if (!navigator.geolocation) { $('locate-status').textContent = 'Trình duyệt này không hỗ trợ định vị. Hãy chọn phường, xã hoặc chọn trên bản đồ.'; return; }
      $('locate-status').textContent = 'Đang hỏi quyền dùng vị trí…';
      navigator.geolocation.getCurrentPosition(p => pin(p.coords.latitude, p.coords.longitude, 'gps'),
        e => { $('locate-status').textContent = e.code === 1 ? 'Bạn chưa cho phép dùng vị trí. Hãy chọn phường, xã hoặc chọn trên bản đồ.' : 'Không lấy được vị trí. Hãy chọn phường, xã hoặc chọn trên bản đồ.'; },
        {enableHighAccuracy: false, timeout: 10000, maximumAge: 600000});
    });
    $('pick-on-map').addEventListener('click', () => {
      S.picking = !S.picking;
      $('pick-on-map').setAttribute('aria-pressed', String(S.picking));
      $('locate-status').textContent = S.picking ? 'Chạm vào bản đồ để chọn vị trí.' : '';
      if (S.picking) { $('home-body').hidden = false; $('home-empty').hidden = true; drawHomeMap(); $('home-map').scrollIntoView({block: 'center', behavior: 'smooth'}); }
    });
  }
  function nearestStations() {
    const p = S.place || {lat: 21.0285, lon: 105.8542};
    return ORDER.map(id => ({...station(id), d: km(p, station(id))})).sort((a, b) => a.d - b.d);
  }

  // ---- charts -------------------------------------------------------------
  function lineChart(box, rows, start, end, readout, opts = {}) {
    box.replaceChildren();
    const slots = Math.round((end - start) / H), byT = new Map(rows.map(r => [Date.parse(r.hour_start_utc), r]));
    const vals = rows.map(r => r.pm25);
    if (!vals.length) { box.append(el('p', {class: 'empty-chart', text: 'Không có số liệu trong khoảng này.'})); return; }
    const w = Math.max(300, Math.min(1100, box.clientWidth || 600)), h = opts.height || (w < 520 ? 210 : 250), L = 40, R = 12, T = 24, B = 34;
    const top = Math.max(20, ...vals) * 1.12, step = top <= 60 ? 10 : top <= 150 ? 25 : 50, max = Math.ceil(top / step) * step;
    const x = i => L + (i + .5) / slots * (w - L - R), y = v => h - B - v / max * (h - T - B);
    const s = svg('svg', {viewBox: `0 0 ${w} ${h}`, tabindex: 0, 'aria-label': opts.label || 'Biểu đồ PM2.5 theo giờ'});
    for (let v = 0; v <= max; v += step) { s.append(svg('line', {x1: L, x2: w - R, y1: y(v), y2: y(v), class: v ? 'grid' : 'axis'}), svg('text', {x: L - 6, y: y(v) + 4, 'text-anchor': 'end', class: 'tick'}, num(v, 0))); }
    s.append(svg('text', {x: 4, y: 11, class: 'tick'}, 'µg/m³'));
    if (opts.reference) refLine(s, opts.reference, L, w - R, y);
    if (slots > 48) { // one label per local day, thinned to fit
      const days = [];
      for (let i = 0; i < slots; i++) if (local(start + i * H).getUTCHours() === 0) days.push(i);
      const every = Math.ceil(days.length / (w < 520 ? 4 : 8));
      days.filter((_, k) => k % every === 0).forEach(i => s.append(svg('line', {x1: x(i), x2: x(i), y1: T, y2: h - B, class: 'grid'}), svg('text', {x: x(i), y: h - 12, 'text-anchor': 'start', class: 'tick'}, dm(start + i * H))));
    } else {
      const every = Math.ceil(slots / (w < 520 ? 4 : 8));
      for (let i = 0; i < slots; i += every) s.append(svg('text', {x: x(i), y: h - 12, 'text-anchor': 'middle', class: 'tick'}, hm(start + i * H)));
    }
    let seg = [], missing = 0;
    const flush = () => { if (seg.length > 1) s.append(svg('path', {d: seg.map((p, i) => (i ? 'L' : 'M') + p.join(',')).join(' '), class: 'line'})); else if (seg.length === 1) s.append(svg('circle', {cx: seg[0][0], cy: seg[0][1], r: 3, class: 'dot'})); seg = []; };
    for (let i = 0; i < slots; i++) {
      const r = byT.get(start + i * H);
      if (!r) { flush(); missing++; s.append(svg('rect', {x: x(i) - (w - L - R) / slots / 2, y: h - B + 2, width: Math.max(1, (w - L - R) / slots - .5), height: 4, class: 'gap'})); continue; }
      seg.push([x(i), y(r.pm25)]);
    }
    flush();
    const guide = svg('line', {y1: T, y2: h - B, class: 'guide', visibility: 'hidden'}), mark = svg('circle', {r: 5, class: 'focus', visibility: 'hidden'});
    s.append(guide, mark);
    let idx = slots - 1;
    const show = i => {
      idx = Math.max(0, Math.min(slots - 1, i)); const t = start + idx * H, r = byT.get(t);
      guide.setAttribute('x1', x(idx)); guide.setAttribute('x2', x(idx)); guide.setAttribute('visibility', 'visible');
      if (r) { mark.setAttribute('cx', x(idx)); mark.setAttribute('cy', y(r.pm25)); mark.setAttribute('visibility', 'visible'); } else mark.setAttribute('visibility', 'hidden');
      readout.textContent = `${hm(t)}–${hm(t + H)}, ${dm(t)}: ` + (r ? `${num(r.pm25)} µg/m³` + (r.flags?.includes('provisional') ? ' (tạm tính)' : '') + (r.flags?.includes('very_low') ? ' (rất thấp, đã đánh dấu)' : '') + (r.flags?.includes('isolated_peak') ? ' (đỉnh đơn lẻ, đã đánh dấu)' : '') : 'không có số liệu');
    };
    s.addEventListener('pointermove', e => { const b = s.getBoundingClientRect(); show(Math.floor(((e.clientX - b.left) * w / b.width - L) / ((w - L - R) / slots))); });
    s.addEventListener('keydown', e => { if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') { e.preventDefault(); show(idx + (e.key === 'ArrowRight' ? 1 : -1)); } });
    s.addEventListener('focus', () => show(idx));
    box.append(s);
    readout.textContent = missing ? `Vạch xám dưới trục: ${missing}/${slots} giờ không có số liệu. Rê chuột hoặc dùng phím mũi tên để xem từng giờ.` : 'Rê chuột hoặc dùng phím mũi tên để xem từng giờ.';
  }
  function refLine(s, ref, x1, x2, y) { // dashed line plus a legend entry above the plot, never over the bars
    s.append(svg('line', {x1, x2, y1: y(ref.v), y2: y(ref.v), class: 'ref'}), svg('line', {x1: x2 - 170, x2: x2 - 150, y1: 8, y2: 8, class: 'ref'}), svg('text', {x: x2 - 145, y: 12, class: 'ref-label'}, ref.label));
  }
  function barChart(box, bars, readout, opts = {}) {
    box.replaceChildren();
    if (!bars.length) { box.append(el('p', {class: 'empty-chart', text: 'Không có số liệu trong khoảng này.'})); return; }
    const w = Math.max(300, Math.min(1100, box.clientWidth || 600)), h = w < 520 ? 220 : 260, L = 40, R = 12, T = 22, B = 34;
    const top = Math.max(20, opts.reference?.v || 0, ...bars.map(b => b.v ?? 0)) * 1.12, step = top <= 60 ? 10 : top <= 150 ? 25 : 50, max = Math.ceil(top / step) * step;
    const bw = (w - L - R) / bars.length, y = v => h - B - v / max * (h - T - B);
    const s = svg('svg', {viewBox: `0 0 ${w} ${h}`, 'aria-label': opts.label || 'Biểu đồ cột'});
    for (let v = 0; v <= max; v += step) s.append(svg('line', {x1: L, x2: w - R, y1: y(v), y2: y(v), class: v ? 'grid' : 'axis'}), svg('text', {x: L - 6, y: y(v) + 4, 'text-anchor': 'end', class: 'tick'}, num(v, 0)));
    const every = Math.ceil(bars.length / (w < 520 ? 5 : 10));
    bars.forEach((b, i) => {
      const x = L + i * bw;
      if (i % every === 0 || i === bars.length - 1 && bars.length < 12) s.append(svg('text', {x: x + bw / 2, y: h - 12, 'text-anchor': 'middle', class: 'tick'}, b.label));
      if (!Number.isFinite(b.v)) { s.append(svg('rect', {x: x + bw * .2, y: h - B + 2, width: bw * .6, height: 4, class: 'gap'})); return; }
      const bar = svg('rect', {x: x + Math.min(2, bw * .1), y: y(b.v), width: Math.max(1, bw - Math.min(4, bw * .2)), height: Math.max(1, h - B - y(b.v)), rx: Math.min(4, bw / 4), class: b.ok ? 'bar' : 'bar weak', tabindex: 0, role: 'button', 'aria-label': b.tip});
      const show = () => { readout.textContent = b.tip; s.querySelectorAll('.bar.on').forEach(n => n.classList.remove('on')); bar.classList.add('on'); };
      bar.addEventListener('pointerenter', show); bar.addEventListener('focus', show); bar.addEventListener('click', show);
      s.append(bar);
    });
    s.append(svg('text', {x: 4, y: 11, class: 'tick'}, 'µg/m³'));
    if (opts.reference) refLine(s, opts.reference, L, w - R, y);
    box.append(s);
    readout.textContent = opts.hint || 'Chạm vào một cột để xem chi tiết.';
  }

  // ---- home -------------------------------------------------------------
  function renderHome() {
    const has = !!S.place;
    $('home-body').hidden = !has && !S.picking; $('home-empty').hidden = has || S.picking;
    if (!has) { if (S.picking) drawHomeMap(); return; }
    const p = S.place, list = nearestStations(), near = list.filter(s => s.d <= NEAR_KM);
    $('place-line').replaceChildren(el('span', {class: 'muted', text: p.type === 'area' ? 'Khu vực đã chọn' : p.how === 'gps' ? 'Vị trí của bạn (đã làm tròn khoảng 1 km)' : 'Điểm đã chọn trên bản đồ (làm tròn khoảng 1 km)'}), el('strong', {text: p.type === 'area' ? areaName(p) : 'Gần ' + p.near.kind + ' ' + p.near.name}));
    const focus = S.focus ? list.find(s => s.id === S.focus) : near[0];
    const state = $('local-state');
    if (!near.length) {
      state.replaceChildren(el('div', {class: 'card gap-card'},
        el('h2', {text: 'Chưa có điểm đo gần khu vực này'}),
        el('p', {text: 'Chưa có trạm quan trắc công khai đủ gần khu vực này. Bạn có thể xem dữ liệu tại các điểm đo khác hoặc tham khảo dự báo khu vực Hà Nội.'}),
        el('p', {class: 'muted', text: `Điểm đo gần nhất là ${list[0].name}, ${distText(list[0].d, list[0].exact)}. AirLake coi một điểm đo là “gần” khi cách dưới ${NEAR_KM} km.`}),
        el('div', {class: 'actions'}, el('a', {class: 'button-link secondary', href: '#du-bao', text: 'Xem dự báo khu vực'}), el('a', {class: 'button-link secondary', href: '#ban-do', text: 'Xem các điểm đo'}))));
    } else {
      state.replaceChildren(el('p', {class: 'near-line', text: near.length === 1 ? `Có 1 điểm đo trong bán kính ${NEAR_KM} km.` : `Có ${near.length} điểm đo trong bán kính ${NEAR_KM} km.`}));
    }
    const card = $('primary-card');
    card.replaceChildren();
    if (focus) card.append(stationCard(focus, near.includes(focus)));
    $('other-stations').replaceChildren(el('h3', {text: focus ? 'Điểm đo khác' : 'Các điểm đo công khai'}),
      ...list.filter(s => s !== focus).map(s => {
        const o = obs(s.id), f = freshness(s.id);
        return el('button', {type: 'button', class: 'station-row', onclick: () => { S.focus = s.id; renderHome(); $('primary-card').scrollIntoView({block: 'start', behavior: 'smooth'}); }},
          el('span', {class: 'row-name', text: s.name}), el('span', {class: 'muted', text: distText(s.d, s.exact)}),
          el('span', {class: 'row-value'}, o?.pm25 !== undefined && o?.pm25 !== null ? `${num(o.pm25)} µg/m³` : '—', ' ', badge(f)));
      }));
    renderRegional();
    drawHomeMap();
  }
  function stationCard(s, isNear) {
    const o = obs(s.id), f = freshness(s.id);
    const card = el('article', {class: 'card station-card', 'data-freshness': f.state},
      el('div', {class: 'card-top'}, el('div', {}, el('h2', {text: s.name}), el('p', {class: 'muted', text: `${s.area[0].toUpperCase() + s.area.slice(1)} · ${distText(s.d, s.exact)}`})), badge(f)),
      !isNear ? el('p', {class: 'caveat', text: `Điểm đo này cách khu vực bạn chọn khá xa, nên số đo không đại diện cho nơi bạn ở.`}) : null,
      el('p', {class: 'big'}, el('strong', {text: o && Number.isFinite(o.pm25) ? num(o.pm25) : '—'}), el('span', {text: ' µg/m³ PM2.5'})),
      el('p', {class: 'when', text: measured(s.id, o)}),
      el('p', {class: 'fresh-text ' + f.state, text: f.text}),
      el('p', {class: 'muted', text: S.now ? `AirLake cập nhật lúc ${when(Date.parse(S.now.published_at))}` + (o?.fetch_status === 'fetch_failed' ? ' · lần lấy số liệu gần nhất từ điểm này không thành công' : '') : ''}),
      el('div', {class: 'context', id: 'ctx-' + s.id}, el('p', {class: 'muted', text: 'Đang tải 24 giờ qua…'})),
      el('h3', {text: '24 giờ qua, theo giờ'}),
      el('div', {class: 'chart small-chart', id: 'spark-' + s.id}), el('p', {class: 'readout', id: 'spark-read-' + s.id}),
      el('div', {class: 'actions'},
        el('a', {class: 'button-link', href: BOT ? `${BOT}?start=st_${s.id}` : '#canh-bao', rel: BOT ? 'noopener' : null, text: 'Nhận cảnh báo qua Telegram'}),
        el('a', {class: 'button-link secondary', href: '#lich-su', onclick: () => { S.h.station = s.id; }, text: 'Xem lịch sử'})),
      el('details', {}, el('summary', {text: 'Về điểm đo này'}), el('p', {text: s.sensor + '.'}), el('p', {class: 'muted', text: `Giấy phép ${s.licence} · ${s.credit}. Cảm biến giá rẻ cho số đo mang tính tham khảo, có thể lệch khi độ ẩm cao.`})));
    fillContext(s);
    return card;
  }
  async function fillContext(s) {
    const end = floorHour(Date.now()), start = end - 24 * H, ctx = () => $('ctx-' + s.id);
    try {
      const [day, month] = await Promise.all([history(s.id, start, end), history(s.id, Math.max(FIRST, end - 30 * D), end)]);
      if (!ctx()) return;
      lineChart($('spark-' + s.id), day.rows, start, end, $('spark-read-' + s.id), {height: 180, label: `PM2.5 theo giờ 24 giờ qua tại ${s.name}`, reference: null});
      const n = day.rows.length, mean = n ? day.rows.reduce((a, r) => a + r.pm25, 0) / n : NaN, o = obs(s.id), parts = [];
      const prov = day.rows.filter(r => r.flags.includes('provisional')).length, full = n - prov;
      parts.push(full >= 18 ? `Trung bình 24 giờ qua: ${num(mean)} µg/m³ (${n}/24 giờ có số liệu${prov ? `, ${prov} giờ tạm tính` : ''}). Hướng dẫn của WHO (2021) cho trung bình 24 giờ là 15 µg/m³.` : `24 giờ qua có ${n}/24 giờ có số liệu${prov ? ` (${prov} giờ tạm tính)` : ''}, chưa đủ để tính trung bình ngày hay so với hướng dẫn WHO.`);
      const vals = month.rows.map(r => r.pm25);
      if (s.hourly && o && Number.isFinite(o.pm25) && vals.length >= 240 && freshness(s.id).state !== 'stale') { // like with like: an hourly mean against hourly means
        const below = vals.filter(v => v < o.pm25).length / vals.length * 100;
        parts.push(`Số đo hiện tại cao hơn ${num(below, 0)}% số giờ có số liệu trong 30 ngày qua tại điểm này (${vals.length} giờ).`);
      }
      ctx().replaceChildren(...parts.map(t => el('p', {text: t})));
    } catch { if (ctx()) ctx().replaceChildren(el('p', {class: 'muted', text: 'Chưa tải được lịch sử 24 giờ. Thử lại sau ít phút.'})); }
  }
  function renderRegional() {
    const box = $('regional'), m = S.now?.model;
    if (!m) { box.replaceChildren(el('p', {class: 'muted', text: 'Chưa có dự báo khu vực.'})); return; }
    const next = m.forecast.filter(r => r.hours_ahead <= 24), vals = next.map(r => r.pm25);
    const first = vals.slice(0, 3).reduce((a, v) => a + v, 0) / 3, last = vals.slice(-3).reduce((a, v) => a + v, 0) / 3;
    const trend = last > first * 1.15 ? 'có xu hướng tăng' : last < first * .85 ? 'có xu hướng giảm' : 'không thay đổi nhiều';
    box.replaceChildren(
      el('div', {class: 'card-top'}, el('div', {}, el('h2', {text: 'Dự báo khu vực Hà Nội'}), el('p', {class: 'muted', text: 'Mô hình CAMS cho cả ô lưới khoảng 45 km · không phải số đo'})), el('span', {class: 'badge model', text: 'Mô hình'})),
      el('p', {text: `Trong 24 giờ tới, mô hình cho rằng bụi mịn khu vực ${trend} (khoảng ${num(Math.min(...vals), 0)}–${num(Math.max(...vals), 0)} µg/m³).`}),
      el('p', {class: 'muted', text: `Lần chạy mô hình: ${when(Date.parse(m.model_run_at))}. Mô hình thường lệch nhiều so với cảm biến (tháng 9 cao hơn khoảng 17–32 µg/m³), nên chỉ dùng để xem xu hướng.`}),
      el('a', {href: '#du-bao', class: 'more', text: 'Xem dự báo các ngày tới →'}));
  }
  function baseMap(id, center, zoom) {
    if (!window.L) return null;
    if (S.maps[id]) return S.maps[id];
    const m = L.map(id, {scrollWheelZoom: false, attributionControl: true}).setView(center, zoom);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 18, attribution: '© OpenStreetMap contributors'}).addTo(m);
    m._layer = L.layerGroup().addTo(m);
    S.maps[id] = m;
    return m;
  }
  function stationMarkers(layer, onClick) {
    for (const id of ORDER) {
      const s = station(id), f = freshness(id), o = obs(id);
      if (!s.exact) L.circle([s.lat, s.lon], {radius: 300, className: 'approx'}).addTo(layer);
      L.circleMarker([s.lat, s.lon], {radius: 9, className: 'st-marker ' + f.state}).addTo(layer)
        .bindTooltip(`${s.name}: ${o && Number.isFinite(o.pm25) ? num(o.pm25) + ' µg/m³' : 'chưa có số đo'} · ${STATE_TEXT[f.state]}${s.exact ? '' : ' · vị trí gần đúng'}`)
        .on('click', () => onClick && onClick(id));
    }
  }
  function drawHomeMap() {
    const m = baseMap('home-map', [21.03, 105.86], 11);
    if (!m) return;
    if (!m._picker) { m._picker = true; m.on('click', e => { if (!S.picking) return; S.picking = false; $('pick-on-map').setAttribute('aria-pressed', 'false'); pin(e.latlng.lat, e.latlng.lng, 'map'); }); }
    m._layer.clearLayers();
    stationMarkers(m._layer, id => { S.focus = id; renderHome(); });
    if (S.place) {
      L.circle([S.place.lat, S.place.lon], {radius: NEAR_KM * 1000, className: 'near-ring'}).addTo(m._layer);
      L.circleMarker([S.place.lat, S.place.lon], {radius: 7, className: 'you'}).addTo(m._layer).bindTooltip(S.place.type === 'area' ? areaName(S.place) : 'Vị trí đã chọn');
      const pts = [[S.place.lat - .037, S.place.lon - .039], [S.place.lat + .037, S.place.lon + .039], ...ORDER.map(id => [station(id).lat, station(id).lon])]; // whole 4 km ring and every station
      m.fitBounds(pts, {padding: [24, 24], maxZoom: 13});
    }
    setTimeout(() => m.invalidateSize(), 50);
  }

  // ---- history ------------------------------------------------------------
  function rangeFor(kind) {
    const now = Date.now(), today = midnight(dateKey(now));
    if (kind === '24h') { const end = floorHour(now); return {start: end - 24 * H, end, text: `24 giờ trọn vẹn gần nhất, đến ${hm(end)} hôm nay`}; }
    if (kind === '7d') return {start: today - 7 * D, end: today, days: 7, text: '7 ngày trọn vẹn gần nhất, không tính hôm nay'};
    if (kind === '30d') return {start: today - 30 * D, end: today, days: 30, text: '30 ngày trọn vẹn gần nhất, không tính hôm nay'};
    if (kind === 'sep') return {start: FIRST, end: midnight('2026-10-01'), days: 30, text: 'Lưu trữ tháng 9/2026'};
    const from = $('h-from').value, to = $('h-to').value;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) return null;
    const start = Math.max(FIRST, midnight(from)), end = Math.min(today + D, midnight(to) + D);
    if (end <= start || end - start > 100 * D) return null;
    return {start, end, days: Math.round((end - start) / D), text: 'Khoảng tự chọn' + (midnight(from) < FIRST ? '. AirLake chỉ có dữ liệu từ 01/09/2026 nên khoảng bắt đầu từ ngày này' : '') + (end > now ? ' (hôm nay chưa hết ngày)' : '')};
  }
  async function renderHistory() {
    const sel = $('h-station');
    if (!sel.options.length) for (const id of ORDER) sel.append(el('option', {value: id, text: STATIONS[id].name}));
    S.h.station = S.h.station || nearestStations()[0].id;
    sel.value = S.h.station;
    document.querySelectorAll('#lich-su [data-range]').forEach(b => b.setAttribute('aria-checked', String(b.dataset.range === S.h.range)));
    $('h-custom').hidden = S.h.range !== 'custom';
    const today = dateKey(Date.now());
    for (const i of ['h-from', 'h-to']) { $(i).min = '2026-09-01'; $(i).max = today; }
    if (!$('h-from').value) { $('h-from').value = dateKey(Date.now() - 14 * D); $('h-to').value = dateKey(Date.now() - D); }
    const r = rangeFor(S.h.range), s = STATIONS[S.h.station];
    if (!r) { $('h-head').replaceChildren(el('p', {class: 'warn', text: 'Khoảng ngày không hợp lệ: chọn trong khoảng 01/09/2026 đến hôm nay, tối đa 100 ngày.'})); return; }
    $('h-head').replaceChildren(el('h2', {text: s.name}), el('p', {class: 'range-dates', text: `${hm(r.start)} ${dmy(r.start)} → ${hm(r.end)} ${dmy(r.end)} (giờ Việt Nam)`}), el('p', {class: 'muted', text: r.text}));
    for (const id of ['h-coverage', 'h-findings', 'h-patterns', 'h-tech', 'h-heatmap']) $(id).replaceChildren();
    $('h-chart').replaceChildren(el('p', {class: 'muted', text: 'Đang tải…'}));
    const token = S.h.token = Symbol();
    try {
      const span = r.end - r.start, prevStart = r.start - span;
      const [hourly, daily, prev] = await Promise.all([history(s.id, r.start, r.end), r.days ? history(s.id, r.start, r.end, 'daily') : null,
        r.days && prevStart >= FIRST ? history(s.id, prevStart, r.start, 'daily') : null]);
      if (token !== S.h.token) return;
      drawHistory(s, r, hourly, daily, prev);
    } catch {
      if (token === S.h.token) $('h-chart').replaceChildren(el('p', {class: 'warn', text: 'Chưa tải được lịch sử. Vui lòng thử lại sau ít phút.'}));
    }
  }
  function drawHistory(s, r, hourly, daily, prev) {
    const c = hourly.coverage, rows = hourly.rows;
    const longest = c.gaps[0];
    $('h-coverage').replaceChildren(el('div', {class: 'cov-bar', role: 'img', 'aria-label': `${num(c.percent)}% số giờ có số liệu`}, el('span', {style: `width:${Math.min(100, c.percent)}%`})),
      el('p', {}, el('strong', {text: `${c.hours_present}/${c.hours_expected} giờ có số liệu (${num(c.percent)}%).`}), ' ',
        r.days ? `${c.valid_days}/${c.days_in_range} ngày đủ dữ liệu (ít nhất 18/24 giờ), ${c.complete_days} ngày đủ cả 24 giờ.` : '',
        c.provisional_hours ? ` Trong đó ${c.provisional_hours} giờ là số tạm tính từ các lần AirLake lấy mẫu, sẽ được thay bằng số đầy đủ từ kho lưu trữ hôm sau.` : '',
        c.gap_count ? ` ${c.gap_count} khoảng trống; dài nhất ${longest.hours} giờ, từ ${hm(Date.parse(longest.start))} ${dm(Date.parse(longest.start))}.` : ' Không có giờ nào bị thiếu.'),
      ...(c.percent < 50 ? [el('p', {class: 'warn', text: 'Khoảng này thiếu nhiều số liệu. Các con số bên dưới chỉ mô tả phần có số liệu, không đại diện cho cả khoảng.'})] : []));
    const ref = {v: 15, label: 'WHO: 15 µg/m³ / 24 giờ'};
    if (!r.days) {
      $('h-chart-title').textContent = 'PM2.5 theo giờ';
      $('h-chart-note').textContent = 'Mỗi điểm là trung bình một giờ. Đoạn đứt là giờ không có số liệu; AirLake không nối hay điền số còn thiếu.';
      lineChart($('h-chart'), rows, r.start, r.end, $('h-chart-readout'), {label: `PM2.5 theo giờ tại ${s.name}`});
    } else {
      $('h-chart-title').textContent = 'PM2.5 trung bình theo ngày';
      $('h-chart-note').textContent = 'Cột đậm: ngày đủ dữ liệu (ít nhất 18/24 giờ). Cột nhạt: ngày thiếu giờ, chỉ để tham khảo. Đường gạch đỏ là hướng dẫn WHO 2021 cho trung bình 24 giờ, chỉ áp dụng cho ngày đủ dữ liệu.';
      const byDate = new Map(daily.rows.map(d => [d.date_local, d])), bars = [];
      for (let t = r.start; t < r.end; t += D) {
        const k = dateKey(t), d = byDate.get(k);
        bars.push({label: dm(t), v: d?.pm25, ok: d?.valid_day, tip: d ? `${dmy(t)}: ${num(d.pm25)} µg/m³ · ${d.hours}/24 giờ có số liệu${d.provisional_hours ? `, ${d.provisional_hours} giờ tạm tính` : ''}${d.valid_day ? '' : ' (chưa đủ, chỉ tham khảo)'}` : `${dmy(t)}: không có số liệu`});
      }
      barChart($('h-chart'), bars, $('h-chart-readout'), {reference: ref, label: `PM2.5 trung bình ngày tại ${s.name}`});
    }
    // findings
    const cards = [], valid = (daily?.rows || []).filter(d => d.valid_day);
    if (!r.days) {
      if (rows.length) {
        const hi = rows.reduce((a, b) => b.pm25 > a.pm25 ? b : a), lo = rows.reduce((a, b) => b.pm25 < a.pm25 ? b : a), mean = rows.reduce((a, b) => a + b.pm25, 0) / rows.length;
        const prov = rows.filter(x => x.flags.includes('provisional')).length;
        cards.push(['Trung bình các giờ có số liệu', `${num(mean)} µg/m³`, `${rows.length}/24 giờ` + (prov ? `, ${prov} giờ tạm tính` : '')]);
        cards.push(['Giờ cao nhất', `${num(hi.pm25)} µg/m³`, `${hm(Date.parse(hi.hour_start_utc))}, ${dm(Date.parse(hi.hour_start_utc))}`]);
        cards.push(['Giờ thấp nhất', `${num(lo.pm25)} µg/m³`, `${hm(Date.parse(lo.hour_start_utc))}, ${dm(Date.parse(lo.hour_start_utc))}`]);
      }
    } else if (valid.length) {
      const mean = valid.reduce((a, d) => a + d.pm25, 0) / valid.length, hi = valid.reduce((a, b) => b.pm25 > a.pm25 ? b : a), lo = valid.reduce((a, b) => b.pm25 < a.pm25 ? b : a), over = valid.filter(d => d.pm25 > 15).length;
      const day = k => dmy(midnight(k));
      cards.push(['Trung bình các ngày đủ dữ liệu', `${num(mean)} µg/m³`, `${valid.length} ngày`]);
      cards.push(['Ngày cao nhất', `${num(hi.pm25)} µg/m³`, day(hi.date_local)]);
      cards.push(['Ngày thấp nhất', `${num(lo.pm25)} µg/m³`, day(lo.date_local)]);
      cards.push(['Ngày trên mức WHO', `${over}/${valid.length} ngày`, 'trung bình 24 giờ trên 15 µg/m³']);
      const pv = (prev?.rows || []).filter(d => d.valid_day), need = Math.ceil(r.days * .7);
      if (prev && valid.length >= need && pv.length >= need) {
        const pm = pv.reduce((a, d) => a + d.pm25, 0) / pv.length, ch = (mean - pm) / pm * 100;
        cards.push(['So với khoảng liền trước', `${ch >= 0 ? 'Cao hơn' : 'Thấp hơn'} ${num(Math.abs(ch), 0)}%`, `${num(pm)} µg/m³ trong ${pv.length} ngày đủ dữ liệu, ${dm(prev.range ? Date.parse(prev.range.start_utc) : r.start - (r.end - r.start))}–${dm(r.start - D)}`]);
      } else if (prev) cards.push(['So với khoảng liền trước', 'Chưa so sánh', `cần ít nhất ${need} ngày đủ dữ liệu ở cả hai khoảng`]);
    } else cards.push(['Chưa có ngày đủ dữ liệu', '—', 'không tính trung bình hay so với WHO']);
    $('h-findings').replaceChildren(...cards.map(([k, v, n]) => el('div', {class: 'finding'}, el('span', {class: 'muted', text: k}), el('strong', {text: v}), el('span', {class: 'muted', text: n}))));
    // patterns: hour of day and weekday/weekend, only with enough data
    const blocks = [];
    if (r.days >= 14) {
      const slots = Array.from({length: 24}, () => []);
      rows.forEach(x => slots[local(Date.parse(x.hour_start_utc)).getUTCHours()].push(x.pm25));
      const minN = Math.min(...slots.map(v => v.length));
      if (minN >= 10) {
        const means = slots.map(v => v.reduce((a, b) => a + b, 0) / v.length), order = means.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]);
        const box = el('div', {class: 'chart'}), read = el('p', {class: 'readout'});
        blocks.push(el('div', {class: 'card'}, el('h2', {text: 'Theo giờ trong ngày'}),
          el('p', {text: `Trong khoảng này, bụi thường thấp nhất khoảng ${order.slice(0, 3).map(x => pad(x[1]) + ':00').join(', ')} và cao nhất khoảng ${order.slice(-3).reverse().map(x => pad(x[1]) + ':00').join(', ')}.`}),
          el('p', {class: 'muted', text: `Mỗi cột là trung bình của ít nhất ${minN} ngày có số liệu ở giờ đó. Đây là mô tả quá khứ, không phải dự báo cho hôm nay.`}), box, read));
        requestAnimationFrame(() => barChart(box, means.map((v, i) => ({label: pad(i) + 'h', v, ok: true, tip: `${pad(i)}:00–${pad((i + 1) % 24)}:00: ${num(v)} µg/m³ (trung bình ${slots[i].length} ngày)`})), read, {label: 'PM2.5 trung bình theo giờ trong ngày'}));
      }
      const wk = valid.filter(d => [0, 6].includes(new Date(d.date_local + 'T00:00:00Z').getUTCDay())), wd = valid.filter(d => !wk.includes(d));
      if (wk.length >= 3 && wd.length >= 6) {
        const m = a => a.reduce((x, d) => x + d.pm25, 0) / a.length;
        blocks.push(el('div', {class: 'card soft'}, el('h2', {text: 'Ngày thường và cuối tuần'}), el('p', {text: `Ngày thường: ${num(m(wd))} µg/m³ (${wd.length} ngày). Cuối tuần: ${num(m(wk))} µg/m³ (${wk.length} ngày). Chỉ tính ngày đủ dữ liệu; số ngày còn ít nên chênh lệch có thể do thời tiết.`})));
      }
    }
    if (r.days && r.days <= 14) {
      const box = el('div', {class: 'chart'}), read = el('p', {class: 'readout'});
      blocks.push(el('div', {class: 'card'}, el('h2', {text: 'Chi tiết theo giờ'}), box, read));
      requestAnimationFrame(() => lineChart(box, rows, r.start, r.end, read, {label: `PM2.5 theo giờ tại ${s.name}`}));
    }
    $('h-patterns').replaceChildren(...blocks);
    // technical
    const sorted = rows.map(x => x.pm25).sort((a, b) => a - b), med = sorted.length ? sorted[Math.floor(sorted.length / 2)] : NaN;
    const basis = {}, flags = {};
    rows.forEach(x => { basis[x.basis] = (basis[x.basis] || 0) + 1; x.flags.forEach(f => { flags[f] = (flags[f] || 0) + 1; }); });
    const BASIS = {openaq_hourly: 'giá trị giờ của OpenAQ', sc_archive: 'trung bình từ kho lưu trữ Sensor.Community', sc_sampled: 'tạm tính từ các lần AirLake lấy mẫu'};
    const FLAG = {very_low: 'rất thấp (dưới 2 µg/m³)', isolated_peak: 'đỉnh đơn lẻ', provisional: 'tạm tính'};
    $('h-tech').replaceChildren(el('ul', {class: 'plain-list'},
      el('li', {text: `Trung vị theo giờ: ${num(med)} µg/m³; thấp nhất ${num(sorted[0])}, cao nhất ${num(sorted[sorted.length - 1])} µg/m³.`}),
      el('li', {text: 'Cách tạo giá trị giờ: ' + (Object.entries(basis).map(([k, v]) => `${BASIS[k] || k}: ${v} giờ`).join('; ') || '—') + '.'}),
      el('li', {text: 'Giờ được đánh dấu (giữ nguyên giá trị): ' + (Object.entries(flags).map(([k, v]) => `${FLAG[k] || k}: ${v}`).join('; ') || 'không có') + '.'}),
      el('li', {text: `Nguồn: ${s.credit} · giấy phép ${s.licence}. Không trộn số liệu mô hình vào chuỗi đo.`})));
    heatmap(rows, r);
    // exports
    const q = `station_id=${s.id}&start=${iso(r.start)}&end=${iso(r.end)}`;
    $('h-export').replaceChildren(...(API ? [['CSV theo giờ', `${q}&format=csv`], ['CSV theo ngày', `${q}&granularity=daily&format=csv`], ['JSON theo giờ', q]].map(([t, u]) => el('a', {class: 'button-link secondary', href: `${API}/v1/history?${u}`, rel: 'noopener', text: t})) : [el('span', {text: 'API chưa sẵn sàng.'})]));
  }
  function heatmap(rows, r) {
    const days = Math.round((r.end - r.start) / D);
    if (days < 2 || days > 40) return;
    const byT = new Map(rows.map(x => [Date.parse(x.hour_start_utc), x.pm25])), max = Math.max(1, ...byT.values());
    const head = el('tr', {}, el('th', {text: 'Ngày'}), ...Array.from({length: 24}, (_, i) => el('th', {text: pad(i)})));
    const body = [];
    for (let t = r.start; t < r.end; t += D) {
      body.push(el('tr', {}, el('th', {text: dm(t)}), ...Array.from({length: 24}, (_, i) => {
        const v = byT.get(t + i * H);
        return el('td', {class: v === undefined ? 'miss' : '', title: `${dm(t)} ${pad(i)}:00: ${v === undefined ? 'không có số liệu' : num(v) + ' µg/m³'}`, style: v === undefined ? null : `--a:${(.12 + .8 * v / max).toFixed(2)}`});
      })));
    }
    $('h-heatmap').replaceChildren(el('h3', {text: 'Mỗi ngày, từng giờ'}), el('p', {class: 'muted', text: 'Màu đậm hơn là nồng độ cao hơn trong khoảng đang xem. Ô gạch chéo là giờ không có số liệu.'}),
      el('div', {class: 'table-scroll'}, el('table', {class: 'heat'}, el('thead', {}, head), el('tbody', {}, body))));
  }

  // ---- map page -------------------------------------------------------------
  function renderMap() {
    const m = baseMap('stations-map', [21.03, 105.88], 11);
    const pick = id => {
      const s = station(id), o = obs(id), f = freshness(id);
      $('station-detail').replaceChildren(el('h2', {text: s.name}), el('p', {class: 'muted', text: s.area[0].toUpperCase() + s.area.slice(1) + (s.exact ? '' : ' · vị trí gần đúng, vòng tròn mờ là vùng có thể đặt cảm biến')}),
        el('p', {class: 'big'}, el('strong', {text: o && Number.isFinite(o.pm25) ? num(o.pm25) : '—'}), el('span', {text: ' µg/m³'})), el('p', {text: measured(id, o)}), el('p', {}, badge(f), ' ', f.text),
        el('p', {class: 'muted', text: s.sensor + '. Giấy phép ' + s.licence + '.'}),
        el('div', {class: 'actions'}, el('a', {class: 'button-link secondary', href: '#lich-su', onclick: () => { S.h.station = id; }, text: 'Xem lịch sử'}), el('a', {class: 'button-link secondary', href: BOT ? `${BOT}?start=st_${id}` : '#canh-bao', text: 'Cảnh báo Telegram'})));
    };
    if (!m) { pick(ORDER[0]); return; }
    m._layer.clearLayers();
    stationMarkers(m._layer, pick);
    const g = S.now?.model?.grid_cell;
    if ($('show-model').checked && g) L.rectangle([[g.latitude - .2, g.longitude - .2], [g.latitude + .2, g.longitude + .2]], {className: 'model-cell'}).addTo(m._layer).bindTooltip('Ô lưới mô hình CAMS (~45 km): một giá trị ước tính cho cả vùng, không phải điểm đo');
    m.fitBounds(ORDER.map(id => [station(id).lat, station(id).lon]), {padding: [60, 60]});
    if ($('show-model').checked && g) m.fitBounds([[g.latitude - .22, g.longitude - .22], [g.latitude + .22, g.longitude + .22]]);
    pick(S.mapPick || nearestStations()[0].id);
    setTimeout(() => m.invalidateSize(), 50);
  }

  // ---- forecast -----------------------------------------------------------
  function renderForecast() {
    const m = S.now?.model;
    if (!m) { $('f-note').textContent = 'Chưa có dự báo.'; return; }
    const rows = m.forecast.map(r => ({hour_start_utc: new Date(Date.parse(r.valid_at) - H).toISOString().replace('.000', ''), pm25: r.pm25, flags: []}));
    const start = Date.parse(rows[0].hour_start_utc), end = Date.parse(rows[rows.length - 1].hour_start_utc) + H;
    $('f-note').textContent = `Lần chạy mô hình: ${when(Date.parse(m.model_run_at))} · cập nhật khoảng 12 giờ một lần · giá trị cho ô lưới quanh ${num(m.grid_cell.latitude, 2)}°B, ${num(m.grid_cell.longitude, 2)}°Đ.`;
    lineChart($('f-chart'), rows, start, end, $('f-readout'), {label: 'PM2.5 dự báo theo giờ của mô hình CAMS'});
    const v = S.now.validation;
    $('f-validation').textContent = v ? `${v.summary} Tháng ${v.period}: ${v.stations.map(x => `${x.name.includes('99385') ? 'Tây Hồ' : 'Gia Lâm'} lệch trung bình ${x.bias > 0 ? '+' : ''}${num(x.bias)} µg/m³`).join(', ')}. Tháng 11, mô hình lại thường thấp hơn số đo.` : '';
  }

  // ---- alerts, data, status ----------------------------------------------
  function renderAlerts() {
    $('alert-stations').replaceChildren(...ORDER.map(id => {
      const s = STATIONS[id], d = S.place ? km(S.place, station(id)) : null;
      return el('a', {class: 'alert-option', href: BOT ? `${BOT}?start=st_${id}` : '#canh-bao', rel: 'noopener'}, el('strong', {text: s.name}),
        el('span', {class: 'muted', text: (s.hourly ? 'Số đo mỗi giờ' : 'Số đo vài phút một lần') + (d !== null ? ' · ' + distText(d, s.exact) + ' khu vực bạn chọn' : '')}), el('span', {class: 'go', text: BOT ? 'Mở Telegram →' : 'Bot chưa sẵn sàng'}));
    }));
  }
  async function renderData() {
    const sel = $('q-station');
    if (!sel.options.length) {
      for (const id of ORDER) sel.append(el('option', {value: id, text: STATIONS[id].name}));
      $('q-from').value = dateKey(Date.now() - 7 * D); $('q-to').value = dateKey(Date.now() - D);
      for (const i of ['q-from', 'q-to']) { $(i).min = '2026-09-01'; $(i).max = dateKey(Date.now()); }
      sel.value = S.h.station || ORDER[0];
    }
    if (API) {
      $('api-docs').href = API + '/v1'; $('api-docs').textContent = API + '/v1';
      $('api-examples').textContent = [`${API}/v1/stations`, `${API}/v1/history?station_id=6123215`, `${API}/v1/history?station_id=99385&start=2026-11-01&end=2026-11-15&granularity=daily&format=csv`, `${API}/v1/now`].join('\n');
    }
    $('archive-export').replaceChildren('Bản xuất 720 giờ của trạm Gia Lâm trong tháng 9/2026, đã chạy qua AWS Glue (Iceberg) và được Athena kiểm tra lại từng dòng; dùng để đối chiếu kỹ thuật. Bản này ghi mỗi giờ theo mốc cuối giờ của OpenAQ (dòng 01:00 là trung bình 00:00–01:00), nên nó gồm giờ 23:00–24:00 ngày 31/8 và thiếu giờ 23:00–24:00 ngày 30/9. Cột aqi là phép tính nội bộ để kiểm tra pipeline, không phải VN_AQI và không dùng để đánh giá sức khỏe. Số liệu theo giờ ở trang Lịch sử đã dùng đúng đầu giờ. ', API ? el('a', {href: `${API}/v1/archive/september-2026?format=csv&limit=1000`, text: 'Tải CSV'}) : '');
    const t = $('datasets');
    try {
      const st = API ? await getJSON(API + '/v1/stations', 300000) : null;
      t.replaceChildren(el('thead', {}, el('tr', {}, ...['Điểm đo', 'Có dữ liệu từ', 'Đến', 'Số giờ', 'Giấy phép'].map(x => el('th', {text: x})))),
        el('tbody', {}, ...(st?.stations || []).map(x => el('tr', {}, el('td', {text: x.name}), el('td', {text: x.history.first_hour_utc ? when(Date.parse(x.history.first_hour_utc)) : '—'}), el('td', {text: x.history.last_hour_utc ? when(Date.parse(x.history.last_hour_utc) + H) : '—'}), el('td', {text: num(x.history.hours, 0)}), el('td', {text: x.licence})))));
    } catch { t.replaceChildren(el('caption', {text: 'Chưa tải được danh sách bộ dữ liệu.'})); }
  }
  async function runQuery() {
    const id = Number($('q-station').value), g = $('q-gran').value, from = $('q-from').value, to = $('q-to').value;
    const start = midnight(from), end = midnight(to) + D;
    if (!API || !(end > start) || start < FIRST || end - start > 100 * D) { $('q-summary').textContent = 'Chọn khoảng ngày từ 01/09/2026 đến hôm nay, tối đa 100 ngày.'; return; }
    $('q-summary').textContent = 'Đang tải…';
    try {
      const v = await getJSON(`${API}/v1/history?station_id=${id}&start=${from}&end=${dateKey(end)}&granularity=${g}&limit=1000`, 300000), c = v.coverage;
      $('q-summary').textContent = `${v.total_rows} dòng · ${c.hours_present}/${c.hours_expected} giờ có số liệu (${num(c.percent)}%) · ${c.valid_days}/${c.days_in_range} ngày đủ dữ liệu · ${v.station.attribution} · ${v.station.licence}.`;
      const cols = g === 'hourly' ? ['hour_start_local', 'pm25', 'n_readings', 'basis', 'flags'] : ['date_local', 'pm25', 'hours', 'valid_day', 'complete_day'];
      $('q-preview').replaceChildren(el('thead', {}, el('tr', {}, ...cols.map(x => el('th', {text: x})))), el('tbody', {}, ...v.rows.slice(0, 10).map(r => el('tr', {}, ...cols.map(x => el('td', {text: Array.isArray(r[x]) ? r[x].join('|') : String(r[x])}))))));
      const base = `${API}/v1/history?station_id=${id}&start=${from}&end=${dateKey(end)}&granularity=${g}`;
      $('q-links').replaceChildren(el('a', {class: 'button-link', href: base + '&format=csv', text: 'Tải CSV'}), el('a', {class: 'button-link secondary', href: base, text: 'Mở JSON'}), ...(v.next ? [el('span', {class: 'muted', text: ' Hơn 1.000 dòng: dùng tham số offset để lấy trang tiếp theo.'})] : []));
    } catch { $('q-summary').textContent = 'Không tải được. Thử lại sau ít phút.'; }
  }
  async function renderStatus() {
    const rows = ORDER.map(id => {
      const o = obs(id), f = freshness(id), s = STATIONS[id];
      return el('tr', {}, el('td', {text: s.name}), el('td', {text: o && Number.isFinite(o.pm25) ? `${num(o.pm25)} µg/m³` : '—'}), el('td', {text: measured(id, o)}),
        el('td', {text: o?.fetched_at ? when(Date.parse(o.fetched_at)) : '—'}), el('td', {}, badge(f)), el('td', {text: o?.fetch_status === 'ok' ? 'Thành công' : o?.fetch_status === 'no_new_reading' ? 'Chưa có số mới' : o ? 'Không thành công' : '—'}));
    });
    $('status-table').replaceChildren(el('thead', {}, el('tr', {}, ...['Điểm đo', 'Số đo gần nhất', 'Thời gian đo', 'AirLake nhận lúc', 'Trạng thái', 'Lần lấy gần nhất'].map(x => el('th', {text: x})))), el('tbody', {}, rows));
    const lines = [S.now ? `Bản số liệu hiện tại được AirLake công bố lúc ${when(Date.parse(S.now.published_at))}.` : '', S.now?.model ? `Mô hình CAMS chạy lúc ${when(Date.parse(S.now.model.model_run_at))}.` : '', S.nowAt ? `Trang tải lần cuối lúc ${when(S.nowAt)}.` : ''];
    try {
      const st = API ? await getJSON(API + '/v1/stations', 300000) : null;
      if (st) lines.push('Chuỗi lịch sử cập nhật lúc: ' + st.stations.map(x => `${STATIONS[x.station_id]?.short || x.station_id} ${when(Date.parse(x.history.updated_at))}`).join(', ') + '.');
    } catch { /* optional */ }
    $('status-times').textContent = lines.filter(Boolean).join(' ');
  }

  // ---- routing ------------------------------------------------------------
  const PAGES = {'quanh-toi': renderHome, 'lich-su': renderHistory, 'ban-do': renderMap, 'du-bao': renderForecast, 'canh-bao': renderAlerts, 'du-lieu': renderData, 'tinh-trang': renderStatus};
  const page = () => { const h = location.hash.slice(1); return PAGES[h] ? h : 'quanh-toi'; };
  function render() {
    const p = page();
    document.querySelectorAll('.view').forEach(v => { v.hidden = v.id !== p; });
    document.querySelectorAll('[data-nav]').forEach(a => { if (a.dataset.nav === p) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current'); });
    PAGES[p]();
  }
  async function init() {
    try { S.areas = (await getJSON('areas.json')).areas; } catch { S.areas = []; }
    initSearch();
    const saved = store.get('airlake.place.v1');
    if (saved?.type === 'area') { const a = S.areas.find(x => x.osm === saved.osm); if (a) { S.place = {type: 'area', ...a}; $('area-input').value = areaName(a); } }
    else if (saved?.type === 'pin' && Number.isFinite(saved.lat) && Number.isFinite(saved.lon)) S.place = {type: 'pin', how: saved.how, lat: saved.lat, lon: saved.lon, near: nearestArea(saved)};
    if (S.place) S.h.station = nearestStations()[0].id;
    $('h-station').addEventListener('change', e => { S.h.station = Number(e.target.value); renderHistory(); });
    document.querySelectorAll('#lich-su [data-range]').forEach(b => b.addEventListener('click', () => { S.h.range = b.dataset.range; renderHistory(); }));
    $('h-apply').addEventListener('click', renderHistory);
    $('q-run').addEventListener('click', runQuery);
    $('show-model').addEventListener('change', renderMap);
    window.addEventListener('hashchange', () => { render(); window.scrollTo(0, 0); });
    let width = innerWidth;
    window.addEventListener('resize', () => { if (Math.abs(innerWidth - width) > 80) { width = innerWidth; render(); } });
    document.addEventListener('visibilitychange', () => { if (!document.hidden && Date.now() - S.nowAt > 60000) loadNow(); });
    setInterval(() => { if (!document.hidden) loadNow(); }, 300000);
    render();
    loadNow();
  }
  document.addEventListener('DOMContentLoaded', init);
})();

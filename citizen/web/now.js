(()=>{
  'use strict';
  const $=id=>document.getElementById(id);
  const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
  const svg=(tag,attrs={},text)=>{const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n;};
  const num=v=>Number.isFinite(v)?new Intl.NumberFormat('vi-VN',{maximumFractionDigits:1}).format(v):'—';
  const time=v=>v?new Intl.DateTimeFormat('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',hour:'2-digit',minute:'2-digit',day:'2-digit',month:'2-digit',year:'numeric'}).format(new Date(v)):'Chưa có';
  const hour=v=>new Intl.DateTimeFormat('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',hour:'2-digit',minute:'2-digit'}).format(new Date(v));
  const weekday=d=>new Intl.DateTimeFormat('vi-VN',{timeZone:'UTC',weekday:'long',day:'2-digit',month:'2-digit'}).format(new Date(d+'T00:00:00Z'));
  const age=v=>{const m=Math.round((Date.now()-Date.parse(v))/60000);return m<60?`${m} phút trước`:m<2880?`${Math.floor(m/60)} giờ ${m%60} phút trước`:`${Math.floor(m/1440)} ngày trước`;};
  // QĐ 1459 PM2.5 bands applied to a single reading: a reference label, not official VN_AQI.
  const bands=[[25,'Tốt'],[50,'Trung bình'],[80,'Kém'],[150,'Xấu'],[250,'Rất xấu'],[Infinity,'Nguy hại']];
  const band=v=>bands.find(([hi])=>v<hi)[1];
  let now=null;

  function observedCard(o){
    const card=node('article',undefined,'feature-card now-card'),state=o.freshness?.status,recent=state==='recent'&&Number.isFinite(o.pm25);
    const top=node('div',undefined,'card-top');top.append(node('span','QUAN TRẮC THỰC TẾ','card-kicker'),node('span',recent?'Số đo gần đây':state==='no_data'?'Chưa có số đo':'Số đo đã cũ','status '+(recent?'recent':state==='no_data'?'unknown':'stale')));
    card.append(top,node('h3',o.name));
    const value=node('p',undefined,'reading');value.append(node('strong',recent?num(o.pm25):'—'),node('span','µg/m³'));card.append(value);
    if(recent)card.append(node('p',`Mức tham khảo: ${band(o.pm25)}`,'band'));
    else card.append(node('p','Không dùng số đo cũ để mô tả hiện tại.','muted'));
    const dl=node('dl',undefined,'reading-meta');for(const [k,v] of [['Đo lúc',time(o.observed_at)],['Cách đây',o.observed_at?age(o.observed_at):'—'],['AirLake lấy lúc',time(o.fetched_at)],['Loại cảm biến',o.station_type==='low_cost'?'Chi phí thấp':'Tham chiếu'],['Vị trí',o.exact_location?'Toạ độ chính xác':'Toạ độ gần đúng (mạng làm tròn)'],['Nguồn',`${o.provider} · ${o.licence}`]]){const d=node('div');d.append(node('dt',k),node('dd',v));dl.append(d);}
    card.append(dl);if(o.fetch_status==='fetch_failed')card.append(node('p',`Lần lấy dữ liệu gần nhất từ nguồn này không thành công; đang hiển thị số đo trước đó.`,'freshness-note'));return card;
  }

  function modelCard(m){
    const card=node('article',undefined,'feature-card now-card model-card');
    const top=node('div',undefined,'card-top');top.append(node('span','ƯỚC TÍNH MÔ HÌNH','card-kicker'),node('span','Không phải số đo','status unknown'));
    card.append(top,node('h3','Khu vực trung tâm Hà Nội (ô lưới ~45 km)'));
    const value=node('p',undefined,'reading');value.append(node('strong',num(m.current.pm25)),node('span','µg/m³'));card.append(value);
    card.append(node('p',`Giá trị mô hình cho ${hour(m.current.valid_at)}. Một ô lưới bao phủ nhiều quận nên không mô tả từng con phố.`,'muted'));
    const dl=node('dl',undefined,'reading-meta');for(const [k,v] of [['Giờ mô hình áp dụng',time(m.current.valid_at)],['Lượt chạy mô hình',time(m.model_run_at)],['Có dữ liệu từ',time(m.model_available_at)],['Nguồn','CAMS Global · Open-Meteo']]){const d=node('div');d.append(node('dt',k),node('dd',v));dl.append(d);}
    card.append(dl);if(m.fetch_status==='reused_previous_run')card.append(node('p','Chưa lấy được lượt chạy mới; đang dùng lượt chạy trước.','freshness-note'));return card;
  }

  function line(box,rows){
    box.replaceChildren();const w=Math.max(300,Math.min(1100,box.clientWidth||900)),h=240,left=44,right=12,top=20,bottom=40;
    const max=Math.ceil(Math.max(10,...rows.map(r=>r.pm25))*1.15/10)*10,x=i=>left+i*(w-left-right)/Math.max(1,rows.length-1),y=v=>h-bottom-v/max*(h-top-bottom);
    const s=svg('svg',{viewBox:`0 0 ${w} ${h}`,'aria-hidden':'true'});
    for(let i=0;i<5;i++){const v=max*i/4;s.append(svg('line',{x1:left,y1:y(v),x2:w-right,y2:y(v),stroke:'#e0e8e3','stroke-dasharray':'3 4'}),svg('text',{x:left-6,y:y(v)+4,'text-anchor':'end','font-size':11,fill:'#52665d'},num(v)));}
    s.append(svg('path',{d:rows.map((r,i)=>`${i?'L':'M'}${x(i).toFixed(1)},${y(r.pm25).toFixed(1)}`).join(''),fill:'none',stroke:'#6b5aa6','stroke-width':2,'stroke-dasharray':'7 4'}));
    rows.forEach((r,i)=>{if(i%6===0||i===rows.length-1)s.append(svg('text',{x:x(i),y:h-18,'text-anchor':i===rows.length-1?'end':'middle','font-size':11,fill:'#52665d'},hour(r.valid_at)));});
    s.append(svg('text',{x:w-right,y:14,'text-anchor':'end','font-size':11,fill:'#52665d'},'µg/m³ · mô hình'));box.append(s);
  }

  function renderForecast(m,v){
    $('forecast-meta').textContent=`Lượt chạy ${time(m.model_run_at)}, có dữ liệu từ ${time(m.model_available_at)}. Mô hình cập nhật khoảng 12 giờ một lần; dự báo dài ${Math.floor(m.forecast_horizon_hours/24)} ngày ${m.forecast_horizon_hours%24} giờ.`;
    drawForecast();
    const w=m.relative_windows_24h;
    $('forecast-windows').textContent=w.distinct?`Trong 24 giờ tới (06:00–21:00), mô hình dự báo PM2.5 tương đối thấp hơn vào ${hour(w.lower.start_utc)}–${hour(w.lower.end_utc)} (khoảng ${num(w.lower.mean)} µg/m³) và cao hơn vào ${hour(w.higher.start_utc)}–${hour(w.higher.end_utc)} (khoảng ${num(w.higher.mean)} µg/m³). Đây là so sánh tương đối của mô hình, không bảo đảm không khí an toàn.`:'Trong 24 giờ tới, mô hình không cho thấy khung giờ ban ngày nào thấp hơn rõ rệt (chênh dưới 20%).';
    $('forecast-days').replaceChildren(...m.daily.map(d=>{const tr=node('tr');tr.append(node('th',weekday(d.date_local)),node('td',num(d.mean)),node('td',`${num(d.min)}–${num(d.max)}`));return tr;}));
    const lowest=m.daily.length?m.daily.reduce((a,b)=>b.mean<a.mean?b:a):null;
    $('forecast-days-note').textContent=lowest?`Theo mô hình, ${weekday(lowest.date_local)} có trung bình thấp nhất trong các ngày đủ giờ. Chỉ dùng để so sánh giữa các ngày.`:'';
    $('forecast-validation').replaceChildren(node('p',v.summary),...v.stations.map(s=>node('p',`${s.name}: mô hình lệch trung bình ${s.bias>0?'+':''}${num(s.bias)} µg/m³ (sai số tuyệt đối trung bình ${num(s.mae)}), tương quan theo giờ ${num(s.r_hourly)}, theo ngày ${num(s.r_daily)} · ${s.hourly_n} giờ, tháng ${v.period}.`,'small')),node('p',v.reference_note,'small'));
  }

  function compare(obs,m){
    const live=obs.filter(o=>o.freshness?.status==='recent'&&Number.isFinite(o.pm25));
    if(!live.length)return'Chưa có số đo gần đây để đối chiếu với mô hình.';
    const lo=Math.min(...live.map(o=>o.pm25)),hi=Math.max(...live.map(o=>o.pm25)),mv=m.current.pm25;
    const rel=mv<lo?'thấp hơn':mv>hi?'cao hơn':'nằm trong khoảng';
    return`Lúc này mô hình (${num(mv)} µg/m³) ${rel} ${live.length>1?`các số đo thực tế (${num(lo)}–${num(hi)} µg/m³)`:`số đo thực tế (${num(lo)} µg/m³)`}. Khi hai nguồn khác nhau, hãy ưu tiên số đo tại điểm gần bạn và coi mô hình là bức tranh chung của vùng.`;
  }

  function drawForecast(){if(now&&!$('du-bao').hidden)line($('forecast-chart'),now.model.forecast.slice(0,24));}
  function notice(){if(!now)return'';const live=now.observations.filter(o=>o.freshness?.status==='recent').length;return`Hiện tại: ${live}/${now.observations.length} điểm quan trắc có giấy phép có số đo gần đây, cùng dự báo mô hình CAMS. Phần lịch sử là bản đã kiểm chứng của tháng 9/2026 tại Dragon, không phải số liệu hôm nay.`;}
  function setNotice(){if(now){$('notice').textContent=notice();$('notice').hidden=false;}}
  window.AirLakeNow={notice};
  let map=null;
  function renderMap(){
    if(!now||map||!window.L||$('khu-vuc').hidden)return;
    map=L.map('station-map',{scrollWheelZoom:false}).setView([21.03,105.85],10);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:16,attribution:'© OpenStreetMap contributors'}).addTo(map);
    const cell=L.circle(now.model.grid_cell.requested,{radius:22500,color:'#6b5aa6',weight:2,dashArray:'6 6',fillOpacity:.06}).addTo(map);map.fitBounds(cell.getBounds(),{padding:[6,6]});cell.bindPopup('Mô hình CAMS Global: độ phân giải ~45 km. Một giá trị cho cả vùng, không phân biệt từng quận.');
    for(const o of now.observations){const recent=o.freshness?.status==='recent';L.circleMarker([o.latitude,o.longitude],{radius:9,color:recent?'#225e51':'#86602b',fillOpacity:.85}).addTo(map).bindPopup(`${o.name}<br>${recent?num(o.pm25)+' µg/m³ · '+time(o.observed_at):'Chưa có số đo gần đây'}<br>${o.provider} · ${o.licence}`);}
  }

  function renderStatus(n){
    const rows=[...n.observations.map(o=>[o.name,'Quan trắc',`Đo ${time(o.observed_at)}`,`Lấy ${time(o.fetched_at)}${o.fetch_status==='fetch_failed'?' · lần gần nhất lỗi':''}`]),['CAMS Global (Open-Meteo)','Mô hình',`Chạy ${time(n.model.model_run_at)}`,`Có từ ${time(n.model.model_available_at)}`]];
    $('now-status').replaceChildren(...rows.map(r=>{const tr=node('tr');r.forEach((c,i)=>tr.append(node(i?'td':'th',c)));return tr;}));
    $('now-generated').textContent=`Bản "hiện tại" được công bố lúc ${time(n.published_at)}. Trang tự kiểm tra bản mới mỗi 5 phút khi đang mở; nguồn đo có nhịp riêng (Dragon theo giờ, cảm biến cộng đồng vài phút, mô hình 12 giờ).`;
  }

  function render(){
    $('now-panel').hidden=false;$('du-bao-empty').hidden=true;$('du-bao-body').hidden=false;
    const card=document.querySelector('.hero .reading-card');if(card)card.hidden=true;
    $('now-observations').replaceChildren(...now.observations.map(observedCard),modelCard(now.model));
    $('now-compare').textContent=compare(now.observations,now.model);
    $('now-geography').textContent=now.geography;
    $('now-attribution').replaceChildren(...[...new Set([...now.observations.map(o=>o.attribution),now.model.source.attribution])].map(a=>node('li',a)));
    renderForecast(now.model,now.validation);renderStatus(now);setNotice();renderMap();drawForecast();
  }

  async function load(){
    const url=window.AIRLAKE_CONFIG?.nowUrl;if(!url)return;
    try{const r=await fetch(url,{cache:'no-cache'});if(!r.ok)throw new Error('http');const next=await r.json();if(next.schema_version!==2)throw new Error('schema');if(now&&next.published_at===now.published_at)return;now=next;}
    catch{if(!now){$('now-panel').hidden=false;$('now-observations').replaceChildren(node('p','Chưa tải được dữ liệu hiện tại. Phần lịch sử bên dưới vẫn dùng được.','muted'));}return;}
    if(map){map.remove();map=null;}
    render();
  }
  const api=window.AIRLAKE_CONFIG?.apiBase;
  if(api){const box=$('public-api');if(box){box.hidden=false;for(const [label,path] of [['Tài liệu API','/v1'],['Hiện tại (JSON)','/v1/now'],['Dragon theo ngày (CSV)','/v1/history?granularity=daily&format=csv&limit=100'],['Dragon theo giờ, 7 ngày cuối (JSON)','/v1/history?start=2026-09-23T17:00:00Z&limit=200']]){const li=node('li');const a=node('a',label);a.href=api+path;a.rel='noopener';li.append(a,node('code',' '+path));$('public-api-links').append(li);}}}
  setInterval(()=>{if(document.visibilityState==='visible')load();},300000);
  document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')load();});
  window.addEventListener('hashchange',()=>setTimeout(()=>{renderMap();drawForecast();map?.invalidateSize();},0));
  window.addEventListener('airlake-ready',()=>setTimeout(setNotice,0));
  window.addEventListener('DOMContentLoaded',load);
})();

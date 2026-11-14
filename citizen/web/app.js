'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const cfg = window.AIRLAKE_CONFIG;
  const colors = ['#28685b', '#b2733c', '#556c9b'];
  const pages = ['quanh-ban', 'xu-huong', 'telegram', 'nghien-cuu', 'tinh-trang', 'khu-vuc'];
  let data = null, selected = null, metric = 'pm25', lastAttempt = 0;
  const fmt = new Intl.DateTimeFormat('vi-VN', {timeZone: 'Asia/Ho_Chi_Minh', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'});
  const number = n => Number.isFinite(n) ? new Intl.NumberFormat('vi-VN', {maximumFractionDigits: 2}).format(n) : '—';
  const when = t => t && Number.isFinite(Date.parse(t)) ? fmt.format(new Date(t)) : 'Chưa có dữ liệu';
  const localInput = t => new Date(Date.parse(t) + 7 * 3600000).toISOString().slice(0, 16);
  const inputTime = t => /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(t) ? Date.parse(t + '+07:00') : NaN;
  function freshness(latest, now = Date.now()) {
    if (!latest || !Number.isFinite(latest.pm25) || latest.pm25 < 0) return {kind: 'unknown', label: 'Chưa có dữ liệu', age: '—', note: 'Trạm chưa có số đo hợp lệ trong bản dữ liệu này.'};
    const age = (now - Date.parse(latest.observed_at)) / 1000;
    if (!Number.isFinite(age) || age < 0) return {kind: 'unknown', label: 'Chưa có dữ liệu', age: '—', note: 'Thời điểm quan trắc chưa hợp lệ. Chưa thể sử dụng số đo.'};
    const c = latest.cadence_seconds;
    const known = Number.isInteger(c) && c >= 1 && c <= 86400;
    const fresh = known ? Math.max(300, c) : 900;
    const stale = known ? Math.max(900, 2*c) : 10800;
    const ageText = age < 3600 ? `${Math.floor(age/60)} phút trước` : age < 86400 ? `${Math.floor(age/3600)} giờ ${Math.floor(age%3600/60)} phút trước` : `${Math.floor(age/86400)} ngày trước`;
    if (age <= fresh) return {kind: 'recent', label: 'Dữ liệu mới', age: ageText, note: 'Số đo còn gần nhịp quan trắc ước lượng. Đây không phải đánh giá sức khỏe.'};
    if (age <= stale) return {kind: 'late', label: 'Cập nhật trễ', age: ageText, note: 'Số đo đã qua nhịp quan trắc ước lượng. Thời gian nhận dữ liệu không thay cho thời gian đo.'};
    return {kind: 'stale', label: 'Dữ liệu đã cũ', age: ageText, note: 'Số đo này không mô tả chắc chắn tình trạng hiện tại. Đây là bản đã lưu; chưa đủ bằng chứng kết luận trạm ngừng hoạt động.'};
  }
  function validate(x) {
    if (!cfg || ![1,2].includes(x.schema_version) || x.mode !== cfg.mode || !Array.isArray(x.stations) || !x.stations.length || !Array.isArray(x.hourly) || x.units !== 'µg/m³' || x.automatic_refresh !== false) throw Error('schema');
    const ids = new Set();
    for (const s of x.stations) {
      if (!Number.isInteger(s.id) || ids.has(s.id) || typeof s.name !== 'string' || (x.mode === 'public' && (s.id !== 6123215 || s.downloads_allowed !== true || s.license !== 'CC BY 4.0'))) throw Error('station');
      ids.add(s.id);
    }
    if(x.schema_version===2){
      if(typeof x.cloud_verified!=='boolean'||x.hourly.length>2304||!Array.isArray(x.daily)||x.daily.length>96||!x.analytics)throw Error('history');
      const dailyKeys=new Set();
      for(const r of x.daily){const key=`${r.station_id}:${r.date_local}`;if(!ids.has(r.station_id)||dailyKeys.has(key)||!/^\d{4}-\d{2}-\d{2}$/.test(r.date_local)||!Number.isFinite(r.pm25)||r.pm25<0||!Number.isInteger(r.hours_present)||r.hours_present<1||r.hours_present>24||r.hours_expected!==24||r.complete_24_hours!==(r.hours_present===24))throw Error('daily');dailyKeys.add(key);}
      for(const key of ['24h','7d','30d']){const a=x.analytics[key];if(!a||!a.stations||Object.keys(a.stations).length!==ids.size||Object.keys(a.stations).some(k=>!ids.has(Number(k))))throw Error('analytics_scope');}
    }
    const keys = new Set();
    for (const r of x.hourly) {
      const t = Date.parse(r.observed_hour_utc), k = `${r.station_id}:${t}`;
      if (!ids.has(r.station_id) || !Number.isFinite(t) || t%3600000 || keys.has(k) || !Number.isFinite(r.pm25) || r.pm25 < 0 || !Number.isInteger(r.aqi_mvp) || r.aqi_mvp < 0 || r.aqi_mvp > 500) throw Error('row');
      keys.add(k);
    }
    if (!Number.isFinite(Date.parse(x.window_start_utc)) || (x.schema_version===1?Date.parse(x.window_end_utc)-Date.parse(x.window_start_utc)!==86400000:!(Date.parse(x.window_end_utc)>Date.parse(x.window_start_utc)&&Date.parse(x.window_end_utc)-Date.parse(x.window_start_utc)<=32*86400000))) throw Error('window');
    return x;
  }
  function svgNode(tag, attrs = {}, text) {
    const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function chart(id, stationIds, field) {
    const container = $(id); container.replaceChildren();
    const range=window.AirLakeHistory?.range(data,id),end=range?.end??Date.parse(data.window_end_utc),start=range?.start??Math.max(Date.parse(data.window_start_utc),end-86400000),slots=Math.round((end-start)/3600000);
    const relevant = data.hourly.filter(r => stationIds.includes(r.station_id) && Date.parse(r.observed_hour_utc) >= start && Date.parse(r.observed_hour_utc) < end);
    const peak = Math.max(10, ...relevant.map(r=>r[field]));
    const step = peak < 50 ? 10 : peak < 200 ? 50 : 100;
    const max = Math.ceil(peak * 1.1 / step) * step;
    const w = Math.max(300, Math.min(1100, container.clientWidth || 940)), h = w < 500 ? 270 : 290, left = 48, right = 24, top = 28, bottom = 48;
    const x = hour => left + hour/Math.max(1,slots-1)*(w-left-right), y = v => h-bottom - v/max*(h-top-bottom);
    const svg = svgNode('svg', {viewBox: `0 0 ${w} ${h}`, 'aria-hidden': 'true'});
    for(let i=0;i<=4;i++) {
      const value=max*i/4, yy=y(value);
      svg.append(svgNode('line',{x1:left,y1:yy,x2:w-right,y2:yy,stroke:'#e7ede5','stroke-dasharray':i?'3 5':'0'}));
      svg.append(svgNode('text',{x:left-10,y:yy+4,'text-anchor':'end',fill:'#66776e','font-size':11},number(value)));
    }
    for(let i=0;i<slots;i++) {
      if(i % Math.max(1,Math.ceil(slots/(w<500?4:7))) === 0 || i === slots-1) {
        const parts = new Intl.DateTimeFormat('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',hour:'2-digit',hourCycle:'h23'}).formatToParts(new Date(start+i*3600000));
        const hour = parts.find(p=>p.type==='hour').value;
        svg.append(svgNode('text',{x:x(i),y:h-25,'text-anchor':'middle',fill:'#52665d','font-size':12},slots>48?new Intl.DateTimeFormat('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',day:'2-digit',month:'2-digit'}).format(new Date(start+i*3600000)):hour+':00'));
      }
    }
    stationIds.forEach((sid,index)=>{
      const rows = new Map(relevant.filter(r=>r.station_id===sid).map(r=>[Date.parse(r.observed_hour_utc),r]));
      let segment=[];
      function flush(){if(segment.length>1)svg.append(svgNode('path',{d:segment.map(([xx,yy],i)=>`${i?'L':'M'}${xx},${yy}`).join(' '),fill:'none',stroke:colors[index%3],'stroke-width':3,'stroke-linecap':'round','data-series':sid}));segment=[];}
      for(let i=0;i<slots;i++) {
        const r=rows.get(start+i*3600000);
        if(!r){flush();svg.append(svgNode('circle',{cx:x(i),cy:h-bottom+5,r:1.8,fill:'#d3dbd1','data-missing':'true'}));continue;}
        segment.push([x(i),y(r[field])]);
        const circle=svgNode('circle',{cx:x(i),cy:y(r[field]),r:4,fill:colors[index%3],stroke:'#fff','stroke-width':2,'data-observation':'true'});
        const detail = `${data.stations.find(s=>s.id===sid).name} · ${when(r.observed_hour_utc)} · ${number(r[field])}${field==='pm25'?' µg/m³':' (chỉ số thử nghiệm)'}`;
        circle.setAttribute('tabindex',svg.querySelector('[data-observation]')?'-1':'0');circle.setAttribute('role','button');circle.setAttribute('aria-label',detail);
        circle.append(svgNode('title',{},detail));
        const describe=()=>$(id+'-readout').textContent=detail;
        circle.addEventListener('focus',describe);circle.addEventListener('pointerenter',describe);circle.addEventListener('click',describe);
        circle.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();describe();}else if(e.key==='ArrowLeft'||e.key==='ArrowRight'){e.preventDefault();const dots=[...svg.querySelectorAll('[data-observation]')],i=dots.indexOf(circle),next=dots[Math.max(0,Math.min(dots.length-1,i+(e.key==='ArrowRight'?1:-1)))];circle.setAttribute('tabindex','-1');next.setAttribute('tabindex','0');next.focus();}});svg.append(circle);
      }flush();
    });
    svg.append(svgNode('text',{x:w-right,y:h-5,'text-anchor':'end',fill:'#52665d','font-size':11},'Giờ Việt Nam · UTC+7'));
    container.append(svg);
    const times=relevant.map(r=>`${when(r.observed_hour_utc)}: ${number(r[field])}`);
    container.setAttribute('aria-label',`${field==='pm25'?'PM2.5, µg/m³':'Chỉ số thử nghiệm'}, ${slots} giờ. ${relevant.length} điểm có số liệu. Các giờ khác chưa có dữ liệu. ${times.join('; ')}`);
  }
  function notice() {
    const parts=[];if(data?.schema_version===2&&data.cloud_verified!==true)parts.push('BẢN KIỂM THỬ LỊCH SỬ · Chưa phát hành và chưa xác nhận xử lý đợt này trên AWS.');
    if(data?.mode==='instructor') parts.push('BẢN TRÌNH DIỄN CÓ XÁC THỰC · Một số nguồn chưa rõ quyền công bố. Không chia sẻ dữ liệu hạn chế ra ngoài.');
    else if(data) parts.push(`Bản công bố thí điểm · ${data.stations.length} điểm đủ điều kiện chia sẻ. Các nguồn khác đang được xác minh quyền công bố.`);
    if(!navigator.onLine)parts.push('Bạn đang ngoại tuyến. Nếu số đo còn hiển thị, đó là bản đã tải trước; thời gian đo không thay đổi.');
    $('notice').textContent=parts.join(' ');$('notice').hidden=!parts.length;
  }
  function render() {
    if(!data)return;
    const station=data.stations.find(s=>s.id===selected), f=freshness(station.latest);
    $('station-source').textContent=`Nguồn: ${station.provider} · qua OpenAQ`;
    $('pm-value').textContent=f.kind==='unknown'?'—':number(station.latest.pm25);
    document.querySelector('.reading-card').dataset.freshness=f.kind;
    $('freshness').textContent=f.label;$('freshness').className='status '+f.kind;
    $('observed-time').textContent=when(station.latest?.observed_at);
    $('ingested-time').textContent=when(station.latest?.ingested_at);$('source-observed-time').textContent=when(station.source_last_reported_at);
    $('observation-age').textContent=f.age;$('freshness-explanation').textContent=f.note;
    document.querySelectorAll('.chart-period').forEach(el=>el.textContent=`${when(data.window_start_utc)} → ${when(data.window_end_utc)} · giờ Việt Nam (24 giờ)`);
    chart('home-chart',[selected],'pm25');
    const ids=$('compare').checked?data.stations.map(s=>s.id):[selected];chart('trend-chart',ids,metric);
    $('trend-chart-title').textContent=metric==='pm25'?'PM2.5 theo giờ · µg/m³':'Chỉ số thử nghiệm theo giờ';
    $('aqi-warning').hidden=metric==='pm25';
    $('chart-legend').replaceChildren(...ids.map((id,i)=>{const el=document.createElement('span'), line=document.createElement('i');line.style.background=colors[i%3];el.append(line,document.createTextNode(data.stations.find(s=>s.id===id).name));return el;}));
    $('station-table').replaceChildren(...data.stations.map(s=>{
      const row=document.createElement('tr'), status=freshness(s.latest), count=data.hourly.filter(r=>r.station_id===s.id && Date.parse(r.observed_hour_utc)>=Date.parse(data.window_start_utc)).length;
      [s.name,status.kind==='unknown'?'—':`${number(s.latest.pm25)} µg/m³`,when(s.latest?.observed_at),status.label,`${count}/${Math.round((Date.parse(data.window_end_utc)-Date.parse(data.window_start_utc))/3600000)}`].forEach((v,i)=>{const td=document.createElement('td');if(i===3){const badge=document.createElement('span');badge.className='status '+status.kind;badge.textContent=v;td.append(badge);}else td.textContent=v;row.append(td);});return row;
    }));
    $('coordinates').replaceChildren(...data.stations.map(s=>{const p=document.createElement('p');p.textContent=`${s.name} · ${s.coordinates.latitude}, ${s.coordinates.longitude} · ${s.provider} · qua OpenAQ`;return p;}));
    $('sample-description').textContent=`Bản đang xem có ${data.hourly.length} số liệu trung bình giờ từ ${data.stations.length} điểm, trong khoảng ${when(data.window_start_utc)} đến ${when(data.window_end_utc)} (giờ Việt Nam).`;
    $('version-label').textContent=`Số liệu tính lúc ${when(data.computed_at)} · Bản trang tạo lúc ${when(data.published_at)} · Không tự làm mới.`;
    $('processed-time').textContent=when(data.computed_at);$('prepared-time').textContent=when(data.published_at);
    const ops=data.operations_status;
    $('collection-state').textContent=ops?`Lần kiểm tra ${when(ops.verified_at)}: thu thập định kỳ ${ops.collection_enabled?'được bật':'đã tạm dừng'}, cập nhật đợt số liệu ${ops.refresh_enabled?'được bật':'đã tạm dừng'}. Đây là thông tin đã đối chiếu khi chuẩn bị bản trang, không phải kiểm tra trực tiếp mỗi lần mở.`:'Chưa có xác nhận trạng thái thu thập trong bản này. Website đang dùng bản số liệu cố định, không tự làm mới.';
    $('comparison-note').textContent=data.stations.length<2?'Bản công khai hiện có một điểm đủ điều kiện chia sẻ. So sánh nhiều trạm chưa có trong bản này.':'Chỉ so sánh các điểm có dữ liệu; không lấy trung bình để suy ra toàn Hà Nội.';
    const ordered=data.hourly.filter(r=>r.station_id===selected).sort((a,b)=>Date.parse(a.observed_hour_utc)-Date.parse(b.observed_hour_utc));
    if(ordered.length>1){const change=ordered.at(-1).pm25-ordered[0].pm25;$('trend-interpretation').textContent=`Trong ${ordered.length} giờ có dữ liệu, PM2.5 ${change>0?'tăng':change<0?'giảm':'không đổi'}${change===0?'':` ${number(Math.abs(change))} µg/m³`} từ giờ đầu đến giờ cuối. Chỉ mô tả khoảng đã đo; không suy ra phần còn thiếu hoặc xu hướng cả tuần.`;}else{$('trend-interpretation').textContent='Chưa đủ hai giờ có số liệu để so sánh sự thay đổi.';}
    const expected=data.stations.length*Math.round((Date.parse(data.window_end_utc)-Date.parse(data.window_start_utc))/3600000);$('quality-summary').textContent=`${data.hourly.length}/${expected} ô trạm–giờ có số liệu; ${expected-data.hourly.length} ô còn thiếu. Không có khóa trạm–giờ trùng trong bản đã đối chiếu.`;
    $('station-status').replaceChildren(...data.stations.map(s=>{const card=document.createElement('article');card.className='feature-card';const h=document.createElement('h2');h.textContent=s.name;const badge=document.createElement('span'),state=freshness(s.latest);badge.className='status '+state.kind;badge.textContent=state.label;card.append(h,badge);const n=data.hourly.filter(r=>r.station_id===s.id).length;for(const text of [`Đo gần nhất: ${when(s.latest?.observed_at)}`,`AirLake nhận số đo: ${when(s.latest?.ingested_at)}`,`Nhịp đo ước lượng: ${Number.isInteger(s.latest?.cadence_seconds)?number(s.latest.cadence_seconds/60)+' phút':'chưa xác nhận'}`,`${n}/${Math.round((Date.parse(data.window_end_utc)-Date.parse(data.window_start_utc))/3600000)} giờ có dữ liệu trong bản lịch sử.`,`Nguồn: ${s.provider} · qua OpenAQ`]){const p=document.createElement('p');p.textContent=text;card.append(p);}return card;}));
    renderResearch();notice();window.AirLakeHistory?.render(data,selected);
  }
  function selection(id){selected=id; $('station').value=String(id);$('trend-station').value=String(id);render();}
  function researchRows(){const sid=Number($('research-station').value),start=inputTime($('date-start').value),end=inputTime($('date-end').value);if(!Number.isFinite(start)||!Number.isFinite(end)||start>=end)throw Error('time');if(data.schema_version===2)return window.AirLakeHistory.researchRows(data,sid,start,end,$('research-granularity').value);return data.hourly.filter(r=>r.station_id===sid&&Date.parse(r.observed_hour_utc)>=start&&Date.parse(r.observed_hour_utc)<end);}
  function renderResearch() {
    const station=data.stations.find(s=>s.id===Number($('research-station').value));
    if(!station)return;
    const allowed=station.downloads_allowed===true;
    let rows=[],valid=true;
    try { rows=researchRows(); } catch { valid=false; }
    $('download-json').disabled=$('download-csv').disabled=!allowed||!valid||rows.length===0;
    $('download-rights').textContent=allowed?`Được chia sẻ theo ${station.license}. Ghi công ${station.provider}${station.attribution?' / '+station.attribution:''} và OpenAQ. Số trung bình giờ do AirLake tổng hợp.`:'Nguồn này chưa rõ quyền chia sẻ. Bảo vệ bằng mật khẩu không thay thế giấy phép; không cung cấp tệp tải xuống.';
    $('research-count').textContent=!valid?'Hãy chọn khoảng thời gian bắt đầu trước thời gian kết thúc.':rows.length?`${rows.length} dòng phù hợp. Giờ bị thiếu không được tạo thêm.`:'Không có số liệu trong khoảng đã chọn. Thử mở rộng thời gian trong phạm vi của bản này.';
    $('research-example').textContent=allowed&&rows.length?JSON.stringify(rows[0],null,2)+'\n\nCSV: station_id,observed_hour_utc,pm25,aqi_mvp,units,source,license':'Chưa có ví dụ từ nguồn được phép chia sẻ trong khoảng này.';
    $('attribution').replaceChildren();
    const p=document.createElement('p');p.textContent=`Nguồn: OpenAQ / ${station.provider}${station.attribution?' / '+station.attribution:''}. Trạm: ${station.name}.`;$('attribution').append(p);
    if(allowed){const a=document.createElement('a');a.href='https://creativecommons.org/licenses/by/4.0/';a.target='_blank';a.rel='noopener noreferrer';a.textContent='Xem giấy phép CC BY 4.0 ↗';$('attribution').append(a);}
    window.AirLakeHistory?.renderPreview(data,station,rows,valid);
  }
  function download(kind){try{const s=data.stations.find(s=>s.id===Number($('research-station').value));if(!s?.downloads_allowed)throw Error('rights');const rows=researchRows();if(!rows.length)throw Error('empty');const source=`OpenAQ / ${s.provider} / ${s.attribution||''}`,license='https://creativecommons.org/licenses/by/4.0/';let body;if(kind==='json')body=JSON.stringify({data:rows,units:'µg/m³',timezone:$('research-granularity').value==='daily'?'Asia/Ho_Chi_Minh':'UTC',dataset_version:cfg.sha256,source,license,aqi_note:'Chỉ số thử nghiệm, không phải VN_AQI chính thức.'},null,2);else {const fields=($('research-granularity').value==='daily'?['station_id','sensor_id','date_local','pm25','hours_present','hours_expected','coverage_percent']:data.schema_version===2?['station_id','sensor_id','observed_hour_utc','pm25','aqi_mvp']:['station_id','observed_hour_utc','pm25','aqi_mvp']).concat(['units','source','license']);const cell=v=>{let t=String(v);if(/^[\s]*[=+\-@\t\r]/.test(t))t="'"+t;return '"'+t.replaceAll('"','""')+'"';};body=[fields.join(','),...rows.map(r=>fields.map(k=>cell({...r,units:'µg/m³',source,license}[k])).join(','))].join('\r\n');}const blob=new Blob([kind==='csv'?'\uFEFF':'',body],{type:kind==='csv'?'text/csv;charset=utf-8':'application/json;charset=utf-8'}),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`airlake-${s.id}.${kind}`;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);$('download-status').textContent=`Đã chuẩn bị ${rows.length} dòng, kèm nguồn và giấy phép.`;}catch{$('download-status').textContent='Chưa thể tải tệp. Kiểm tra trạm và khoảng thời gian; nguồn hạn chế không được tải xuống.';}}
  function navigate(){const page=pages.includes(location.hash.slice(1))?location.hash.slice(1):pages[0];for(const id of pages){$(id).hidden=id!==page;$(id).classList.toggle('active',id===page);}document.querySelectorAll('[data-nav]').forEach(a=>{if(a.dataset.nav===page)a.setAttribute('aria-current','page');else a.removeAttribute('aria-current');});document.title=`AirLake · ${$(page).querySelector('h1').textContent.replace(/\s+/g,' ')}`;if(data)requestAnimationFrame(render);}
  async function load() {
    if(Date.now()-lastAttempt<30000 && lastAttempt)return;
    lastAttempt=Date.now();$('retry').disabled=true;
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),8000);
    try {
      if(!cfg || !/^data\/[a-f0-9]{64}\.json$/.test(cfg.dataUrl))throw Error('config');
      const response=await fetch(cfg.dataUrl,{signal:controller.signal,credentials:'same-origin',redirect:'error',cache:'no-cache'});
      if(!response.ok)throw Error('unavailable');
      if(Number(response.headers.get('Content-Length'))>2000000)throw Error('size');
      const body=await response.arrayBuffer();if(body.byteLength>2000000)throw Error('size');
      const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',body))].map(x=>x.toString(16).padStart(2,'0')).join('');
      if(hash!==cfg.sha256)throw Error('integrity');
      data=validate(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(body)));
      selected=data.stations.some(s=>s.id===selected)?selected:data.stations[0].id;
      for(const id of ['station','trend-station','research-station'])$(id).replaceChildren(...data.stations.map(s=>new Option(s.name,String(s.id))));
      $('station').value=$('trend-station').value=String(selected);
      $('date-start').value=localInput(data.window_start_utc);$('date-end').value=localInput(data.window_end_utc);
      $('compare').disabled=data.stations.length<2;$('error-panel').hidden=true;render();window.dispatchEvent(new Event('airlake-ready'));
    } catch {
      $('error-panel').hidden=false;notice();
    } finally {
      clearTimeout(timer);setTimeout(()=>$('retry').disabled=false,30000);
    }
  }
  $('station').addEventListener('change',e=>selection(Number(e.target.value)));$('trend-station').addEventListener('change',e=>selection(Number(e.target.value)));$('compare').addEventListener('change',render);
  $('metric-pm').addEventListener('click',()=>{metric='pm25';$('metric-pm').setAttribute('aria-pressed','true');$('metric-aqi').setAttribute('aria-pressed','false');render();});$('metric-aqi').addEventListener('click',()=>{metric='aqi_mvp';$('metric-pm').setAttribute('aria-pressed','false');$('metric-aqi').setAttribute('aria-pressed','true');render();});
  for(const id of ['research-station','date-start','date-end'])$(id).addEventListener('change',()=>{if(data)renderResearch();});$('download-json').addEventListener('click',()=>download('json'));$('download-csv').addEventListener('click',()=>download('csv'));$('retry').addEventListener('click',load);window.addEventListener('hashchange',navigate);let resizeFrame;window.addEventListener('resize',()=>{cancelAnimationFrame(resizeFrame);if(data)resizeFrame=requestAnimationFrame(render);});window.addEventListener('offline',notice);window.addEventListener('online',notice);setInterval(()=>{if(data)render();},60000);window.AirLake={freshness,validate,getData:()=>data,getSelected:()=>selected,render,select:selection};navigate();load();
})();

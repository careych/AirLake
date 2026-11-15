(()=>{
  'use strict';
  const $=id=>document.getElementById(id);
  const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
  const svg=(tag,attrs={},text)=>{const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n;};
  const num=v=>Number.isFinite(v)?new Intl.NumberFormat('vi-VN',{maximumFractionDigits:1}).format(v):'—';
  const tz={timeZone:'Asia/Ho_Chi_Minh'};
  const part=(v,o)=>Object.fromEntries(new Intl.DateTimeFormat('en-GB',{...tz,hourCycle:'h23',...o}).formatToParts(new Date(v)).map(p=>[p.type,p.value]));
  const time=v=>{if(!v)return'chưa có';const p=part(v,{hour:'2-digit',minute:'2-digit',day:'2-digit',month:'2-digit'});return`${p.hour}:${p.minute} ${p.day}/${p.month}`;};
  const hour=v=>new Intl.DateTimeFormat('vi-VN',{...tz,hour:'2-digit',minute:'2-digit'}).format(new Date(v));
  const weekday=d=>new Intl.DateTimeFormat('vi-VN',{timeZone:'UTC',weekday:'long',day:'2-digit',month:'2-digit'}).format(new Date(d+'T00:00:00Z'));
  const ago=v=>{const m=Math.max(0,Math.round((Date.now()-Date.parse(v))/60000));return m<60?`${m} phút trước`:m<2880?`${Math.floor(m/60)} giờ ${m%60} phút trước`:`${Math.floor(m/1440)} ngày trước`;};
  // QĐ 1459/QĐ-TCMT PM2.5 bands applied to one reading: a reference, not official VN_AQI.
  const BANDS=[[25,'Tốt'],[50,'Trung bình'],[80,'Kém'],[150,'Xấu'],[250,'Rất xấu'],[Infinity,'Nguy hại']];
  const band=v=>BANDS.find(([hi])=>v<hi)[1];
  // Display facts per physical station. IDs stay as technical metadata.
  const STATIONS={
    6123215:{label:'Gia Lâm · Ocean Park',source:'Cảm biến Dragon (AirGradient), qua OpenAQ',sensor:'Cảm biến quang học giá rẻ AirGradient',place:'Khu đô thị Vinhomes Ocean Park, huyện Gia Lâm',cadence:'Số trung bình mỗi giờ',history:'Lịch sử theo giờ 01–30/09/2026 (720 giờ) có thể tải về',limits:'Cảm biến giá rẻ, chưa hiệu chuẩn với trạm tham chiếu; trong tháng 9 đọc cao hơn hai trạm tham chiếu gần đó.',link:'https://explore.openaq.org/locations/6123215',licence:'CC BY 4.0',color:'#1f6f8b'},
    99385:{label:'Tây Hồ · Cảm biến cộng đồng',source:'Cảm biến #99385 (SDS011), Sensor.Community',sensor:'Cảm biến SDS011 do người dân lắp đặt',place:'Khu vực quận Tây Hồ (vị trí gần đúng, mạng làm tròn tọa độ)',cadence:'Vài phút một số đo',history:'Chỉ có số đo từ 15/11/2026; chưa công bố bộ lịch sử',limits:'Cảm biến giá rẻ; trong tháng 9 đọc thấp hơn khoảng một nửa so với các trạm khác, nên chủ yếu dùng để xem tăng hay giảm.',link:'https://maps.sensor.community/#14/21.0700/105.8240',licence:'ODbL 1.0',color:'#b0602a'},
  };
  const ORDER=[6123215,99385];  // numeric object keys would sort ascending
  const listed=()=>ORDER.map(id=>[id,STATIONS[id]]);
  let now=null,selected=6123215,maps={};
  try{const s=Number(localStorage.getItem('airlake-station'));if(STATIONS[s])selected=s;}catch{}

  const obsOf=id=>now?.observations.find(o=>o.station_id===id);
  function freshness(o){
    if(!o?.observed_at)return{kind:'unknown',text:'Nguồn chưa có số đo.'};
    const age=(Date.now()-Date.parse(o.observed_at))/1000,limit=o.freshness?.threshold_seconds||10800;
    if(o.fetch_status==='fetch_failed')return{kind:'stale',text:`Lần lấy dữ liệu gần nhất không thành công. Đang hiển thị số đo lúc ${time(o.observed_at)}.`};
    if(age>limit)return{kind:'stale',text:`Chưa có số đo mới kể từ ${time(o.observed_at)}. ${o.station_id===6123215?'Trạm thường gửi mỗi giờ.':'Cảm biến thường gửi vài phút một lần.'}`};
    return{kind:'recent',text:o.station_id===6123215?'Trạm gửi số trung bình mỗi giờ; số tiếp theo thường có sau khoảng một giờ.':'Cảm biến gửi số đo vài phút một lần.'};
  }

  function trend(o){
    const rows=now?.recent?.[String(o.station_id)]||[];
    if(rows.length<6||!Number.isFinite(o.pm25))return'Chưa đủ số đo trong 24 giờ qua để so sánh.';
    const mean=rows.reduce((a,r)=>a+r.pm25,0)/rows.length,diff=o.pm25-mean;
    const word=Math.abs(diff)<0.1*mean?'gần bằng':diff>0?'cao hơn':'thấp hơn';
    return`Số đo này ${word} mức trung bình ${num(mean)} µg/m³ của ${rows.length} số đo trong 24 giờ qua tại trạm.`;
  }

  function chart(box,rows,color,opts={}){
    box.replaceChildren();
    if(!rows.length){box.append(node('p',opts.empty||'Chưa có dữ liệu.','empty-chart'));return;}
    const w=Math.max(280,Math.min(1100,box.clientWidth||600)),h=opts.h||180,left=36,right=8,top=10,bottom=26;
    const start=opts.start,end=opts.end,max=Math.ceil(Math.max(10,...rows.map(r=>r.v))*1.15/10)*10;
    const x=t=>left+(t-start)/(end-start)*(w-left-right),y=v=>h-bottom-v/max*(h-top-bottom);
    const s=svg('svg',{viewBox:`0 0 ${w} ${h}`,role:'img','aria-label':opts.label||'Biểu đồ PM2.5'});
    for(let i=0;i<=3;i++){const v=max*i/3;s.append(svg('line',{x1:left,y1:y(v),x2:w-right,y2:y(v),stroke:'#e3e9e5'}),svg('text',{x:left-5,y:y(v)+4,'text-anchor':'end','font-size':10,fill:'#5d706c'},num(v)));}
    for(let k=0;k<=4;k++){const t=start+k*(end-start)/4;s.append(svg('text',{x:x(t),y:h-8,'text-anchor':k===0?'start':k===4?'end':'middle','font-size':10,fill:'#5d706c'},hour(new Date(t).toISOString())));}
    let d='';rows.forEach((r,i)=>{const gap=i&&r.t-rows[i-1].t>(opts.gap||7200000);d+=`${!i||gap?'M':'L'}${x(r.t).toFixed(1)},${y(r.v).toFixed(1)}`;});
    s.append(svg('path',{d,fill:'none',stroke:color,'stroke-width':2,...(opts.dash?{'stroke-dasharray':'6 4'}:{})}));
    if(!opts.dash)rows.forEach(r=>{const c=svg('circle',{cx:x(r.t),cy:y(r.v),r:2.5,fill:color});c.append(svg('title',{},`${hour(new Date(r.t).toISOString())} · ${num(r.v)} µg/m³`));s.append(c);});
    box.append(s);
  }

  function select(id){
    selected=id;try{localStorage.setItem('airlake-station',String(id));}catch{}
    renderStation();
    for(const m of Object.values(maps))m.markers?.forEach(([sid,mk])=>mk.setStyle({weight:sid===id?4:1}));
  }

  function renderTabs(){
    $('station-tabs').replaceChildren(...listed().map(([id,s])=>{
      const b=node('button',undefined,'station-tab');b.type='button';b.setAttribute('role','tab');b.setAttribute('aria-selected',String(Number(id)===selected));
      const o=obsOf(Number(id));b.append(node('span',s.label,'tab-label'),node('span',o?.pm25!=null?`${num(o.pm25)} µg/m³ · ${hour(o.observed_at)}`:'chưa có số đo','tab-value'));
      b.addEventListener('click',()=>{select(Number(id));renderTabs();});return b;}));
  }

  function renderStation(){
    const s=STATIONS[selected],o=obsOf(selected),f=freshness(o),p=$('station-panel');
    p.replaceChildren();
    const head=node('div',undefined,'panel-head');head.append(node('h2',s.label),node('p',s.source,'muted small'));p.append(head);
    const value=node('p',undefined,'reading');value.append(node('strong',o?.pm25!=null?num(o.pm25):'—'),node('span','µg/m³ PM2.5'));p.append(value);
    const when=node('p',undefined,'when');when.append(node('span',o?.observed_at?`Đo lúc ${time(o.observed_at)}`:'Chưa có số đo'),node('span',o?.observed_at?` · ${ago(o.observed_at)}`:'','muted'));p.append(when);
    p.append(node('p',f.text,'fresh '+f.kind));
    if(o?.pm25!=null&&f.kind==='recent'){const lv=node('p',undefined,'level');lv.append(node('span',`Mức tham khảo: ${band(o.pm25)}`,'level-name'),node('span',' theo thang PM2.5 của QĐ 1459 (không phải chỉ số VN_AQI)','muted small'));p.append(lv);}
    if(o)p.append(node('p',trend(o),'trend-line'));
    const box=node('div',undefined,'chart');p.append(node('h3','24 giờ qua','sub-h'),box);
    const end=Date.parse(now.published_at),rows=(now.recent?.[String(selected)]||[]).map(r=>({t:Date.parse(r.observed_at),v:r.pm25}));
    chart(box,rows,s.color,{start:end-86400000,end,empty:'AirLake bắt đầu thu thập liên tục từ 15/11/2026; biểu đồ sẽ đầy dần.',label:`PM2.5 24 giờ qua tại ${s.label}`,gap:selected===99385?2700000:7200000});
    const cta=node('a',`Nhận tin khi bụi ở ${s.label.split(' ·')[0]} vượt ngưỡng`,'button-link');
    const bot=botUrl();cta.href=bot?`${bot}?start=st_${selected}`:'#telegram';if(bot)cta.rel='noopener';p.append(cta);
    const meta=node('p',undefined,'small muted');meta.append(node('span',`Nguồn: ${s.source} · ${s.licence} · `),Object.assign(node('a','Thông tin trạm'),{href:'#khu-vuc'}));p.append(meta);
  }

  function renderRegional(){
    const m=now.model,next=m.forecast.slice(0,24),vals=next.map(r=>r.pm25);
    $('regional-value').textContent=num(m.current.pm25);
    $('regional-meta').textContent=`Ước tính cho ${hour(m.current.valid_at)} · mô hình chạy lúc ${time(m.model_run_at)}`;
    $('regional-range').textContent=vals.length?`24 giờ tới: khoảng ${num(Math.min(...vals))}–${num(Math.max(...vals))} µg/m³`:'';
    if(!$('quanh-ban').hidden)chart($('regional-chart'),next.map(r=>({t:Date.parse(r.valid_at),v:r.pm25})),'#6b5aa6',{start:Date.parse(next[0].valid_at),end:Date.parse(next.at(-1).valid_at),dash:true,h:140,label:'Dự báo PM2.5 khu vực 24 giờ tới'});
  }

  function line(box,rows){
    chart(box,rows.map(r=>({t:Date.parse(r.valid_at),v:r.pm25})),'#6b5aa6',{start:Date.parse(rows[0].valid_at),end:Date.parse(rows.at(-1).valid_at),dash:true,h:220,label:'Dự báo PM2.5 24 giờ tới'});
  }
  function drawForecast(){if(now&&!$('du-bao').hidden)line($('forecast-chart'),now.model.forecast.slice(0,24));}

  function renderForecast(m,v){
    $('forecast-meta').textContent=`Mô hình chạy lúc ${time(m.model_run_at)}, cập nhật khoảng 12 giờ một lần. Dự báo đến ${time(m.forecast.at(-1).valid_at)}.`;
    drawForecast();
    const w=m.relative_windows_24h;
    $('forecast-windows').textContent=w.distinct?`Trong 24 giờ tới, mô hình cho thấy bụi thấp hơn vào khoảng ${hour(w.lower.start_utc)}–${hour(w.lower.end_utc)} (~${num(w.lower.mean)} µg/m³) và cao hơn vào khoảng ${hour(w.higher.start_utc)}–${hour(w.higher.end_utc)} (~${num(w.higher.mean)} µg/m³). Đây là so sánh trong cùng dự báo, không phải lời khuyên an toàn.`:'Trong 24 giờ tới, các khung giờ ban ngày chênh nhau không nhiều (dưới 20%).';
    $('forecast-days').replaceChildren(...m.daily.map(d=>{const tr=node('tr');tr.append(node('th',weekday(d.date_local)),node('td',num(d.mean)),node('td',`${num(d.min)}–${num(d.max)}`));return tr;}));
    $('forecast-days-note').textContent='Dùng để so sánh các ngày với nhau, không phải giá trị chính xác tại từng quận.';
    $('forecast-validation').replaceChildren(node('p','Trong tháng 9/2026, mô hình thường cao hơn số đo tại các trạm; giữa tháng 11 có lúc lại thấp hơn. Vì vậy nên xem dự báo để biết xu hướng tăng hay giảm, không dùng như con số chính xác.'),...v.stations.map(s=>node('p',`${s.name==='Dragon'?STATIONS[6123215].label:STATIONS[99385].label}: chênh trung bình ${s.bias>0?'+':''}${num(s.bias)} µg/m³, ${s.hourly_n} giờ đối chiếu.`,'small')));
  }

  function renderStations(){
    $('station-details').replaceChildren(...listed().map(([id,s])=>{
      const o=obsOf(Number(id)),card=node('article',undefined,'feature-card station-detail');
      card.append(node('h2',s.label),node('p',s.place,'muted'));
      const dl=node('dl',undefined,'facts');
      for(const [k,v] of [['Thiết bị',s.sensor],['Nguồn dữ liệu',`${s.source} · ${s.licence}`],['Nhịp gửi số liệu',s.cadence],['Số đo gần nhất',o?.observed_at?`${num(o.pm25)} µg/m³ lúc ${time(o.observed_at)}`:'chưa có'],['AirLake cập nhật lần cuối',time(o?.fetched_at)],['Lịch sử',s.history],['Lưu ý',s.limits],['Mã kỹ thuật',`${id}`]]){const d=node('div');d.append(node('dt',k),node('dd',v));dl.append(d);}
      const links=node('p',undefined,'quick-links');const src=Object.assign(node('a','Xem trang nguồn','button-link secondary'),{href:s.link,rel:'noopener',target:'_blank'});
      const sub=Object.assign(node('a','Đăng ký cảnh báo','button-link'),{href:botUrl()?`${botUrl()}?start=st_${id}`:'#telegram'});links.append(sub,src);
      card.append(dl,links);return card;}));
  }

  function renderMap(id){
    const el=$(id);if(!now||!window.L||!el||el.offsetParent===null)return;
    if(maps[id]){maps[id].map.invalidateSize();return;}
    const map=L.map(el,{scrollWheelZoom:false}).setView([21.03,105.86],11);
    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:16,attribution:'© OpenStreetMap'}).addTo(map);
    const markers=[];
    for(const o of now.observations){const s=STATIONS[o.station_id];if(!s)continue;
      const mk=L.circleMarker([o.latitude,o.longitude],{radius:o.exact_location?9:12,color:s.color,fillColor:s.color,fillOpacity:.75,weight:o.station_id===selected?4:1}).addTo(map);
      mk.bindTooltip(`${s.label}${o.exact_location?'':' (vị trí gần đúng)'}`);mk.on('click',()=>{select(o.station_id);renderTabs();});markers.push([o.station_id,mk]);}
    if(id==='station-map'){const c=L.circle(now.model.grid_cell.requested,{radius:22500,color:'#6b5aa6',weight:1,dashArray:'6 6',fillOpacity:.04,interactive:false}).addTo(map);c.bindTooltip('Vùng mô hình dự báo, khoảng 45 km');map.fitBounds(c.getBounds(),{padding:[4,4]});}
    maps[id]={map,markers};
  }

  function renderStatus(n){
    const rows=[...n.observations.map(o=>[STATIONS[o.station_id]?.label||o.name,`Đo lúc ${time(o.observed_at)}`,`AirLake lấy lúc ${time(o.fetched_at)}${o.fetch_status==='fetch_failed'?' (lần gần nhất lỗi)':''}`]),['Dự báo khu vực (CAMS)',`Mô hình chạy lúc ${time(n.model.model_run_at)}`,`Có dữ liệu từ ${time(n.model.model_available_at)}`]];
    $('now-status').replaceChildren(...rows.map(r=>{const tr=node('tr');r.forEach((c,i)=>tr.append(node(i?'td':'th',c)));return tr;}));
    $('now-generated').textContent=`AirLake công bố bản mới nhất lúc ${time(n.published_at)}. Trang tự kiểm tra bản mới mỗi 5 phút. Thời gian công bố không phải thời gian đo.`;
  }

  function botUrl(){const b=window.AIRLAKE_CONFIG?.botUrl;return b&&/^https:\/\/t\.me\/[A-Za-z0-9_]{5,32}$/.test(b)?b:null;}
  function notice(){return'';}
  window.AirLakeNow={notice};

  function render(){
    $('home-live').hidden=false;$('legacy-home').hidden=true;$('du-bao-empty').hidden=true;$('du-bao-body').hidden=false;
    const live=now.observations.filter(o=>freshness(o).kind==='recent').length;
    $('coverage-line').textContent=`AirLake có ${now.observations.length} điểm đo công khai: ${ORDER.map(id=>STATIONS[id].label.split(' ·')[0]).join(' và ')}. ${live} điểm có số đo mới.`;
    $('now-attribution').replaceChildren(...[...new Set([...now.observations.map(o=>o.attribution),now.model.source.attribution])].map(a=>node('li',a)));
    renderTabs();renderStation();renderRegional();renderForecast(now.model,now.validation);renderStations();renderStatus(now);renderMap('home-map');renderMap('station-map');drawForecast();
  }

  async function load(){
    const url=window.AIRLAKE_CONFIG?.nowUrl;if(!url)return;
    try{const r=await fetch(url,{cache:'no-cache'});if(!r.ok)throw new Error('http');const next=await r.json();if(next.schema_version!==2)throw new Error('schema');if(now&&next.published_at===now.published_at)return;now=next;}
    catch{if(!now){$('legacy-home').hidden=false;$('notice').textContent='Chưa tải được số đo hiện tại. Trang sẽ thử lại sau ít phút.';$('notice').hidden=false;}return;}
    $('notice').hidden=true;render();
  }

  const bot=botUrl();
  if(bot){const a=$('telegram-link');a.href=bot;a.hidden=false;$('telegram-pending').hidden=true;}
  const api=window.AIRLAKE_CONFIG?.apiBase;
  if(api){const box=$('public-api');if(box){box.hidden=false;for(const [label,path] of [['Mô tả API','/v1'],['Số đo hiện tại (JSON)','/v1/now'],['Gia Lâm theo ngày, tháng 9/2026 (CSV)','/v1/history?granularity=daily&format=csv&limit=100'],['Gia Lâm theo giờ, 7 ngày cuối tháng 9 (JSON)','/v1/history?start=2026-09-23T17:00:00Z&limit=200']]){const li=node('li');const a=node('a',label);a.href=api+path;a.rel='noopener';li.append(a,node('code',' '+path));$('public-api-links').append(li);}}}
  setInterval(()=>{if(document.visibilityState==='visible')load();},300000);
  document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')load();});
  window.addEventListener('hashchange',()=>setTimeout(()=>{if(!now)return;renderMap('home-map');renderMap('station-map');drawForecast();renderStation();renderRegional();},0));
  let resizeTimer;window.addEventListener('resize',()=>{clearTimeout(resizeTimer);resizeTimer=setTimeout(()=>{if(now){drawForecast();renderStation();renderRegional();}},200);});
  window.addEventListener('DOMContentLoaded',load);
})();

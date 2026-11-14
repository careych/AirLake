'use strict';
(() => {
  const $=id=>document.getElementById(id);
  const num=n=>Number.isFinite(n)?new Intl.NumberFormat('vi-VN',{maximumFractionDigits:2}).format(n):'—';
  const time=t=>!Number.isFinite(Date.parse(t))?'Chưa có thời gian':new Intl.DateTimeFormat('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(t));
  const day=d=>d.slice(8,10)+'/'+d.slice(5,7);
  const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
  const svg=(tag,attrs={},text)=>{const n=document.createElementNS('http://www.w3.org/2000/svg',tag);Object.entries(attrs).forEach(([k,v])=>n.setAttribute(k,v));if(text!==undefined)n.textContent=text;return n;};
  const supports=d=>d?.schema_version===2&&d.analytics;
  function range(d,id){const end=Date.parse(d.window_end_utc);return {end,start:Math.max(Date.parse(d.window_start_utc),end-(id==='home-chart'?1:({'24h':1,'7d':7,'30d':30}[$('history-range').value]||7))*86400000)};}
  function bars(id,rows,label,value,readout){
    const box=$(id);box.replaceChildren();
    const good=rows.filter(r=>Number.isFinite(value(r)));
    if(!good.length){box.append(node('p','Chưa đủ số ngày có quan trắc để trình bày so sánh này.','empty-chart'));return;}
    const w=Math.max(300,Math.min(1100,box.clientWidth||940)),h=270,left=47,right=15,top=25,bottom=45,max=Math.ceil(Math.max(10,...good.map(value))*1.1/10)*10;
    const drawing=svg('svg',{viewBox:`0 0 ${w} ${h}`,'aria-hidden':'true'}),step=(w-left-right)/rows.length;
    for(let i=0;i<5;i++){const v=max*i/4,y=h-bottom-v/max*(h-top-bottom);drawing.append(svg('line',{x1:left,y1:y,x2:w-right,y2:y,stroke:'#e0e8e3','stroke-dasharray':'3 4'}),svg('text',{x:left-8,y:y+4,'text-anchor':'end','font-size':11,fill:'#52665d'},num(v)));}
    rows.forEach((r,i)=>{const x=left+i*step;if(Number.isFinite(value(r))){const v=value(r),y=h-bottom-v/max*(h-top-bottom),bar=svg('rect',{x:x+step*.15,y,width:step*.7,height:h-bottom-y,fill:r.complete_24_hours===false?'#9b8256':'#28685b',tabindex:0,role:'button','data-history-value':true,'aria-label':`${label(r)}: ${num(v)} µg/m³`});const show=()=>$(readout).textContent=`${label(r)} · ${num(v)} µg/m³${r.hours_present!==undefined?` · ${r.hours_present}/24 giờ`:r.days!==undefined?` · ${r.days} ngày có quan trắc`:''}`;bar.append(svg('title',{},`${label(r)} · ${num(v)} µg/m³`));bar.addEventListener('focus',show);bar.addEventListener('pointerenter',show);bar.addEventListener('click',show);drawing.append(bar);}if(i%Math.ceil(rows.length/(w<500?5:10))===0||i===rows.length-1)drawing.append(svg('text',{x:x+step/2,y:h-20,'text-anchor':'middle','font-size':11,fill:'#52665d'},label(r)));});
    drawing.append(svg('text',{x:w-right,y:15,'text-anchor':'end','font-size':11,fill:'#52665d'},'µg/m³'));box.append(drawing);
  }
  function heatmap(summary){
    const box=$('history-heatmap');box.replaceChildren();
    const rows=summary.daily.map(r=>r.date_local),values=new Map(summary.heatmap.map(r=>[r.date_local+':'+r.hour_local,r])),max=Math.max(1,...summary.heatmap.map(r=>r.pm25));
    const table=node('table'),head=node('thead'),hr=node('tr');hr.append(node('th','Ngày / giờ'));
    for(let h=0;h<24;h++)hr.append(node('th',String(h).padStart(2,'0')+':00'));head.append(hr);table.append(head);const body=node('tbody');
    for(const date of rows){const tr=node('tr');tr.append(node('th',day(date)));for(let h=0;h<24;h++){const td=node('td'),r=values.get(date+':'+h),cell=node('button',r?num(r.pm25):'—','heat-cell');cell.type='button';cell.tabIndex=values.size&&body.children.length===0&&h===0?0:-1;cell.setAttribute('aria-label',`${date}, ${h}:00: ${r?num(r.pm25)+' µg/m³':'chưa có dữ liệu'}`);cell.title=cell.getAttribute('aria-label');if(r){cell.style.backgroundColor=`rgba(33,105,90,${.08+.75*r.pm25/max})`;cell.style.color=r.pm25/max>.55?'#fff':'#153c34';}else cell.dataset.missing='true';const show=()=>$('heatmap-readout').textContent=cell.getAttribute('aria-label');cell.addEventListener('focus',show);cell.addEventListener('click',show);cell.addEventListener('keydown',e=>{const delta={ArrowLeft:-1,ArrowRight:1,ArrowUp:-24,ArrowDown:24}[e.key];if(delta!==undefined){e.preventDefault();const cells=[...box.querySelectorAll('button')],i=cells.indexOf(cell),next=cells[Math.max(0,Math.min(cells.length-1,i+delta))];cell.tabIndex=-1;next.tabIndex=0;next.focus();}});td.append(cell);tr.append(td);}body.append(tr);}table.append(body);box.append(table);
  }
  function researchRows(d,sid,start,end,granularity){
    if(granularity==='daily'){
      if(new Date(start+7*3600000).toISOString().slice(11,19)!=='00:00:00'||new Date(end+7*3600000).toISOString().slice(11,19)!=='00:00:00')throw Error('daily-midnight');
      return d.daily.filter(r=>r.station_id===sid&&Date.parse(r.date_local+'T00:00:00+07:00')>=start&&Date.parse(r.date_local+'T00:00:00+07:00')<end).map(r=>({...r,sensor_id:d.stations.find(s=>s.id===sid).sensor_id}));
    }
    return d.hourly.filter(r=>r.station_id===sid&&Date.parse(r.observed_hour_utc)>=start&&Date.parse(r.observed_hour_utc)<end).map(r=>({...r,sensor_id:d.stations.find(s=>s.id===sid).sensor_id}));
  }
  function renderPreview(d,station,rows,valid){
    const daily=$('research-granularity').value==='daily';
    const fields=daily?['date_local','pm25','hours_present','coverage_percent']:['observed_hour_utc','pm25','aqi_mvp'];
    const names=daily?['Ngày Việt Nam','PM2.5 (µg/m³)','Giờ có số liệu / 24','Độ phủ (%)']:['Giờ UTC','PM2.5 (µg/m³)','Chỉ số thử nghiệm'];
    const tr=node('tr');names.forEach(n=>tr.append(node('th',n)));$('research-preview-header').replaceChildren(tr);
    $('research-preview').replaceChildren(...rows.slice(0,20).map(r=>{const tr=node('tr');fields.forEach(f=>tr.append(node('td',f==='hours_present'?r[f]+'/24':typeof r[f]==='number'?num(r[f]):r[f]??'—')));return tr;}));
    $('preview-count').textContent=valid?`Hiển thị ${Math.min(20,rows.length)}/${rows.length} dòng đã chọn. Tệp tải xuống chứa toàn bộ các dòng được phép chia sẻ trong khoảng này.`:'Theo ngày: hãy chọn 00:00 ở cả hai đầu khoảng thời gian.';
  }
  function render(d,selected){
    const active=supports(d);$('history-range-label').hidden=!active;$('historical-analytics').hidden=!active;
    $('research-granularity').querySelector('[value=daily]').disabled=!active;
    if(!active)return;
    $('prepared-time').previousElementSibling.textContent='Bản số liệu đã đối chiếu lúc';$('prepared-time').textContent=time(d.dataset_verified_at);$('version-label').textContent=`Số liệu lịch sử tính lúc ${time(d.computed_at)} · Bản dữ liệu ${window.AIRLAKE_CONFIG.sha256.slice(0,12)} · Không tự làm mới.`;
    const label=$('history-range').value,analysis=d.analytics[label],summary=analysis.stations[String(selected)],station=d.stations.find(s=>s.id===selected);
    $('home-period').textContent=`24 giờ cuối trong bản lịch sử: ${time(Date.parse(d.window_end_utc)-86400000)} → ${time(d.window_end_utc)} · giờ Việt Nam`;
    $('home-period-title').textContent='Một ngày trong bản lịch sử đã có';$('trend-period').textContent=`${time(analysis.start_utc)} → ${time(analysis.end_utc)} · giờ Việt Nam`;
    const cards=[['Giờ có quan trắc',`${summary.hour_count}/${summary.expected_hours}`],['Độ phủ trong khoảng',num(summary.coverage_percent)+'%'],['Ngày đủ 24 giờ',String(summary.complete_days)]];
    for(const [key,title] of [['lowest_hour','Giờ ghi nhận thấp nhất'],['highest_hour','Giờ ghi nhận cao nhất']]){const r=summary.extremes[key];cards.push([title,r?`${num(r.pm25_avg)} µg/m³ · ${time(r.observed_hour_utc)}`:'Chưa có số liệu']);}
    $('analytics-summary').replaceChildren(...cards.map(([label,value])=>{const card=node('article',undefined,'feature-card');card.append(node('p',label,'muted'),node('strong',value));return card;}));
    $('daily-method').textContent=`${station.name} · ${summary.daily.length} ngày có số liệu; ${summary.complete_days} ngày đủ 24 mốc giờ. Cột nâu nhạt là ngày thiếu giờ; trung bình không được coi là đánh giá sức khỏe.`;
    bars('daily-chart',summary.daily,r=>day(r.date_local),r=>r.pm25_avg,'daily-readout');heatmap(summary);
    bars('hour-pattern-chart',summary.hour_of_day,r=>String(r.hour_local).padStart(2,'0')+':00',r=>r.pm25_avg,'pattern-readout');
    const weekdays=summary.weekday_weekend;
    $('weekday-comparison').textContent=weekdays.status==='DESCRIPTIVE_ONLY'?`Trong ${weekdays.weekday_days} ngày thường đủ giờ: ${num(weekdays.weekday_mean)} µg/m³; ${weekdays.weekend_days} ngày cuối tuần đủ giờ: ${num(weekdays.weekend_mean)} µg/m³. Mỗi ngày có trọng số bằng nhau. Chỉ là so sánh lịch sử, không dự báo.`:'Cần ít nhất 3 ngày thường và 2 ngày cuối tuần, mỗi ngày đủ 24 giờ, để so sánh có cơ sở.';
    const month=summary.monthly_extremes;
    $('monthly-extremes').textContent=month.status==='OBSERVED_COMPLETE_DAYS_ONLY'?`Trong ${month.complete_days} ngày đủ giờ của tháng đã chọn: cao nhất ${day(month.highest_day.date_local)} (${num(month.highest_day.pm25_avg)} µg/m³), thấp nhất ${day(month.lowest_day.date_local)} (${num(month.lowest_day.pm25_avg)} µg/m³). Chỉ xét các ngày đủ 24 giờ; không suy đoán ngày bị thiếu.`:`Chưa đủ độ phủ để xác định cực trị tháng: ${month.complete_days} ngày đủ giờ. Cần chọn cả tháng và có ít nhất 80% số ngày đủ 24 giờ. Không suy ra kết quả tháng từ vài giờ.`;
    const all=d.analytics['30d'];
    $('area-cards').replaceChildren(...d.stations.map(s=>{const card=node('article',undefined,'feature-card'),status=window.AirLake.freshness(s.latest);card.append(node('h2',s.name),node('span',status.label,'status '+status.kind),node('p',`${s.latest?num(s.latest.pm25)+' µg/m³':'Chưa có số đo'} · ${s.latest?time(s.latest.observed_at):'chưa có thời gian'}`),node('p',`Vị trí: ${s.coordinates.latitude}, ${s.coordinates.longitude}`));const link=node('a','Mở vị trí trên bản đồ ↗');link.href=`https://www.openstreetmap.org/?mlat=${s.coordinates.latitude}&mlon=${s.coordinates.longitude}#map=14/${s.coordinates.latitude}/${s.coordinates.longitude}`;link.target='_blank';link.rel='noopener noreferrer';card.append(link);return card;}));
    const sets=d.stations.map(s=>new Set(d.hourly.filter(r=>r.station_id===s.id&&Date.parse(r.observed_hour_utc)>=Date.parse(analysis.start_utc)&&Date.parse(r.observed_hour_utc)<Date.parse(analysis.end_utc)).map(r=>r.observed_hour_utc)));
    const common=new Set([...sets[0]].filter(t=>sets.every(set=>set.has(t))));
    $('area-window').textContent=`${time(analysis.start_utc)} → ${time(analysis.end_utc)} · ${common.size} giờ chung. Không lấy trung bình các trạm để đại diện toàn thành phố.`;
    $('area-comparison').replaceChildren(...d.stations.map(s=>{const rs=d.hourly.filter(r=>r.station_id===s.id&&common.has(r.observed_hour_utc)),tr=node('tr'),own=analysis.stations[String(s.id)];[s.name,rs.length?num(rs.reduce((a,r)=>a+r.pm25,0)/rs.length)+' µg/m³':'Chưa có giờ chung',common.size,`${own.hour_count}/${own.expected_hours}`].forEach(v=>tr.append(node('td',v)));return tr;}));
    for(const card of $('station-status').children){const id=d.stations.find(s=>s.name===card.querySelector('h2').textContent)?.id;if(!id)continue;const s=d.stations.find(s=>s.id===id);for(const label of ['24h','7d','30d']){const q=d.analytics[label].stations[String(id)];card.append(node('p',`${label==='24h'?'24 giờ':label==='7d'?'7 ngày':'30 ngày'} cuối bản lịch sử: ${q.hour_count}/${q.expected_hours} giờ (${num(q.coverage_percent)}%).`));}card.append(node('p',s.source_last_reported_at?`Nguồn báo quan trắc cuối: ${time(s.source_last_reported_at)}. Chưa đồng nghĩa AirLake đã nhận giá trị mới.`:'Chưa có xác nhận mới từ nguồn.'));}
    $('sample-description').textContent=`Bản lịch sử: ${d.historical_raw_records} bản ghi nguồn hợp lệ → ${d.hourly.length} mốc giờ và ${d.daily.length} dòng theo ngày từ ${d.stations.length} điểm, ${time(d.window_start_utc)} → ${time(d.window_end_utc)}. Không phải dữ liệu trực tiếp.`;
  }
  window.AirLakeHistory={range:(d,id)=>supports(d)?range(d,id):null,researchRows,renderPreview,render};
  $('history-range').addEventListener('change',()=>window.AirLake.render());
  $('research-granularity').addEventListener('change',()=>window.AirLake.render());
  window.addEventListener('airlake-ready',()=>window.AirLake.render());
  if(window.AirLake.getData())window.AirLake.render();
})();

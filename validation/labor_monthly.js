let language='zh';try{language=localStorage.getItem('svu-language')==='en'?'en':'zh'}catch(e){}
const text={zh:{title:'美国月度追踪',scope:'美国 · 私营非农部门全体雇员 · 月度 · 季节调整',method:'小时收入为税前名义工资，并非到手收入、实际购买力或雇主总成本。周工时采用 CES 带薪工时口径，可能包含带薪休假，不等同于实际劳动时间。不纳入 SVU 资产篮子。',wageTitle:'平均小时收入 · 美元/小时',hoursTitle:'平均每周工时 · 小时/周',historyTitle:'月度历史记录',monthHead:'观测月份',wageHead:'美元/小时',hoursHead:'小时/周',rangeLabel:'显示最近月数',revision:'P = 官方初步值。官方修订随新快照更新，原始响应按内容哈希归档。缺月不补值；抓取成功不代表已发布当月数据。',checked:'抓取时间（UTC）',age:'距抓取天数',prelim:'初步值 P',final:'未标注为初步值',month:'观测月份'},en:{title:'United States monthly tracker',scope:'United States · All employees, total private nonfarm · Monthly · Seasonally adjusted',method:'Hourly earnings are gross nominal earnings, not take-home pay, real purchasing power or total employer compensation. CES hours are paid hours and may include paid leave; they are not a measure of hours actually worked. These series are separate from the SVU asset basket.',wageTitle:'Average hourly earnings · USD/hour',hoursTitle:'Average weekly hours · hours/week',historyTitle:'Monthly observations',monthHead:'Observation month',wageHead:'USD/hour',hoursHead:'Hours/week',rangeLabel:'Recent months',revision:'P = official preliminary value. New snapshots incorporate official revisions; raw responses are archived by content hash. Missing months are not filled. Successful retrieval does not imply current-month availability.',checked:'Retrieved (UTC)',age:'Days since retrieval',prelim:'Preliminary P',final:'Not flagged preliminary',month:'Observation month'}};
function chart(id,rows){const s=document.getElementById(id),v=rows.map(r=>r.value),lo=Math.min(...v),hi=Math.max(...v),span=hi-lo||1;let d='',last=null;rows.forEach((r,i)=>{const m=Number(r.month.slice(0,4))*12+Number(r.month.slice(5));d+=(last===null||m!==last+1?'M':'L')+(20+i*560/Math.max(rows.length-1,1))+','+(175-(r.value-lo)*145/span)+' ';last=m});s.setAttribute('viewBox','0 0 600 210');s.setAttribute('aria-label',rows[0].month+' — '+rows.at(-1).month);s.replaceChildren();const p=document.createElementNS('http://www.w3.org/2000/svg','path');p.setAttribute('d',d);p.setAttribute('stroke','#41d9ff');p.setAttribute('stroke-width','2');p.setAttribute('fill','none');s.append(p);for(const [x,y,t] of [[10,16,hi.toFixed(2)],[10,194,lo.toFixed(2)],[90,207,rows[0].month],[480,207,rows.at(-1).month]]){const el=document.createElementNS(s.namespaceURI,'text');el.setAttribute('x',x);el.setAttribute('y',y);el.setAttribute('fill','#adbdcf');el.setAttribute('font-size','12');el.textContent=t;s.append(el)}}
function render(){
 renderAnnual();
 const t=text[language];document.documentElement.lang=language==='en'?'en':'zh-CN';document.title=laborCopy[language].title+' · SVU';
 for(const id of ['title','scope','method','wageTitle','hoursTitle','historyTitle','monthHead','wageHead','hoursHead','rangeLabel','revision'])document.getElementById(id).textContent=t[id];
 document.getElementById('language').textContent=language==='en'?'ZH':'EN';
 document.getElementById('fresh').textContent=t.checked+': '+DATA.retrieved_at+' · '+t.age+': '+Math.max(0,Math.floor((Date.now()-Date.parse(DATA.retrieved_at))/86400000));
 document.getElementById('source').textContent='BLS: CES0500000003 / CES0500000002 · SHA-256: '+DATA.raw_sha256;
 const allW=DATA.series.CES0500000003,allH=DATA.series.CES0500000002;
 syncLaborWindow(allW,'month','bls-monthly');
 const w=laborRows(allW,'month'),h=laborRows(allH,'month');
 for(const [id,rows] of [['wage',w],['hours',h]]){
  const r=rows.at(-1);document.getElementById(id).textContent=r?r.value.toFixed(2):'—';
  document.getElementById(id+'Date').textContent=r?t.month+': '+r.month+' · '+(r.preliminary?t.prelim:t.final):laborCopy[language].empty;
  document.getElementById(id+'Chart').replaceChildren();if(rows.length)chart(id+'Chart',rows);wireLaborChart(document.getElementById(id+'Chart'),rows,'month');
 }
 renderWageBenchmark('monthly',w,'month','hourly');
 const wm=new Map(w.map(r=>[r.month,r])),hm=new Map(h.map(r=>[r.month,r])),body=document.getElementById('rows');body.replaceChildren();
 [...new Set([...wm.keys(),...hm.keys()])].sort().reverse().forEach(m=>{const tr=document.createElement('tr');for(const value of [m,...[wm.get(m),hm.get(m)].map(r=>r?r.value.toFixed(2)+(r.preliminary?' P':''):'—')]){const td=document.createElement('td');td.textContent=value;tr.append(td)}body.append(tr)});
 applyLaborControls();
}

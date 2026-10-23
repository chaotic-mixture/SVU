const laborCopy={zh:{eyebrow:'SVU 研究 / 劳动与收入',title:'劳动与收入',home:'主页',currency:'货币',energy:'能源',hedge:'黄金 / 白银',equity:'股票指数',warning:'研究计价页面。参考单位采用明确的美元基期约定，不代表官方汇率、到手工资或实际购买力。工资数据不加入资产篮子。',controls:'显示控制',dataset:'数据集',annual:'各国年度数据 · ILOSTAT',monthly:'美国月度数据 · BLS',window:'显示窗口',start:'开始',end:'结束',allHistory:'全部历史',display:'图表序列',all:'全选',none:'清空',reset:'恢复默认',raw:'原币工资 / 工时',currencyUnits:'货币类别参考单位',svuUnits:'SVU 参考单位',selection:'勾选只控制图表显示，不改变完整篮子与权重。改变窗口会将趋势首期重置为 100；参考单位金额仍使用固定基期。',years:'近 {n} 年',empty:'所选窗口暂无观测',definitions:'统计口径',noSeries:'请至少选择一条图表序列'},en:{eyebrow:'SVU Research / Labour and earnings',title:'Labour and earnings',home:'Home',currency:'Currencies',energy:'Energy',hedge:'Gold / silver',equity:'Equity indices',warning:'Research pricing view. Reference units use an explicit USD base-date convention; they are not official exchange rates, take-home earnings or real purchasing power. Earnings are not added to the asset basket.',controls:'Display controls',dataset:'Dataset',annual:'Countries · Annual · ILOSTAT',monthly:'United States · Monthly · BLS',window:'Display window',start:'Start',end:'End',allHistory:'All history',display:'Chart series',all:'Select all',none:'Clear',reset:'Reset defaults',raw:'Original earnings / hours',currencyUnits:'Currency-category reference units',svuUnits:'SVU reference units',selection:'Selections control charts only, not basket membership or weights. Window changes rebase trends to 100; reference-unit amounts retain the fixed base date.',years:'Last {n} year(s)',empty:'No observations in this window',definitions:'Definitions',noSeries:'Select at least one chart series'}};
let laborWindow={key:'',periods:[],start:'',end:''};
const laborVisible=new Set(['raw','currency','svu']);
function laborMode(){return document.getElementById('dataset').value}
function periodDate(p,end=false){return p.length===4?p+(end?'-12-31':'-01-01'):p+(end?'-'+new Date(Number(p.slice(0,4)),Number(p.slice(5)),0).getDate():'-01')}
function syncLaborWindow(rows,period,key){
 if((period==='year')!==(laborMode()==='annual'))return;
 const periods=rows.map(r=>r[period]);
 if(laborWindow.key!==key){laborWindow={key,periods,start:periods[0]||'',end:periods.at(-1)||''}}else laborWindow.periods=periods;
 // Preserve the chosen window across country/survey changes, including empty
 // selections. Clamp only when the available extent actually excludes a bound.
 if(periods.length){
  const clamp=value=>!value?null:value<periods[0]?periods[0]:value>periods.at(-1)?periods.at(-1):value;
  laborWindow.start=clamp(laborWindow.start)||periods[0];
  laborWindow.end=clamp(laborWindow.end)||periods.at(-1);
 }
 const {start,end}=laborWindow;
 for(const [id,value] of [['start',start],['end',end]]){
  const date=document.getElementById(id+'Date'),slider=document.getElementById(id+'Slider');
  date.value=value?periodDate(value,id==='end'):'';date.disabled=!periods.length;
  date.min=periods.length?periodDate(periods[0]):'';date.max=periods.length?periodDate(periods.at(-1),true):'';
  const nearest=periods.reduce((best,p,i)=>Math.abs(Date.parse(periodDate(p))-Date.parse(periodDate(value||p)))<Math.abs(Date.parse(periodDate(periods[best]))-Date.parse(periodDate(value||p)))?i:best,0);
  slider.max=Math.max(0,periods.length-1);slider.value=nearest;slider.disabled=!periods.length;
 }
 document.getElementById('windowLabel').textContent=start?start+' — '+end:laborCopy[language].empty;
}
function laborRows(rows,period){if((period==='year')!==(laborMode()==='annual'))return rows;return rows.filter(r=>r[period]>=laborWindow.start&&r[period]<=laborWindow.end)}
function wireLaborChart(svg,rows,period){
 document.getElementById(svg.id+'-tip')?.remove();
 svg.onpointermove=svg.onfocus=svg.onkeydown=null;
 if(!rows.length)return;
 const tip=document.createElement('div');tip.id=svg.id+'-tip';tip.className='chart-tip';tip.setAttribute('role','status');svg.after(tip);
 let current=0;const show=i=>{current=i;const r=rows[i];tip.style.display='block';tip.textContent=r[period]+': '+r.value.toLocaleString(language,{maximumFractionDigits:3})+(r.preliminary?' · P':'')+(r.status?' · '+r.status:'')};
 svg.setAttribute('tabindex','0');svg.onfocus=()=>show(current);
 svg.onpointermove=e=>{const rect=svg.getBoundingClientRect(),ratio=Math.max(0,Math.min(1,((e.clientX-rect.left)/rect.width*600-20)/560));let i=Math.round(ratio*(rows.length-1));if(period==='year'){const year=Number(rows[0].year)+ratio*(Number(rows.at(-1).year)-Number(rows[0].year));i=rows.reduce((best,r,j)=>Math.abs(Number(r.year)-year)<Math.abs(Number(rows[best].year)-year)?j:best,0)}show(i)};
 svg.onpointerleave=svg.onblur=()=>tip.style.display='none';
 svg.onkeydown=e=>{if(['ArrowLeft','ArrowRight'].includes(e.key)){e.preventDefault();show(Math.max(0,Math.min(rows.length-1,current+(e.key==='ArrowRight'?1:-1))))}};
}
function applyLaborControls(){
 const t=laborCopy[language];document.querySelectorAll('[data-labor]').forEach(el=>el.textContent=t[el.dataset.labor]);
 for(const mode of ['annual','monthly']){document.querySelector('#dataset option[value='+mode+']').textContent=t[mode];document.getElementById(mode+'-view').hidden=laborMode()!==mode}
 document.getElementById('annual-selectors').hidden=laborMode()!=='annual';
 document.getElementById('bls-definitions').textContent=t.definitions;
 document.querySelectorAll('[data-years]').forEach(b=>b.textContent=t.years.replace('{n}',b.dataset.years));
 for(const [id,file] of [['home','svu_daily.html'],...['currency','energy','monetary_hedge','equity_index'].map(k=>['nav-'+k,'domains/'+k+'.html'])]){const a=document.getElementById(id);a.hidden=!DATA.navigation_generation;if(DATA.navigation_generation)a.href='./generations/'+DATA.navigation_generation+'/'+file}
 document.getElementById('annual-chart').toggleAttribute('hidden',!laborVisible.has('raw'));
 for(const id of ['wageChart','hoursChart'])document.getElementById(id).toggleAttribute('hidden',!laborVisible.has('raw'));
 if(!laborVisible.has('raw'))for(const id of ['annual-chart','wageChart','hoursChart']){const tip=document.getElementById(id+'-tip');if(tip)tip.style.display='none'}
 document.querySelectorAll('[data-series]').forEach(el=>el.checked=laborVisible.has(el.dataset.series));
}
function initializeLaborControls(){
 document.getElementById('language').onclick=()=>{language=language==='en'?'zh':'en';try{localStorage.setItem('svu-language',language)}catch(e){}render()};
 document.getElementById('dataset').onchange=render;
 document.getElementById('range').onchange=render;
 for(const id of ['start','end']){
  document.getElementById(id+'Slider').oninput=e=>{laborWindow[id]=laborWindow.periods[Number(e.target.value)];if(laborWindow.start>laborWindow.end)laborWindow[id==='start'?'end':'start']=laborWindow[id];render()};
  document.getElementById(id+'Date').onchange=e=>{if(!e.target.value)return;laborWindow[id]=e.target.value.slice(0,laborMode()==='annual'?4:7);if(laborWindow.start>laborWindow.end)laborWindow[id==='start'?'end':'start']=laborWindow[id];render()};
 }
 document.querySelectorAll('[data-years]').forEach(button=>button.onclick=()=>{const ps=laborWindow.periods;if(!ps.length)return;laborWindow.end=ps.at(-1);const cutoff=Number(laborWindow.end.slice(0,4))-Number(button.dataset.years)+1;laborWindow.start=ps.find(p=>Number(p.slice(0,4))>=cutoff)||ps[0];render()});
 document.getElementById('allHistory').onclick=()=>{laborWindow.key='';render()};
 for(const id of ['all','none','reset'])document.getElementById(id).onclick=()=>{laborVisible.clear();if(id!=='none')['raw','currency','svu'].forEach(k=>laborVisible.add(k));if(id==='reset')laborWindow.key='';render()};
 document.querySelectorAll('[data-series]').forEach(input=>input.onchange=()=>{if(input.checked)laborVisible.add(input.dataset.series);else laborVisible.delete(input.dataset.series);render()});
}

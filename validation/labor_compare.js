const comparisonCountries=new Set(['CHN','USA'].filter(c=>c in DATA.ilo.countries));
const comparisonSources={};
let comparisonBasis='svu';
const compareCopy={zh:{select:'国家多选对比',search:'搜索国家名称或代码',all:'全选搜索结果',none:'清空国家',title:'多国同图对比',basis:'对比基准',empty:'请勾选需要对比的国家。',missing:'所选指标或窗口无数据',source:'调查来源',raw:'原币工资趋势（各国窗口首期 = 100）',hours:'实际工时（小时/周）',currency:'货币类别参考单位',svu:'SVU 参考单位',note:'颜色区分国家。原币工资只比较归一化趋势，不能比较不同本币的金额；各国起始年份可能不同。参考单位使用相同固定基期，但统计覆盖和调查定义仍可能不同。缺失不补值，调查断点不连线。',detail:'单国详情',count:'已选国家',noSeries:'当前没有可显示的观测或序列。',noConversion:'没有同期可换算数据；不会使用其他国家的数据代替。',available:'已绘制',points:'个观测点（缺年不连线）',observed:'所选调查原始数据年份',detailHint:'单国详情的国家选择不改变上方已勾选的对比国家。'},en:{select:'Compare countries',search:'Search country name or code',all:'Select search results',none:'Clear countries',title:'Countries on one chart',basis:'Comparison basis',empty:'Select countries to compare.',missing:'No observations for this indicator or window',source:'Survey source',raw:'Original earnings trends (first period per country = 100)',hours:'Hours actually worked (hours/week)',currency:'Currency-category reference units',svu:'SVU reference units',note:'Colors identify countries. Original earnings compare normalized trends, not amounts in different local currencies; starting years may differ. Reference units share a fixed base date, but survey coverage and definitions can differ. Missing observations are not filled and survey breaks are not connected.',detail:'Single-country details',count:'Selected countries',noSeries:'No observations or selected series to display.',noConversion:'No matching convertible observations; another country’s data is never substituted.',available:'Plotted',points:'observations (gaps are not connected)',observed:'Original years for the selected survey',detailHint:'The single-country details selector does not change the checked comparison countries.'}};
function comparisonName(c){return DATA.ilo.country_names[c]?.[language]||DATA.ilo.countries[c]||c}
function comparisonGroups(c,metric){return DATA.ilo.series.filter(g=>g.country===c&&g.metric===metric).sort((a,b)=>b.rows.at(-1).year.localeCompare(a.rows.at(-1).year)||a.source.localeCompare(b.source))}
function selectedComparisonGroups(metric){return [...comparisonCountries].sort().flatMap(c=>{const groups=comparisonGroups(c,metric);const key=c+'|'+metric;const g=groups.find(g=>g.source===comparisonSources[key])||groups[0];return g?[g]:[]})}
function renderCountryChecks(){
 const t=compareCopy[language],search=document.getElementById('country-search');search.placeholder=t.search;search.setAttribute('aria-label',t.search);
 document.getElementById('compare-country-label').textContent=t.select+' · '+comparisonCountries.size;
 document.getElementById('countries-all').textContent=t.all;document.getElementById('countries-none').textContent=t.none;
 const query=search.value.trim().toLocaleLowerCase(),box=document.getElementById('country-checks');box.replaceChildren();
 for(const c of Object.keys(DATA.ilo.countries).sort((a,b)=>comparisonName(a).localeCompare(comparisonName(b),language))){
  const names=DATA.ilo.country_names[c]||{};
  if(query&&![c,DATA.ilo.countries[c],names.zh||'',names.en||''].join(' ').toLocaleLowerCase().includes(query))continue;
  const label=benchmarkNode('label',undefined,box);label.className='check';const input=benchmarkNode('input',undefined,label);input.type='checkbox';input.dataset.country=c;input.checked=comparisonCountries.has(c);benchmarkNode('span',comparisonName(c)+' · '+c,label);
  input.onchange=()=>{input.checked?comparisonCountries.add(c):comparisonCountries.delete(c);render()};
 }
}
function renderCountryComparison(metric,groups){
 renderCountryChecks();const t=compareCopy[language],root=document.getElementById('country-comparison');root.replaceChildren();
 document.getElementById('annual-country').textContent=t.detail;
 benchmarkNode('h2',t.title,root);
 document.getElementById('single-country-summary').textContent=t.detail;
 const basisLabel=benchmarkNode('label',t.basis+' · ',root),basisSelect=benchmarkNode('select',undefined,basisLabel);basisSelect.id='comparison-basis';basisSelect.style.maxWidth='100%';
 annualOptions(basisSelect,(metric==='hours'?['raw']:['raw','currency','svu']).map(k=>[k,k==='raw'&&metric==='hours'?t.hours:t[k]]),metric==='hours'?'raw':comparisonBasis);
 basisSelect.onchange=()=>{comparisonBasis=basisSelect.value;laborVisible.add(comparisonBasis);render()};
 benchmarkNode('p',t.note,root);
 if(!comparisonCountries.size){benchmarkNode('p',t.empty,root);return}
 const ordered=[...comparisonCountries].sort();
 const color=c=>{const n=[...c].reduce((v,ch)=>v*26+ch.charCodeAt(0)-65,0);return 'hsl('+((n*137.507764)%360).toFixed(3)+' 75% 70%)'};
 const sources=benchmarkNode('details',undefined,root);sources.open=false;benchmarkNode('summary',t.source+' · '+comparisonCountries.size,sources);
 for(const c of ordered){const choices=comparisonGroups(c,metric),g=groups.find(g=>g.country===c),label=benchmarkNode('label',undefined,sources);label.style.display='block';label.style.margin='10px 0';benchmarkNode('span',comparisonName(c)+' · ',label);
  if(!g){benchmarkNode('span',t.missing,label);continue}
  const select=benchmarkNode('select',undefined,label);select.style.maxWidth='100%';select.dataset.comparisonSource=c;select.setAttribute('aria-label',comparisonName(c)+' · '+t.source);
  annualOptions(select,choices.map(g=>[g.source,DATA.ilo.sources[g.source]+' · '+g.source]),g.source);
  select.onchange=()=>{comparisonSources[c+'|'+metric]=select.value;render()};
 }
 const activeBasis=metric==='hours'?'raw':comparisonBasis;
 const status=benchmarkNode('div',undefined,root);status.id='comparison-status';status.setAttribute('role','status');
 for(const c of ordered){
  const g=groups.find(g=>g.country===c),windowRows=g?laborRows(g.rows,'year'):[],rows=windowRows.filter(r=>activeBasis==='raw'||r.conversion);
  const line=benchmarkNode('p',undefined,status);line.dataset.countryStatus=c;
  if(!laborVisible.has(activeBasis)){line.textContent=comparisonName(c)+' · '+t.noSeries;continue}
  if(rows.length){line.textContent=comparisonName(c)+' · '+t.available+': '+rows.length+' '+t.points+' · '+rows[0].year+'–'+rows.at(-1).year;line.style.color=color(c)}
  else{line.textContent=comparisonName(c)+' · '+(g&&windowRows.length&&activeBasis!=='raw'?t.noConversion:t.missing)+(g?' · '+t.observed+': '+g.rows[0].year+'–'+g.rows.at(-1).year:'');line.className='warning'}
 }
 let drawn=false;
 for(const basis of [metric==='hours'?'raw':comparisonBasis]){
  if(!laborVisible.has(basis)||(metric==='hours'&&basis!=='raw'))continue;
  const lines=groups.map(g=>{const rows=laborRows(g.rows,'year').filter(r=>basis==='raw'||r.conversion);return {country:g.country,rows:rows.map(r=>({...r,plot:basis==='raw'?(metric==='hours'?r.value:100*r.value/rows[0].value):r.conversion[basis]}))}}).filter(g=>g.rows.length);
  benchmarkNode('h3',basis==='raw'&&metric==='hours'?t.hours:t[basis],root);
  if(!lines.length){benchmarkNode('p',t.noSeries,root);continue}drawn=true;
  const wrap=benchmarkNode('div',undefined,root);wrap.className='chart';const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');wrap.append(svg);svg.setAttribute('viewBox','0 0 800 330');svg.setAttribute('role','img');svg.setAttribute('aria-label',t.title+' · '+t[basis]);svg.dataset.comparisonBasis=basis;
  const points=lines.flatMap(g=>g.rows),years=points.map(r=>Number(r.year)),values=points.map(r=>r.plot),first=Math.min(...years),last=Math.max(...years),low=Math.min(...values),high=Math.max(...values),span=high-low||1;
  const x=r=>70+(Number(r.year)-first)*690/Math.max(1,last-first),y=v=>275-(v-low)*235/span;
  function shape(tag,attrs,txt){const e=document.createElementNS(svg.namespaceURI,tag);for(const [k,v] of Object.entries(attrs))e.setAttribute(k,v);if(txt!==undefined)e.textContent=txt;svg.append(e);return e}
  for(const ratio of [0,.5,1]){const value=low+ratio*span;shape('line',{x1:70,x2:760,y1:y(value),y2:y(value),stroke:'#2a3a53'});shape('text',{x:5,y:y(value)+4,fill:'#8fa3bd','font-size':12},value.toFixed(1))}
  for(const g of lines){let d='',previous=null;for(const r of g.rows){d+=(previous===Number(r.year)-1&&r.status!=='B'?'L':'M')+x(r)+','+y(r.plot)+' ';previous=Number(r.year);const dot=shape('circle',{cx:x(r),cy:y(r.plot),r:3,fill:color(g.country),tabindex:0});const title=document.createElementNS(svg.namespaceURI,'title');title.textContent=comparisonName(g.country)+' · '+r.year+': '+r.plot.toFixed(3);dot.append(title);dot.onfocus=dot.onpointerenter=()=>{tip.textContent=title.textContent;tip.style.display='block'}}shape('path',{d,stroke:color(g.country),'stroke-width':2,fill:'none','data-country':g.country})}
  shape('text',{x:70,y:315,fill:'#8fa3bd','font-size':12},first);shape('text',{x:720,y:315,fill:'#8fa3bd','font-size':12},last);
  const tip=benchmarkNode('div',undefined,root);tip.className='chart-tip';tip.setAttribute('role','status');const legend=benchmarkNode('div',undefined,root);legend.className='actions';for(const g of lines){const label=benchmarkNode('span','● '+comparisonName(g.country)+' ('+g.rows[0].year+'–'+g.rows.at(-1).year+')',legend);label.style.color=color(g.country)}
 }
 if(!drawn)benchmarkNode('p',t.noSeries,root);
 benchmarkNode('p',t.detailHint,root);
}
document.getElementById('country-search').oninput=renderCountryChecks;
document.getElementById('countries-all').onclick=()=>{document.querySelectorAll('[data-country]').forEach(el=>{if(el.tagName==='INPUT')comparisonCountries.add(el.dataset.country)});render()};
document.getElementById('countries-none').onclick=()=>{comparisonCountries.clear();render()};

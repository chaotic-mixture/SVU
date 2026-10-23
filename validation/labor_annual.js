// Embedded into the standalone report; all upstream strings use textContent.
const annualText={
  zh:{title:'各国劳动与收入',coverage:'有数据的国家和地区',country:'国家或地区',metric:'指标',survey:'调查来源',hourly:'雇员平均小时工资（本币/小时）',monthly:'雇员平均月工资（本币/月）',hours:'雇员平均每周实际工作时间（小时/周）',scope:'ILOSTAT · 年度 · 男女合计。工资与工时可能来自不同调查；工资为本币金额，不作跨国金额排名。各国覆盖范围和定义不完全一致。',missing:'此国家或地区暂无该指标；可切换指标查看。',year:'观测年份',value:'数值',status:'官方状态',notes:'官方备注（原文）',source:'官方调查名称（原文）',history:'年度历史记录',none:'无标记',warning:'年度数据存在发布滞后。最新年份不等于当前年份；无数据不补值。不同调查分别展示，不自动拼接。官方原始备注保留英文，以避免误译统计定义。',old:'距最新观测年份',years:'年',detail:'来源与校验记录',link:'ILOSTAT 数据说明',break:'序列断点',unreliable:'可靠性不足',adjusted:'调整值',model:'模型外推',real:'实际值',imputed:'插补值'},
  en:{title:'Labour and earnings by country',coverage:'Countries and territories with observations',country:'Country or territory',metric:'Indicator',survey:'Survey source',hourly:'Mean hourly earnings of employees (local currency/hour)',monthly:'Mean monthly earnings of employees (local currency/month)',hours:'Mean weekly hours actually worked per employee (hours/week)',scope:'ILOSTAT · Annual · Both sexes. Earnings and hours may come from different surveys. Earnings are in local currency, without a cross-country monetary ranking. National coverage and definitions differ.',missing:'No observations for this indicator in this country or territory. Try another indicator.',year:'Observation year',value:'Value',status:'Official status',notes:'Official notes (original)',source:'Official survey name (original)',history:'Annual observations',none:'Unflagged',warning:'Annual releases have a publication lag. The latest observation need not be the current year; missing values are not filled. Surveys are shown separately and never spliced. Official metadata is preserved in its original English.',old:'Years since latest observation',years:'',detail:'Sources and integrity records',link:'ILOSTAT data documentation',break:'Break in series',unreliable:'Unreliable',adjusted:'Adjusted',model:'Model-based extrapolation',real:'Real value',imputed:'Imputation'}
};
function annualEl(id){return document.getElementById(id)}
function annualOptions(select,options,preferred){select.replaceChildren();for(const [value,label] of options){const o=document.createElement('option');o.value=value;o.textContent=label;select.append(o)}if(options.some(x=>x[0]===preferred))select.value=preferred}
function renderAnnual(){
  const d=DATA.ilo,t=annualText[language];
  annualEl('annual-fresh').textContent=text[language].checked+': '+DATA.retrieved_at;
  for(const key of ['title','coverage','country','metric','survey','scope','warning','history','year','value','status','notes','detail','link'])annualEl('annual-'+key).textContent=t[key];
  annualEl('annual-coverage').textContent=t.coverage+': '+Object.keys(d.countries).length;
  const country=annualEl('annual-country-select'),metric=annualEl('annual-metric-select'),survey=annualEl('annual-survey-select');
  const selectedCountry=country.value||('CHN' in d.countries?'CHN':Object.keys(d.countries)[0]);
  const names=Object.entries(d.countries).map(([c,n])=>[c,(d.country_names[c]?.[language]||n)+' · '+c]).sort((a,b)=>a[1].localeCompare(b[1],language));
  annualOptions(country,names,selectedCountry);
  annualOptions(metric,['hourly','monthly','hours'].map(k=>[k,t[k]]),metric.value||'monthly');
  const groups=d.series.filter(s=>s.country===country.value&&s.metric===metric.value).sort((a,b)=>b.rows.at(-1).year.localeCompare(a.rows.at(-1).year)||a.source.localeCompare(b.source));
  annualOptions(survey,groups.map(g=>[g.source,d.sources[g.source]+' · '+g.source+' · '+g.rows[0].year+'–'+g.rows.at(-1).year]),survey.value);
  survey.disabled=!groups.length;
  let group=groups.find(g=>g.source===survey.value);
  const compareGroups=selectedComparisonGroups(metric.value);
  const windowRows=compareGroups.length?[...new Set([...compareGroups.flatMap(g=>g.rows.map(r=>r.year)),...(group?.rows||[]).map(r=>r.year)])].sort().map(year=>({year})):group?.rows||[];
  syncLaborWindow(windowRows,'year',country.value+'|'+metric.value+'|'+survey.value+'|'+[...comparisonCountries].sort().join(',')+'|'+JSON.stringify(comparisonSources));
  renderCountryComparison(metric.value,compareGroups);
  if(group){const filtered=laborRows(group.rows,'year');group=filtered.length?{...group,rows:filtered}:null}
  const body=annualEl('annual-rows'),svg=annualEl('annual-chart');
  body.replaceChildren();svg.replaceChildren();
  annualEl('annual-latest').textContent=group?group.rows.at(-1).value.toLocaleString(language,{maximumFractionDigits:3}):'—';
  annualEl('annual-latest-label').textContent=t[metric.value];
  annualEl('annual-date').textContent=group?t.year+': '+group.rows.at(-1).year+' · '+t.old+': '+Math.max(0,new Date().getUTCFullYear()-Number(group.rows.at(-1).year))+' '+t.years:t.missing;
  if(group){
    const rows=group.rows,v=rows.map(r=>r.value),lo=Math.min(...v),hi=Math.max(...v),first=Number(rows[0].year),last=Number(rows.at(-1).year);let path='',previous=null;
    for(const r of rows){const y=Number(r.year);path+=(previous===y-1&&r.status!=='B'?'L':'M')+(20+(y-first)*560/Math.max(1,last-first))+','+(175-(r.value-lo)*145/(hi-lo||1))+' ';previous=y}
    svg.setAttribute('viewBox','0 0 600 210');svg.setAttribute('aria-label',t[metric.value]+' '+first+'–'+last);
    const line=document.createElementNS(svg.namespaceURI,'path');line.setAttribute('d',path);line.setAttribute('stroke','#41d9ff');line.setAttribute('fill','none');line.setAttribute('stroke-width','2');svg.append(line);
    for(const r of rows){const dot=document.createElementNS(svg.namespaceURI,'circle');dot.setAttribute('cx',20+(Number(r.year)-first)*560/Math.max(1,last-first));dot.setAttribute('cy',175-(r.value-lo)*145/(hi-lo||1));dot.setAttribute('r',2.5);dot.setAttribute('fill','#41d9ff');svg.append(dot)}
    for(const [x,y,label] of [[10,16,hi.toFixed(2)],[10,194,lo.toFixed(2)],[90,207,first],[480,207,last]]){const el=document.createElementNS(svg.namespaceURI,'text');el.setAttribute('x',x);el.setAttribute('y',y);el.setAttribute('fill','#adbdcf');el.setAttribute('font-size','12');el.textContent=label;svg.append(el)}
    const statusKeys={A:'adjusted',B:'break',M:'model',U:'unreliable',R:'real',I:'imputed'};
    for(const r of [...rows].reverse()){const tr=document.createElement('tr');const status=r.status?(t[statusKeys[r.status]]||d.statuses[r.status]||r.status)+' ('+r.status+')':t.none;for(const value of [r.year,r.value.toLocaleString(language,{maximumFractionDigits:3}),status,r.notes.map(n=>n.text+' ['+n.code+']').join('; ')]){const td=document.createElement('td');td.textContent=value;tr.append(td)}body.append(tr)}
  }
  wireLaborChart(svg,group?.rows||[],'year');
  renderWageBenchmark('annual',group?.rows||[],'year',metric.value);
  annualEl('annual-hashes').textContent=Object.entries(d.raw_sha256).map(([k,v])=>k+' · SHA-256: '+v).join('\n');
}
for(const id of ['country','metric','survey'])annualEl('annual-'+id+'-select').onchange=render;

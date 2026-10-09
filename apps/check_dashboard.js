
let userSort={sort:"cpu_hours",direction:"desc"};
let accountSort={sort:"name",direction:"asc"};
let lastUsers=[];let lastAccounts=[];
async function getJSON(url){const r=await fetch(url);if(!r.ok)throw new Error(`${url}: HTTP ${r.status} ${await r.text()}`);return r.json()}
function rangeStart(v){const now=new Date(),ms={"24h":864e5,"7d":7*864e5,"30d":30*864e5,"90d":90*864e5,"180d":180*864e5,"1y":365*864e5}[v];return new Date(now-ms)}
function toISOInput(v){const d=new Date(v);d.setMinutes(d.getMinutes()-d.getTimezoneOffset());return d.toISOString().slice(0,16)}
function getRange(){const range=document.getElementById("range").value;if(range!=="custom")return {start:rangeStart(range).toISOString(),end:new Date().toISOString()};const s=document.getElementById("custom-start").value,e=document.getElementById("custom-end").value;if(!s||!e)throw new Error("Please enter both a custom start and end time.");const start=new Date(s),end=new Date(e);if(!(end>start))throw new Error("Custom end time must be later than start time.");return {start:start.toISOString(),end:end.toISOString()}}
function baseLayout(title){return{title,margin:{l:65,r:25,t:55,b:55},hovermode:"x unified",xaxis:{automargin:true},yaxis:{automargin:true},legend:{orientation:"h"}}}
function fmt(v){return Number(v||0).toLocaleString(undefined,{maximumFractionDigits:1})}
function safe(v){return v==null?"":String(v).replace(/[&<>\"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m]))}
function pieData(rows,metric,label){const sorted=[...rows].sort((a,b)=>Number(b[metric]||0)-Number(a[metric]||0));const top=sorted.slice(0,10);const other=sorted.slice(10).reduce((s,r)=>s+Number(r[metric]||0),0);const labels=top.map(r=>r[label]);const values=top.map(r=>Number(r[metric]||0));if(other>0){labels.push("Other");values.push(other)}return {labels,values}}
function renderPie(id,rows,metric,label,title){const p=pieData(rows,metric,label);Plotly.react(id,[{labels:p.labels,values:p.values,type:"pie",hole:.35,textinfo:"label+percent",hovertemplate:"%{label}: %{value:.1f}<extra></extra>"}],{title,margin:{l:20,r:20,t:55,b:20},legend:{orientation:"h"}},{responsive:true})}
async function loadOverview(){try{const {start,end}=getRange(),interval=document.getElementById("interval").value;const [summary,gpu,metadata,series]=await Promise.all([getJSON("/api/summary/"),getJSON("/api/gpu/"),getJSON("/api/metadata/"),getJSON(`/api/timeseries/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&interval=${encodeURIComponent(interval)}`)]);document.getElementById("job-count").textContent=summary.job_count??0;document.getElementById("running-count").textContent=summary.running_count??0;document.getElementById("pending-count").textContent=summary.pending_count??0;document.getElementById("completed-count").textContent=summary.completed_count??0;document.getElementById("gpu-count").textContent=gpu.allocated_gpu_count??0;document.getElementById("refreshed").textContent=metadata.refreshed_at?`Snapshot: ${new Date(metadata.refreshed_at).toLocaleString()}`:"";const rows=series.results||[],x=rows.map(r=>r.timestamp);Plotly.react("resource-chart",[{x,y:rows.map(r=>r.allocated_cpus),name:"Allocated CPUs",type:"scatter",mode:"lines"},{x,y:rows.map(r=>r.allocated_gpus),name:"Allocated GPUs",type:"scatter",mode:"lines",yaxis:"y2"}],{...baseLayout("Allocated resources over time"),yaxis:{title:"CPUs"},yaxis2:{title:"Concurrent GPUs",overlaying:"y",side:"right"}},{responsive:true});Plotly.react("job-flow-chart",[{x,y:rows.map(r=>r.active_job_count),name:"Active jobs",type:"scatter",mode:"lines"},{x,y:rows.map(r=>r.jobs_started),name:"Started",type:"bar"},{x,y:rows.map(r=>r.jobs_completed),name:"Completed",type:"bar"},{x,y:rows.map(r=>r.jobs_failed),name:"Failed",type:"bar"}],{...baseLayout("Job activity"),barmode:"group"},{responsive:true});Plotly.react("gpu-chart",[{x,y:rows.map(r=>r.allocated_gpus),name:"Concurrent allocated GPUs",type:"scatter",mode:"lines",fill:"tozeroy"}],baseLayout("Concurrent GPU allocation"),{responsive:true});document.getElementById("error").textContent=""}catch(e){document.getElementById("error").textContent="Unable to load dashboard: "+e.message}}
async function loadUsers(){try{const {start,end}=getRange(),u=document.getElementById("user-filter").value,a=document.getElementById("user-account-filter").value;lastUsers=await getJSON(`/api/users/usage/?user=${encodeURIComponent(u)}&account=${encodeURIComponent(a)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=10000&sort=${encodeURIComponent(userSort.sort)}&direction=${userSort.direction}`);document.getElementById("users-body").innerHTML=lastUsers.map(r=>`<tr><td>${safe(r.user)}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.allocated_nodes)}</td><td class="num">${fmt(r.cpu_hours)}</td><td class="num">${fmt(r.gpu_hours)}</td><td class="num">${fmt(r.memory_gb_hours)}</td><td class="num">${fmt(r.gpu_memory_gb)}</td></tr>`).join("");renderPie("users-cpu-pie",lastUsers,"cpu_hours","user","Users: CPU-hour share");renderPie("users-gpu-pie",lastUsers,"gpu_hours","user","Users: GPU-hour share");updateSortIndicators("sort-",userSort)}catch(e){document.getElementById("error").textContent="Unable to load users: "+e.message}}
async function loadAccounts(){try{const {start,end}=getRange(),filter=document.getElementById("account-filter").value.trim();const [tree,direct]=await Promise.all([getJSON(`/api/accounts/tree/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&sort=name&direction=asc`),getJSON(`/api/accounts/?account=${encodeURIComponent(filter)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=10000&sort=${encodeURIComponent(accountSort.sort)}&direction=${accountSort.direction}`)]);lastAccounts=tree;const visible=hierarchicalAccounts(tree,filter);document.getElementById("accounts-body").innerHTML=visible.map(r=>`<tr><td class="indent" style="--level:${Number(r.display_level||0)}">${safe(r.account)}</td><td>${safe(r.display_parent||"")}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.allocated_nodes)}</td><td class="num">${fmt(r.cpu_hours)}</td><td class="num">${fmt(r.gpu_hours)}</td><td class="num">${fmt(r.memory_gb_hours)}</td><td class="num">${fmt(r.gpu_memory_gb)}</td></tr>`).join("");renderPie("accounts-cpu-pie",direct,"cpu_hours","account","Accounts: CPU-hour share");renderPie("accounts-gpu-pie",direct,"gpu_hours","account","Accounts: GPU-hour share");updateSortIndicators("asort-",accountSort)}catch(e){document.getElementById("error").textContent="Unable to load accounts: "+e.message}}
function hierarchicalAccounts(rows,filter){
  const byName=new Map(rows.filter(r=>r.account).map(r=>[String(r.account),{...r,children:[]} ]));
  const roots=[];
  for(const r of byName.values()){
    const parent=String(r.parent||"");
    if(parent && parent.toLowerCase()!=="root" && byName.has(parent)) byName.get(parent).children.push(r);
    else roots.push(r);
  }
  const rootRows=[...byName.values()].filter(r=>String(r.account).toLowerCase()==="root");
  // Promote root's children to the visible top level; never display the default root row.
  const visibleRoots=[];
  for(const r of roots){if(String(r.account).toLowerCase()==="root") visibleRoots.push(...r.children);else visibleRoots.push(r)}
  for(const root of rootRows){for(const child of root.children){if(!visibleRoots.includes(child))visibleRoots.push(child)}}
  const sortValue=r=>{const key=accountSort.sort; if(key==="name"||key==="account")return String(r.account||"").toLocaleLowerCase();return Number(r[key]??(key==="nodes"?r.allocated_nodes:0))||0};
  const cmp=(a,b)=>{const av=sortValue(a),bv=sortValue(b);let c=typeof av==="string"?av.localeCompare(bv):av-bv;return accountSort.direction==="desc"?-c:c};
  const flat=[];
  function visit(r,level,parentName){
    flat.push({...r,display_level:level,display_parent:parentName});
    r.children.sort(cmp).forEach(child=>visit(child,level+1,r.account));
  }
  visibleRoots.sort(cmp).forEach(r=>visit(r,0,""));
  if(!filter)return flat;
  const pattern=filter.replace(/[.+^${}()|[\]\\]/g,"\\$&").replace(/\*/g,".*").replace(/%/g,".*");
  let re;try{re=new RegExp(pattern,"i")}catch(e){re=new RegExp(filter.replace(/[.*+?^${}()|[\]\\]/g,"\\$&"),"i")}
  const matched=new Set(flat.filter(r=>re.test(String(r.account||""))).map(r=>r.account));
  // Keep ancestors of matches so filtered rows still communicate their place in the hierarchy.
  for(const r of flat){if(matched.has(r.account)){let p=r.parent,guard=0;while(p&&guard++<100){matched.add(p);const ancestor=byName.get(String(p));p=ancestor?ancestor.parent:null}}}
  return flat.filter(r=>matched.has(r.account)&&String(r.account).toLowerCase()!=="root");
}
function updateSortIndicators(prefix,state){document.querySelectorAll(`[id^="${prefix}"]`).forEach(x=>x.textContent="");const el=document.getElementById(prefix+state.sort);if(el)el.textContent=state.direction==="asc"?"▲":"▼"}
function toggleSort(state,key){if(state.sort===key)state.direction=state.direction==="asc"?"desc":"asc";else{state.sort=key;state.direction=key==="name"||key==="user"?"asc":"desc"}}
function sortUsers(key){toggleSort(userSort,key);loadUsers()}
function sortAccounts(key){toggleSort(accountSort,key);loadAccounts()}
document.getElementById("range").addEventListener("change",()=>{const custom=document.getElementById("range").value==="custom";document.getElementById("custom-range").classList.toggle("visible",custom);if(custom&&!document.getElementById("custom-start").value){const end=new Date();document.getElementById("custom-end").value=toISOInput(end);document.getElementById("custom-start").value=toISOInput(new Date(end.getTime()-7*864e5))}else loadActiveTab()});document.getElementById("apply-range").addEventListener("click",loadActiveTab);document.getElementById("interval").addEventListener("change",loadOverview);
function loadActiveTab(){const active=document.querySelector(".tab.active").dataset.panel;if(active==="overview")loadOverview();else if(active==="users")loadUsers();else loadAccounts()}
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));b.classList.add("active");document.getElementById(b.dataset.panel).classList.add("active");loadActiveTab()}));
loadOverview();

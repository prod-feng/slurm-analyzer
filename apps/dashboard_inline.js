
let userSort={sort:"cpu_hours",direction:"desc"};
let accountSort={sort:"name",direction:"asc"};
let partitionSort={sort:"cpu_hours",direction:"desc"};
let pendingLoads=0;
function setLoading(active){pendingLoads=Math.max(0,pendingLoads+(active?1:-1));const indicator=document.getElementById("loading-indicator");if(indicator)indicator.classList.toggle("visible",pendingLoads>0);const button=document.getElementById("apply-range");if(button)button.disabled=pendingLoads>0;}
let lastPartitions=[];
let lastUsers=[];let lastAccounts=[];
async function getJSON(url){const r=await fetch(url);if(!r.ok)throw new Error(`${url}: HTTP ${r.status} ${await r.text()}`);return r.json()}
function rangeStart(v){const now=new Date(),ms={"24h":864e5,"7d":7*864e5,"30d":30*864e5,"90d":90*864e5,"180d":180*864e5,"1y":365*864e5}[v];return new Date(now-ms)}
function toISOInput(v){const d=new Date(v);d.setMinutes(d.getMinutes()-d.getTimezoneOffset());return d.toISOString().slice(0,16)}
function getRange(){const range=document.getElementById("range").value;if(range!=="custom")return {start:rangeStart(range).toISOString(),end:new Date().toISOString()};const s=document.getElementById("custom-start").value,e=document.getElementById("custom-end").value;if(!s||!e)throw new Error("Please enter both a custom start and end time.");const start=new Date(s),end=new Date(e);if(!(end>start))throw new Error("Custom end time must be later than start time.");return {start:start.toISOString(),end:end.toISOString()}}
function baseLayout(title){return{title,margin:{l:65,r:25,t:55,b:55},hovermode:"x unified",xaxis:{automargin:true},yaxis:{automargin:true},legend:{orientation:"h"}}}
function fmt(v){return Number(v||0).toLocaleString(undefined,{maximumFractionDigits:1})}
function safe(v){return v==null?"":String(v).replace(/[&<>\"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m]))}
function pieData(rows,metric,label){const sorted=[...rows].sort((a,b)=>Number(b[metric]||0)-Number(a[metric]||0));const top=sorted.slice(0,10);const other=sorted.slice(10).reduce((s,r)=>s+Number(r[metric]||0),0);const labels=top.map(r=>r[label]);const values=top.map(r=>Number(r[metric]||0));if(other>0){labels.push("Other");values.push(other)}return {labels,values}}
function renderPie(id,rows,metric,label,title){
  const sorted=[...rows].sort((a,b)=>Number(b[metric]||0)-Number(a[metric]||0));
  const total=rows.reduce((sum,r)=>sum+Number(r[metric]||0),0);
  const totalText=metric==="jobs"?Math.round(total).toLocaleString():total.toLocaleString(undefined,{maximumFractionDigits:1});
  const units={jobs:"jobs",allocated_nodes:"node allocations",cpu_hours:"CPU-hours",gpu_hours:"GPU-hours",memory_gb_hours:"GiB-hours",total_tasks:"tasks",disk_read_gib:"GiB data read",disk_write_gib:"GiB data written",elapsed_hours:"hours"};
  const unit=units[metric]||metric.replaceAll("_"," ");
  const titleText=`${title}<br><sup>Total: ${totalText} ${unit}</sup>`;
  // For any metric, a single account/user with >=80% of the total makes a
  // conventional pie hard to read. Show the dominant contributor vs everyone
  // else, plus a second pie expanding the remaining contributors.
  if(total>0 && sorted.length>1 && Number(sorted[0][metric]||0)/total>=0.8){
    const dominant=sorted[0],rest=sorted.slice(1),dominantValue=Number(dominant[metric]||0);
    const restTotal=rest.reduce((sum,r)=>sum+Number(r[metric]||0),0);
    const restPie=pieData(rest,metric,label);
    const traces=[
      {type:"pie",labels:[dominant[label],"All others"],values:[dominantValue,restTotal],domain:{x:[0,0.47],y:[0,1]},textinfo:"label+percent",hovertemplate:`%{label}: %{value:,.1f} ${unit} (%{percent})<extra></extra>`,sort:false},
      {type:"pie",labels:restPie.labels,values:restPie.values,domain:{x:[0.53,1],y:[0,1]},textinfo:"label+percent",hovertemplate:`%{label}: %{value:,.1f} ${unit} (%{percent} of remaining share)<extra></extra>`}
    ];
    Plotly.react(id,traces,{title:{text:titleText},annotations:[{text:"Overall share",x:0.235,y:0.98,xref:"paper",yref:"paper",showarrow:false},{text:`All others (${restTotal.toLocaleString(undefined,{maximumFractionDigits:1})} ${unit})`,x:0.765,y:0.98,xref:"paper",yref:"paper",showarrow:false}],margin:{l:10,r:10,t:95,b:20},showlegend:false},{responsive:true});
    return;
  }
  const p=pieData(rows,metric,label);
  Plotly.react(id,[{labels:p.labels,values:p.values,type:"pie",textinfo:"label+percent",hovertemplate:"%{label}: %{value:.1f} (%{percent})<extra></extra>"}],{title:{text:titleText},margin:{l:20,r:20,t:80,b:25},legend:{orientation:"h"}},{responsive:true});
}
function fmtMaybe(v,suffix=""){return v==null||!Number.isFinite(Number(v))?"N/A":fmt(Number(v))+suffix}
function secondsHours(v){return v==null?null:Number(v)/3600}
async function loadOverview(){
  setLoading(true);
  try{
    const {start,end}=getRange(),interval=document.getElementById("interval").value;
    const [summary,metadata,series]=await Promise.all([
      getJSON(`/api/summary/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`),
      getJSON("/api/metadata/"),
      getJSON(`/api/timeseries/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&interval=${encodeURIComponent(interval)}`)
    ]);
    for(const [id,key] of [["completed-count","completed_count"],["cancelled-count","cancelled_count"],["failed-count","failed_count"],["other-count","other_count"],["cpu-hours","cpu_hours"],["gpu-hours","gpu_hours"]])document.getElementById(id).textContent=fmt(summary[key]??0);
    document.getElementById("running-count").textContent=summary.running_count==null?"Unavailable":fmt(summary.running_count);
    document.getElementById("pending-count").textContent=summary.pending_count==null?"Unavailable":fmt(summary.pending_count);
    document.getElementById("peak-cpus").textContent=fmtMaybe(summary.max_cpus_per_job);
    document.getElementById("peak-gpus").textContent=fmtMaybe(summary.max_gpus_per_job);
    document.getElementById("peak-ram").textContent=fmtMaybe(summary.max_requested_memory_gib," GiB");
    document.getElementById("peak-recorded-ram").textContent=fmtMaybe(summary.max_recorded_rss_gib," GiB");
    document.getElementById("peak-gpu-ram").textContent=fmtMaybe(summary.max_recorded_gpu_memory_gib," GiB");
    document.getElementById("peak-runtime").textContent=fmtMaybe(summary.max_elapsed_runtime_hours," h");
    document.getElementById("peak-limit").textContent=fmtMaybe(summary.max_time_limit_hours," h");
    document.getElementById("peak-disk-read").textContent=fmtMaybe(summary.max_disk_read_gib," GiB");
    document.getElementById("peak-disk-write").textContent=fmtMaybe(summary.max_disk_write_gib," GiB");
    document.getElementById("peak-disk-io").textContent=fmtMaybe(summary.max_disk_io_gib," GiB");
    document.getElementById("peak-nodes").textContent=fmtMaybe(summary.max_nodes_per_job);
    document.getElementById("avg-nodes").textContent=fmtMaybe(summary.avg_nodes_per_job);
    document.getElementById("peak-tasks").textContent=fmtMaybe(summary.max_tasks_per_job);
    document.getElementById("avg-tasks").textContent=fmtMaybe(summary.avg_tasks_per_job);
    document.getElementById("refreshed").textContent=metadata.refreshed_at?`Snapshot: ${new Date(metadata.refreshed_at).toLocaleString()}`:"";
    const rows=series.results||[],x=rows.map(r=>r.timestamp);
    Plotly.react("job-flow-chart",[
      {x,y:rows.map(r=>r.jobs_submitted),name:"Submitted",type:"bar"},
      {x,y:rows.map(r=>r.jobs_completed),name:"Completed",type:"bar"},
      {x,y:rows.map(r=>r.jobs_failed),name:"Failed / timed out",type:"bar"}
    ],{...baseLayout("Job counts by event time"),barmode:"group",yaxis:{title:"Jobs",rangemode:"tozero"}},{responsive:true});
    Plotly.react("cpu-chart",[{x,y:rows.map(r=>r.avg_allocated_cpus_per_job),name:"Average allocated CPUs per job started",type:"scatter",mode:"lines+markers"}],baseLayout("Average CPUs per job"),{responsive:true});
    Plotly.react("gpu-chart",[{x,y:rows.map(r=>r.avg_allocated_gpus_per_job),name:"Average allocated GPUs per job started",type:"scatter",mode:"lines+markers"}],baseLayout("Average GPUs per job"),{responsive:true});
    Plotly.react("memory-chart",[
      {x,y:rows.map(r=>r.avg_requested_memory_gib_per_job),name:"Average requested memory (GiB/job)",type:"scatter",mode:"lines+markers"},
      {x,y:rows.map(r=>r.avg_recorded_memory_gib_per_job),name:"Average recorded MaxRSS (GiB/job)",type:"scatter",mode:"lines+markers"}
    ],baseLayout("Average RAM per job"),{responsive:true});
    Plotly.react("runtime-chart",[
      {x,y:rows.map(r=>secondsHours(r.avg_elapsed_runtime_seconds)),name:"Average actual runtime (hours)",type:"scatter",mode:"lines+markers"},
      {x,y:rows.map(r=>secondsHours(r.avg_time_limit_seconds)),name:"Average Slurm time limit (hours)",type:"scatter",mode:"lines+markers"}
    ],baseLayout("Average runtime vs time limit"),{responsive:true});
    Plotly.react("resource-chart",[
      {x,y:rows.map(r=>r.allocated_cpus),name:"Concurrent allocated CPUs",type:"scatter",mode:"lines"},
      {x,y:rows.map(r=>r.allocated_gpus),name:"Concurrent allocated GPUs",type:"scatter",mode:"lines",yaxis:"y2"}
    ],{...baseLayout("Concurrent resource allocation"),yaxis:{title:"CPUs"},yaxis2:{title:"GPUs",overlaying:"y",side:"right"}},{responsive:true});
    document.getElementById("error").textContent="";
  }catch(e){document.getElementById("error").textContent="Unable to load dashboard: "+e.message}finally{setLoading(false)}
}
async function loadUsers(){setLoading(true);try{const {start,end}=getRange(),u=document.getElementById("user-filter").value,a=document.getElementById("user-account-filter").value;lastUsers=await getJSON(`/api/users/usage/?user=${encodeURIComponent(u)}&account=${encodeURIComponent(a)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=10000&sort=${encodeURIComponent(userSort.sort)}&direction=${userSort.direction}`);document.getElementById("users-body").innerHTML=lastUsers.map(r=>`<tr><td>${safe(r.user)}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.allocated_nodes)}</td><td class="num">${fmt(r.cpu_hours)}</td><td class="num">${fmt(r.gpu_hours)}</td><td class="num">${fmt(r.memory_gb_hours)}</td><td class="num">${fmt(r.gpu_memory_gb)}</td><td class="num">${fmt(r.total_tasks)}</td><td class="num">${fmt(r.disk_read_gib)}</td><td class="num">${fmt(r.disk_write_gib)}</td></tr>`).join("");renderPie("users-jobs-pie",lastUsers,"jobs","user","Users: job-count share");renderPie("users-nodes-pie",lastUsers,"allocated_nodes","user","Users: allocated-node share");renderPie("users-cpu-pie",lastUsers,"cpu_hours","user","Users: CPU-hour share");renderPie("users-gpu-pie",lastUsers,"gpu_hours","user","Users: GPU-hour share");renderPie("users-ram-pie",lastUsers,"memory_gb_hours","user","Users: RAM usage share (GiB-hours)");renderPie("users-tasks-pie",lastUsers,"total_tasks","user","Users: task-count share (sum of NTasks)");renderPie("users-read-pie",lastUsers,"disk_read_gib","user","Users: data-read share (MaxDiskRead, GiB)");renderPie("users-write-pie",lastUsers,"disk_write_gib","user","Users: data-write share (MaxDiskWrite, GiB)");updateSortIndicators("sort-",userSort)}catch(e){document.getElementById("error").textContent="Unable to load users: "+e.message}finally{setLoading(false)}}
async function loadAccounts(){
  setLoading(true);
  try{
    const {start,end}=getRange(),filter=document.getElementById("account-filter").value.trim();
    const tree=await getJSON(`/api/accounts/tree/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&sort=name&direction=asc`);
    lastAccounts=tree;
    const visible=hierarchicalAccounts(tree,filter);
    document.getElementById("accounts-body").innerHTML=visible.map(r=>`<tr><td class="indent" style="--level:${Number(r.display_level||0)}">${safe(r.account)}</td><td>${safe(r.display_parent||"")}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.allocated_nodes)}</td><td class="num">${fmt(r.cpu_hours)}</td><td class="num">${fmt(r.gpu_hours)}</td><td class="num">${fmt(r.memory_gb_hours)}</td><td class="num">${fmt(r.gpu_memory_gb)}</td><td class="num">${fmt(r.total_tasks)}</td><td class="num">${fmt(r.disk_read_gib)}</td><td class="num">${fmt(r.disk_write_gib)}</td></tr>`).join("");
    updateAccountPieFilters();
    renderAccountPies();
    updateSortIndicators("asort-",accountSort);
  }catch(e){document.getElementById("error").textContent="Unable to load accounts: "+e.message}finally{setLoading(false)}
}
function accountDepths(rows){
  const byName=new Map(rows.filter(r=>r.account).map(r=>[String(r.account),r]));
  const memo=new Map();
  function depth(r,seen=new Set()){
    const name=String(r.account);
    if(memo.has(name))return memo.get(name);
    if(name.toLowerCase()==="root")return -1;
    if(seen.has(name))return 0;
    const parent=String(r.parent||"");
    if(!parent||parent.toLowerCase()==="root"||!byName.has(parent)){memo.set(name,0);return 0}
    const d=depth(byName.get(parent),new Set([...seen,name]))+1;
    memo.set(name,d);return d;
  }
  return rows.filter(r=>r.account&&String(r.account).toLowerCase()!=="root").map(r=>({...r,_depth:depth(r)}));
}
function descendantsOf(rows,ancestor){
  const byName=new Map(rows.map(r=>[String(r.account),r]));
  const out=[];
  for(const r of rows){let p=String(r.parent||""),guard=0;while(p&&guard++<100){if(p===ancestor){out.push(r);break}const parent=byName.get(p);p=parent?String(parent.parent||""):""}}
  return out;
}
function updateAccountPieFilters(){
  const rows=accountDepths(lastAccounts),departments=rows.filter(r=>r._depth===0);
  const deptSel=document.getElementById("pi-dept-filter"),projectSel=document.getElementById("project-pi-filter");
  const oldDept=deptSel.value,oldPi=projectSel.value;
  deptSel.innerHTML='<option value="">All departments</option>'+departments.map(r=>`<option value="${safe(r.account)}">${safe(r.account)}</option>`).join("");
  if(departments.some(r=>r.account===oldDept))deptSel.value=oldDept;
  const pis=rows.filter(r=>r._depth===1&&(!deptSel.value||descendantsOf(rows,deptSel.value).some(d=>d.account===r.account)));
  projectSel.innerHTML='<option value="">All PIs</option>'+pis.map(r=>`<option value="${safe(r.account)}">${safe(r.account)}</option>`).join("");
  if(pis.some(r=>r.account===oldPi))projectSel.value=oldPi;
}
function renderAccountPies(){
  const rows=accountDepths(lastAccounts),metric=document.getElementById("account-pie-metric").value;
  const dept=document.getElementById("pi-dept-filter").value,pi=document.getElementById("project-pi-filter").value;
  const departments=rows.filter(r=>r._depth===0);
  const pis=rows.filter(r=>r._depth===1&&(!dept||descendantsOf(rows,dept).some(d=>d.account===r.account)));
  const projects=rows.filter(r=>r._depth>=2&&(!dept||descendantsOf(rows,dept).some(d=>d.account===r.account))&&(!pi||descendantsOf(rows,pi).some(d=>d.account===r.account)));
  const metricLabel={jobs:"jobs",cpu_hours:"CPU-hours",gpu_hours:"GPU-hours",memory_gb_hours:"memory GiB-hours",elapsed_hours:"elapsed hours",total_tasks:"tasks",disk_read_gib:"data-read GiB",disk_write_gib:"data-write GiB"}[metric]||metric;
  renderPie("accounts-dept-pie",departments,metric,"account",`Departments: ${metricLabel} share`);
  renderPie("accounts-pi-pie",pis,metric,"account",`PIs: ${metricLabel} share`);
  renderPie("accounts-project-pie",projects,metric,"account",`Projects: ${metricLabel} share`);
  // Keep each account level in its own full-width row, with its metrics grouped together.
  const maxDepth=rows.reduce((m,r)=>Math.max(m,r._depth),0);
  const holder=document.getElementById("accounts-level-pies");
  holder.innerHTML="";
  const metrics=[
    ["cpu_hours","CPU-hours share"], ["gpu_hours","GPU-hours share"],
    ["memory_gb_hours","RAM usage share (GiB-hours)"], ["total_tasks","Task-count share (sum of NTasks)"],
    ["disk_read_gib","Data-read share (GiB)"], ["disk_write_gib","Data-write share (GiB)"],
  ];
  for(let depth=0;depth<=maxDepth;depth++){
    const atLevel=rows.filter(r=>r._depth===depth);
    if(!atLevel.length)continue;
    const group=document.createElement("div");group.className="account-level-group";
    const heading=document.createElement("h3");heading.className="account-level-title";heading.textContent=`Level ${depth+1}`;group.appendChild(heading);
    const chartRow=document.createElement("div");chartRow.className="account-level-charts";group.appendChild(chartRow);holder.appendChild(group);
    for(const [metricKey,metricTitle] of metrics){
      const id=`accounts-level-${depth}-${metricKey}`;const box=document.createElement("div");box.className="chart pie-chart";box.innerHTML=`<div id="${id}"></div>`;chartRow.appendChild(box);
      renderPie(id,atLevel,metricKey,"account",`Level ${depth+1}: ${metricTitle}`);
    }
  }
}
function togglePartitionSort(key){if(partitionSort.sort===key)partitionSort.direction=partitionSort.direction==="asc"?"desc":"asc";else{partitionSort.sort=key;partitionSort.direction=key==="partition"?"asc":"desc"}}
function updatePartitionSortIndicators(){for(const key of ["partition","jobs","cpu_hours","gpu_hours","avg_nodes_per_job","max_nodes_per_job","avg_tasks_per_job","max_tasks_per_job","memory_gb_hours","total_tasks","disk_read_gib","disk_write_gib"]){const el=document.getElementById(`psort-${key}`);if(el)el.textContent=partitionSort.sort===key?(partitionSort.direction==="asc"?"▲":"▼"):""}}
function renderPartitionTable(){const key=partitionSort.sort;const rows=[...lastPartitions].sort((a,b)=>{const av=a[key],bv=b[key];let cmp;if(key==="partition")cmp=String(av||"").localeCompare(String(bv||""));else cmp=(Number(av)||0)-(Number(bv)||0);return partitionSort.direction==="asc"?cmp:-cmp});document.getElementById("partitions-body").innerHTML=rows.map(r=>`<tr><td>${safe(r.partition)}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.cpu_hours)}</td><td class="num">${fmt(r.gpu_hours)}</td><td class="num">${fmtMaybe(r.avg_nodes_per_job)}</td><td class="num">${fmtMaybe(r.max_nodes_per_job)}</td><td class="num">${fmtMaybe(r.avg_tasks_per_job)}</td><td class="num">${fmtMaybe(r.max_tasks_per_job)}</td><td class="num">${fmt(r.memory_gb_hours)}</td><td class="num">${fmt(r.total_tasks)}</td><td class="num">${fmt(r.disk_read_gib)}</td><td class="num">${fmt(r.disk_write_gib)}</td></tr>`).join("");updatePartitionSortIndicators()}
function sortPartitions(key){togglePartitionSort(key);renderPartitionTable()}
async function loadPartitions(){
  setLoading(true);
  try{
    const {start,end}=getRange();
    const [partitionRows, gpuTypeData]=await Promise.all([
      getJSON(`/api/partitions/usage/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`),
      getJSON(`/api/partitions/gpu-types/?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`)
    ]);
    lastPartitions=partitionRows;
    renderPartitionTable();
    const gpuTypes=gpuTypeData.types||[], partitionGpuTypes=gpuTypeData.partition_types||[];
    renderPie("partition-gpu-type-jobs-pie",gpuTypes,"jobs","gpu_type","GPU models: job-count share");
    renderPie("partition-gpu-type-count-pie",gpuTypes,"total_gpus","gpu_type","GPU models: allocated GPU share");
    renderPie("partition-gpu-type-hours-pie",gpuTypes,"gpu_hours","gpu_type","GPU models: GPU-hours share");
    document.getElementById("partition-gpu-types-body").innerHTML=partitionGpuTypes.map(r=>`<tr><td>${safe(r.partition)}</td><td>${safe(r.gpu_type)}</td><td class="num">${fmt(r.jobs)}</td><td class="num">${fmt(r.total_gpus)}</td><td class="num">${fmtMaybe(r.avg_gpus_per_job)}</td><td class="num">${fmt(r.gpu_hours)}</td></tr>`).join("");
    renderPie("partition-cpu-pie",lastPartitions,"cpu_hours","partition","Partitions: CPU-hours share");
    renderPie("partition-gpu-pie",lastPartitions,"gpu_hours","partition","Partitions: GPU-hours share");
    renderPie("partition-ram-pie",lastPartitions,"memory_gb_hours","partition","Partitions: RAM usage share (GiB-hours)");
    renderPie("partition-tasks-pie",lastPartitions,"total_tasks","partition","Partitions: task-count share (sum of NTasks)");
    renderPie("partition-read-pie",lastPartitions,"disk_read_gib","partition","Partitions: data-read share (GiB)");
    renderPie("partition-write-pie",lastPartitions,"disk_write_gib","partition","Partitions: data-write share (GiB)");
    document.getElementById("error").textContent="";
  }catch(e){document.getElementById("error").textContent="Unable to load partitions: "+e.message}finally{setLoading(false)}
}

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
document.getElementById("range").addEventListener("change",()=>{const custom=document.getElementById("range").value==="custom";document.getElementById("custom-range").classList.toggle("visible",custom);if(custom&&!document.getElementById("custom-start").value){const end=new Date();document.getElementById("custom-end").value=toISOInput(end);document.getElementById("custom-start").value=toISOInput(new Date(end.getTime()-7*864e5))}else loadActiveTab()});document.getElementById("apply-range").addEventListener("click",event=>{event.preventDefault();loadActiveTab()});document.getElementById("interval").addEventListener("change",loadOverview);
document.getElementById("account-pie-metric").addEventListener("change",renderAccountPies);
document.getElementById("pi-dept-filter").addEventListener("change",()=>{updateAccountPieFilters();renderAccountPies()});
document.getElementById("project-pi-filter").addEventListener("change",renderAccountPies);
function loadActiveTab(){const active=document.querySelector(".tab.active").dataset.panel;if(active==="overview")loadOverview();else if(active==="users")loadUsers();else if(active==="accounts")loadAccounts();else if(active==="partitions")loadPartitions()}
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll(".tab").forEach(x=>x.classList.remove("active"));document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));b.classList.add("active");document.getElementById(b.dataset.panel).classList.add("active");loadActiveTab()}));
loadOverview();

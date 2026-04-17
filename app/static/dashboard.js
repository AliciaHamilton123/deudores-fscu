/* Deudores FSCU · DecideChile — render dashboard desde aggregates.json */

const COLOR = {
    blue: '#266FE0', blueDark: '#184D9B', green: '#10B981',
    yellow: '#F59E0B', red: '#EF4444', purple: '#8B5CF6',
    cyan: '#06B6D4', gray: '#9CA3AF',
};
const PALETTE = [COLOR.blue, COLOR.green, COLOR.yellow, COLOR.purple, COLOR.cyan, COLOR.red, COLOR.gray, COLOR.blueDark];

Chart.defaults.font.family = "'Hanken Grotesk', -apple-system, sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.color = '#3A3A3A';
Chart.defaults.borderColor = '#E5E7EB';
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.boxWidth = 8;

const fmt = n => (n ?? 0).toLocaleString('es-CL');

async function load() {
    const r = await fetch('/static/aggregates.json');
    const d = await r.json();
    renderHero(d);
    renderKPIs(d);
    renderPerfiles(d);
    renderGeografia(d);
    renderDemografia(d);
    renderPatrimonio(d);
    renderLinkedIn(d);
    setupNav();
}

function renderHero(d) {
    document.getElementById('hero-total').textContent = fmt(d.resumen.total);
    document.getElementById('lk-count').textContent = fmt(d.resumen.en_linkedin);
    document.getElementById('lk-pct').textContent = d.resumen.pct_linkedin;
}

function renderKPIs(d) {
    const r = d.resumen;
    const kpis = [
        { label: 'Deudores totales', value: fmt(r.total), sub: 'Año tributario 2026' },
        { label: 'Identificados en LinkedIn', value: fmt(r.en_linkedin), sub: `${r.pct_linkedin}% del total`, accent: true },
        { label: 'Patrimonio alto (decil 8-10)', value: fmt(r.patrimonio_alto), sub: `${(100*r.patrimonio_alto/r.total).toFixed(1)}%` },
        { label: 'Con ≥1 vehículo', value: fmt(r.con_vehiculos), sub: `${(100*r.con_vehiculos/r.total).toFixed(1)}%` },
        { label: 'Con ≥1 propiedad', value: fmt(r.con_propiedades), sub: `${(100*r.con_propiedades/r.total).toFixed(1)}%` },
    ];
    document.getElementById('kpis').innerHTML = kpis.map(k => `
        <div class="kpi">
            <div class="kpi-label">${k.label}</div>
            <div class="kpi-value ${k.accent ? 'accent' : ''}">${k.value}</div>
            <div class="kpi-sub">${k.sub}</div>
        </div>
    `).join('');
}

function renderPerfiles(d) {
    const data = d.perfiles;
    const labels = data.map(x => x.perfil.replace(/^\d+\.\s*/, ''));
    const values = data.map(x => x.n);
    new Chart(document.getElementById('ch-perfiles'), {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{ data: values, backgroundColor: PALETTE.concat(PALETTE), borderWidth: 1, borderColor: '#fff' }],
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right', labels: { font: { size: 10 } } },
                tooltip: { callbacks: { label: ctx => `${ctx.label}: ${fmt(ctx.parsed)} (${(100*ctx.parsed/values.reduce((a,b)=>a+b,0)).toFixed(1)}%)` } },
            },
        },
    });
    const max = Math.max(...values);
    document.getElementById('tbl-perfiles').innerHTML = `
        <table class="perfil-tbl">
            ${data.map(p => `<tr>
                <td>${p.perfil.replace(/^\d+\.\s*/, '')}<span class="perfil-bar" style="width:${100*p.n/max}px"></span></td>
                <td>${fmt(p.n)}</td>
                <td>${p.pct}%</td>
            </tr>`).join('')}
        </table>
    `;
}

function renderGeografia(d) {
    horizontalBar('ch-region', d.por_region.map(r=>r.region), d.por_region.map(r=>r.n), 'Deudores');
    pie('ch-macrozona', d.por_macrozona.map(m=>m.macrozona), d.por_macrozona.map(m=>m.n));
    const top20 = d.por_comuna.slice(0, 20);
    horizontalBar('ch-comuna', top20.map(c=>c.comuna), top20.map(c=>c.n), 'Deudores');
}

function renderDemografia(d) {
    pie('ch-sexo', d.por_sexo.map(x=>x.sexo), d.por_sexo.map(x=>x.n));
    bar('ch-edad', d.por_edad.map(x=>x.rango_edad), d.por_edad.map(x=>x.n), 'Deudores');
    bar('ch-decil', d.por_decil.map(x=>x.decil), d.por_decil.map(x=>x.n), 'Deudores');

    // Edad × Sexo stacked
    const ages = [...new Set(d.por_edad_sexo.map(x=>x.rango_edad))].sort();
    const sexos = [...new Set(d.por_edad_sexo.map(x=>x.sexo))];
    const datasets = sexos.map((s, i) => ({
        label: s, data: ages.map(a => (d.por_edad_sexo.find(x => x.rango_edad===a && x.sexo===s) || {}).n || 0),
        backgroundColor: PALETTE[i],
    }));
    new Chart(document.getElementById('ch-edad-sexo'), {
        type: 'bar', data: { labels: ages, datasets },
        options: { responsive: true, scales: { x: { stacked: true }, y: { stacked: true } } },
    });

    horizontalBar('ch-nse', d.por_nse.map(x=>x.nse), d.por_nse.map(x=>x.n), 'Deudores');
}

function renderPatrimonio(d) {
    bar('ch-vehiculos', d.por_vehiculos.map(x=>x.bucket), d.por_vehiculos.map(x=>x.n), 'Deudores');
    bar('ch-propiedades', d.por_propiedades.map(x=>x.bucket), d.por_propiedades.map(x=>x.n), 'Deudores');
    pie('ch-condicion', d.condicion_propietario.map(x=>x.bucket), d.condicion_propietario.map(x=>x.n));
}

function renderLinkedIn(d) {
    horizontalBar('ch-seniority', d.por_seniority.map(x=>x.seniority), d.por_seniority.map(x=>x.n), 'Deudores');
    pie('ch-tier', d.por_tier.map(x=>x.tier), d.por_tier.map(x=>x.n));
    horizontalBar('ch-industria', d.top_industrias.slice(0,20).map(x=>x.industry), d.top_industrias.slice(0,20).map(x=>x.n), 'Deudores');
    horizontalBar('ch-empresa', d.top_empresas.slice(0,30).map(x=>x.company), d.top_empresas.slice(0,30).map(x=>x.n), 'Deudores');
}

/* === helpers === */
function bar(id, labels, values, label) {
    new Chart(document.getElementById(id), {
        type: 'bar',
        data: { labels, datasets: [{ label, data: values, backgroundColor: COLOR.blue }] },
        options: { responsive: true, plugins: { legend: { display: false } } },
    });
}
function horizontalBar(id, labels, values, label) {
    new Chart(document.getElementById(id), {
        type: 'bar',
        data: { labels, datasets: [{ label, data: values, backgroundColor: COLOR.blue }] },
        options: {
            indexAxis: 'y', responsive: true,
            plugins: { legend: { display: false } },
            scales: { y: { ticks: { font: { size: 11 } } } },
        },
    });
}
function pie(id, labels, values) {
    new Chart(document.getElementById(id), {
        type: 'doughnut',
        data: { labels, datasets: [{ data: values, backgroundColor: PALETTE, borderWidth: 1, borderColor: '#fff' }] },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom', labels: { font: { size: 10 }, padding: 8 } },
                tooltip: { callbacks: { label: ctx => `${ctx.label}: ${fmt(ctx.parsed)}` } },
            },
        },
    });
}

function setupNav() {
    const links = document.querySelectorAll('.nav-link');
    const sections = Array.from(links).map(l => document.querySelector(l.getAttribute('href')));
    window.addEventListener('scroll', () => {
        const y = window.scrollY + 100;
        let active = 0;
        sections.forEach((s, i) => { if (s && s.offsetTop <= y) active = i; });
        links.forEach((l, i) => l.classList.toggle('active', i === active));
    });
}

load();

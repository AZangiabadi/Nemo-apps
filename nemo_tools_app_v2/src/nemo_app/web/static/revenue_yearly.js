/* global Chart */
(() => {
  const form = document.getElementById('yearly-form');
  const year = document.getElementById('revenue-year');
  const caps = document.getElementById('yearly-caps');
  const previous = document.getElementById('previous-year');
  const next = document.getElementById('next-year');
  const show = document.getElementById('show-year');
  const download = document.getElementById('export-year');
  const status = document.getElementById('yearly-status');
  const results = document.getElementById('yearly-results');
  const money = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD'});
  const statuses = {available: 'Available', not_saved: 'Not saved', future: 'Future month', no_lab_breakdown: 'Re-analyze for labs'};
  let report = null;
  let chart = null;
  let generation = 0;
  let busy = false;
  const text = (id, value) => { document.getElementById(id).textContent = value; };
  const amount = value => value === null ? 'Not available' : money.format(value);

  function navigation() {
    year.disabled = busy;
    caps.disabled = busy;
    previous.disabled = busy || !year.validity.valid || Number(year.value) <= Number(year.min);
    next.disabled = busy || !year.validity.valid || Number(year.value) >= Number(year.max);
    show.disabled = busy;
  }

  function render(data) {
    report = data;
    results.hidden = false;
    text('year-heading', `${data.year} | January - December`);
    text('year-coverage', `NEMO session caps ${caps.checked ? 'applied' : 'ignored'} | Badger historical charges preserved`);
    text('year-lab-total', data.lab_months ? money.format(data.lab_total) : 'Not available');
    text('year-total', data.saved_months ? money.format(data.total) : 'Not available');
    text('year-other', data.lab_months ? money.format(data.other) : 'Not available');
    text('year-months', `${data.lab_months} / 12`);
    text('year-missing', `${data.missing_months} past/current months not saved`);
    document.getElementById('year-estimates').hidden = !data.estimated_months;
    text('year-estimates', `${data.estimated_months} Badger months include historical cap estimates. Recorded amounts and adjustments are available in the monthly analysis.`);
    document.getElementById('year-unclassified').hidden = !data.months.some(row => row.status === 'no_lab_breakdown');
    text('year-unclassified', `${money.format(data.unclassified)} of saved revenue has no lab breakdown and is excluded from the stacked columns. Re-analyze those months to add lab data.`);
    if (chart) chart.destroy();
    chart = new Chart(document.getElementById('yearly-chart'), {
      type: 'bar',
      data: {
        labels: data.months.map(row => row.label),
        datasets: data.labs.map((lab, index) => ({label: lab,
          data: data.months.map(row => row.labs === null ? null : row.labs[lab]),
          backgroundColor: ['#0f766e', '#4f69b5', '#bd506d'][index], maxBarThickness: 54
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        interaction: {intersect: false, mode: 'index'},
        onHover: (event, _elements, chart) => {
          const hit = chart.getElementsAtEventForMode(event, 'nearest', {intersect: true}, false);
          chart.canvas.style.cursor = hit.length ? 'pointer' : 'default';
        },
        onClick: (event, _elements, chart) => {
          const hit = chart.getElementsAtEventForMode(event, 'nearest', {intersect: true}, false);
          if (!hit.length) return;
          const link = document.getElementById('yearly-rows').rows[hit[0].index]?.querySelector('a');
          if (link) link.click();
        },
        plugins: {
          legend: {position: 'bottom', labels: {boxWidth: 12, padding: 18}},
          tooltip: {callbacks: {
            title: items => `${data.months[items[0].dataIndex].label} ${data.year} (${data.months[items[0].dataIndex].source})`,
            label: item => `${item.dataset.label}: ${money.format(item.parsed.y)}`,
            footer: items => `Displayed labs: ${money.format(items.reduce((sum, item) => sum + item.parsed.y, 0))}`
          }}
        },
        scales: {
          x: {stacked: true, grid: {display: false}, ticks: {maxRotation: 0, autoSkip: false, font: {size: 11}}},
          y: {stacked: true, beginAtZero: true, grid: {color: '#e4e7ec'}, ticks: {callback: value =>
            new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', notation: 'compact'}).format(value)}}
        }
      }
    });
    document.getElementById('yearly-rows').replaceChildren(...data.months.map(row => {
      const element = document.createElement('tr');
      const month = element.insertCell();
      if (row.status === 'future') {
        month.textContent = row.label;
      } else {
        const link = document.createElement('a');
        link.textContent = row.label;
        link.href = `/tools/revenue?month=${encodeURIComponent(row.period)}&caps=${caps.checked ? '1' : '0'}`;
        link.setAttribute('aria-label', `Open ${row.label} ${data.year} monthly analysis`);
        month.append(link);
      }
      [row.source || '-', `${statuses[row.status]}${row.estimated ? ' (estimate)' : ''}`,
        ...data.labs.map(lab => amount(row.labs === null ? null : row.labs[lab])),
        amount(row.lab_total), amount(row.other), amount(row.total)].forEach((value, index) => {
        const cell = element.insertCell(); cell.textContent = value;
        if (index >= 2) cell.className = 'numeric';
      });
      return element;
    }));
    download.disabled = false;
  }

  async function load(event) {
    if (event) event.preventDefault();
    if (!form.reportValidity()) return;
    const ticket = ++generation;
    const viewport = window.RevenueViewport.begin();
    busy = true; navigation(); download.disabled = true; results.hidden = true;
    status.className = 'revenue-status'; status.textContent = 'Loading saved monthly analyses...';
    const query = new URLSearchParams({year: year.value, hourly_caps: caps.checked ? '1' : '0'});
    try {
      const response = await fetch(`${form.getAttribute('action')}?${query}`, {cache: 'no-store'});
      if (response.redirected) throw new Error('Your session has expired. Sign in again.');
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Unable to load yearly revenue.');
      if (ticket !== generation) return;
      render(data);
      const url = new URL(location.href); url.search = query.toString(); history.replaceState(null, '', url);
      status.textContent = !data.saved_months ? 'No saved analyses for this year.' :
        (data.missing_months ? 'Partial year: some monthly analyses are not saved.' : 'Saved monthly analyses loaded.');
    } catch (error) {
      if (ticket === generation) { status.className = 'revenue-status error'; status.textContent = error.message; }
    } finally {
      if (ticket === generation) { busy = false; navigation(); await window.RevenueViewport.finish(viewport); }
    }
  }

  function move(offset) {
    if (busy || !year.validity.valid) return;
    const value = Number(year.value) + offset;
    if (value < Number(year.min) || value > Number(year.max)) return;
    year.value = value;
    load();
  }
  previous.addEventListener('click', () => move(-1));
  next.addEventListener('click', () => move(1));
  year.addEventListener('change', navigation);
  caps.addEventListener('change', () => load());
  form.addEventListener('submit', load);
  download.addEventListener('click', () => {
    if (!report) return;
    const rows = [['Month', 'Source', 'Status', ...report.labs, 'Three-lab revenue', 'Other charges', 'Total revenue'],
      ...report.months.map(row => [row.period, row.source, `${statuses[row.status]}${row.estimated ? ' (estimate)' : ''}`,
        ...report.labs.map(lab => row.labs === null ? '' : row.labs[lab]), row.lab_total ?? '', row.other ?? '', row.total ?? ''])];
    const csv = rows.map(row => row.map(value => `"${String(value).replaceAll('"', '""')}"`).join(',')).join('\r\n');
    const url = URL.createObjectURL(new Blob([csv], {type: 'text/csv;charset=utf-8;'}));
    const link = document.createElement('a'); link.href = url; link.download = `revenue-year-${report.year}.csv`; link.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  const query = new URL(location.href).searchParams;
  if (/^\d{1,4}$/.test(query.get('year') || '')) year.value = query.get('year');
  caps.checked = query.get('hourly_caps') === '1';
  navigation(); load();
})();

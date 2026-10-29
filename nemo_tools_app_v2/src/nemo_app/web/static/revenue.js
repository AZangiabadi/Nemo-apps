/* global Chart */
(() => {
  const form = document.getElementById('revenue-form');
  const button = document.getElementById('analyze');
  const showButton = document.getElementById('show-analysis');
  const monthInput = document.getElementById('billing-month');
  const previousMonth = document.getElementById('previous-month');
  const nextMonth = document.getElementById('next-month');
  const status = document.getElementById('analysis-status');
  const results = document.getElementById('revenue-results');
  const money = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD'});
  const charts = [];
  let report = null;
  let generation = 0;
  const text = (id, value) => { document.getElementById(id).textContent = value; };

  function syncMonthNavigation() {
    const invalid = !monthInput.value || !monthInput.validity.valid;
    previousMonth.disabled = showButton.disabled || invalid || monthInput.value <= monthInput.min;
    nextMonth.disabled = showButton.disabled || invalid || monthInput.value >= monthInput.max;
  }

  function changeMonth(offset) {
    if (!monthInput.value || !monthInput.validity.valid) return;
    const [year, month] = monthInput.value.split('-').map(Number);
    const index = year * 12 + month - 1 + offset;
    const value = `${String(Math.floor(index / 12)).padStart(4, '0')}-${String(index % 12 + 1).padStart(2, '0')}`;
    if (value < monthInput.min || value > monthInput.max) return;
    monthInput.value = value;
    syncMonthNavigation();
    form.requestSubmit(showButton);
  }
  previousMonth.addEventListener('click', () => changeMonth(-1));
  nextMonth.addEventListener('click', () => changeMonth(1));
  monthInput.addEventListener('change', syncMonthNavigation);

  function chart(id, type, labels, datasets, extra = {}) {
    const compactMoney = value => new Intl.NumberFormat('en-US', {
      style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1
    }).format(value);
    const horizontal = extra.indexAxis === 'y';
    charts.push(new Chart(document.getElementById(id), {
      type, data: {labels, datasets},
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: false,
        interaction: {intersect: false, mode: horizontal ? 'nearest' : 'index'},
        plugins: {
          legend: {display: false},
          tooltip: {callbacks: {label: item => `${item.dataset.label}: ${money.format(horizontal ? item.parsed.x : item.parsed.y)}`}}
        },
        scales: {
          x: {stacked: true, grid: {display: false}, ticks: horizontal ? {callback: compactMoney} : {maxTicksLimit: 12, maxRotation: 0}},
          y: {stacked: true, beginAtZero: true, grid: {color: '#e4e7ec'}, ticks: horizontal ? {} : {callback: compactMoney}}
        },
        ...extra
      }
    }));
  }

  function piRows() {
    const limit = Number(document.getElementById('pi-limit').value) || report.pis.length;
    const rows = report.pis.slice(0, limit).map((pi, index) => {
      const row = document.createElement('tr');
      const values = [index + 1, pi.name, money.format(pi.total), pi.projects, money.format(pi.gold),
        report.total ? `${(pi.total / report.total * 100).toFixed(1)}%` : '0.0%'];
      values.forEach((value, column) => {
        const cell = row.insertCell();
        cell.textContent = value;
        if (column >= 2) cell.className = 'numeric';
        if (column === 1 && pi.email) {
          const email = document.createElement('small');
          email.textContent = pi.email;
          cell.append(email);
        }
      });
      return row;
    });
    if (!rows.length) {
      const row = document.createElement('tr');
      const cell = row.insertCell(); cell.colSpan = 6; cell.textContent = 'No invoiceable activity for this month.';
      rows.push(row);
    }
    document.getElementById('pi-rows').replaceChildren(...rows);
  }

  function render(data) {
    report = data;
    results.hidden = false;
    text('period-label', data.label);
    text('cap-mode', `Session caps ${data.apply_hourly_caps ? 'applied' : 'ignored'} | Project caps applied`);
    text('total-revenue', money.format(data.total));
    text('gold-revenue', money.format(data.gold));
    text('fee-revenue', money.format(data.fees));
    text('other-revenue', money.format(data.other));
    const count = (value, singular, plural = `${singular}s`) => `${value} ${value === 1 ? singular : plural}`;
    text('invoice-count', `${count(data.invoice_count, 'invoice')} | ${count(data.pi_count, 'PI')} | ${count(data.project_count, 'project')}`);
    text('updated', `Calculated ${new Date(data.generated_at).toLocaleString('en-US', {timeZone: 'America/New_York'})} ET`);
    document.getElementById('billing-month').value = data.period;
    form.elements.apply_hourly_caps.checked = data.apply_hourly_caps;
    const savedUrl = new URL(location.href);
    savedUrl.searchParams.delete('analysis');
    savedUrl.searchParams.set('month', data.period);
    savedUrl.searchParams.set('caps', data.apply_hourly_caps ? '1' : '0');
    history.replaceState(null, '', savedUrl);
    charts.splice(0).forEach(item => item.destroy());
    const days = data.daily.map(item => Number(item.date.slice(-2)));
    chart('daily-chart', 'bar', days, [
      {label: 'Charges', data: data.daily.map(item => item.charges), backgroundColor: '#0f766e', maxBarThickness: 22},
      {label: 'Access fees', data: data.daily.map(item => item.fees), backgroundColor: '#4f69b5', maxBarThickness: 22}
    ]);
    chart('gold-chart', 'bar', days, [
      {label: 'Gold surcharge', data: data.daily.map(item => item.gold), backgroundColor: '#ab790b', maxBarThickness: 18}
    ]);
    chart('type-chart', 'bar', data.applications.map(item => item.name), [{
      label: 'Final revenue', data: data.applications.map(item => item.total),
      backgroundColor: ['#0f766e', '#4f69b5', '#bd506d', '#ab790b'], maxBarThickness: 28
    }], {indexAxis: 'y'});
    piRows();
    document.getElementById('daily-rows').replaceChildren(...data.daily.map(item => {
      const row = document.createElement('tr');
      [item.date, money.format(item.charges), money.format(item.fees), money.format(item.gold), money.format(item.total)]
        .forEach(value => { row.insertCell().textContent = value; });
      return row;
    }));
    document.getElementById('type-data').replaceChildren(...data.applications.map(item => {
      const row = document.createElement('div'); row.className = 'revenue-type-item';
      const label = document.createElement('span'); label.textContent = item.name;
      const value = document.createElement('strong'); value.textContent = money.format(item.total);
      row.append(label, value); return row;
    }));
  }

  async function poll(url, ticket) {
    let failures = 0;
    while (ticket === generation) {
      let terminal = false;
      try {
        const response = await fetch(url, {cache: 'no-store'});
        if (response.redirected) throw new Error('Your session has expired. Sign in again.');
        if (!response.ok) throw new Error('Revenue analysis is unavailable. Run the analysis again.');
        const data = await response.json();
        if (ticket !== generation) return;
        terminal = Boolean(data.finished);
        failures = 0;
        if (data.finished) {
          if (data.status !== 'completed') throw new Error(data.error || 'Analysis failed. Please try again.');
          render(data.data);
          status.textContent = data.data.total ? 'Analysis complete and saved.' : 'Analysis saved. No invoiceable activity for this month.';
          return;
        }
        status.textContent = data.message || 'Calculating invoice revenue...';
      } catch (error) {
        if (terminal || ++failures >= 3) throw error;
        status.textContent = 'Connection interrupted. Retrying...';
      }
      await new Promise(resolve => window.setTimeout(resolve, 1500));
    }
  }

  async function analyze(event) {
    if (event) event.preventDefault();
    const action = event?.submitter?.value || 'show';
    const ticket = ++generation;
    button.disabled = true; showButton.disabled = true;
    syncMonthNavigation();
    if (action === 'analyze') button.textContent = 'Calculating...';
    status.className = 'revenue-status';
    status.textContent = action === 'show' ? 'Loading saved analysis...' : 'Fetching monthly billing from NEMO...';
    results.hidden = !(report && report.period === form.elements.billing_month.value &&
      report.apply_hourly_caps === form.elements.apply_hourly_caps.checked);
    try {
      const body = new FormData(form);
      body.set('action', action);
      const response = await fetch(form.getAttribute('action'), {method: 'POST', body});
      if (response.redirected) throw new Error('Your session has expired. Sign in again.');
      const data = await response.json();
      if (action === 'show' && response.status === 404) {
        status.textContent = data.error;
        return;
      }
      if (!response.ok) throw new Error(data.error || 'Unable to start revenue analysis.');
      if (data.saved) {
        render(data.data);
        status.textContent = 'Saved analysis.';
        return;
      }
      const url = new URL(location.href);
      url.searchParams.set('analysis', data.status_url.split('/').at(-2));
      history.replaceState(null, '', url);
      await poll(data.status_url, ticket);
    } catch (error) {
      status.className = 'revenue-status error'; status.textContent = error.message;
    } finally {
      button.disabled = false; showButton.disabled = false; button.textContent = 'Re-analyze';
      syncMonthNavigation();
    }
  }

  document.getElementById('pi-limit').addEventListener('change', () => { if (report) piRows(); });
  document.getElementById('export-revenue').addEventListener('click', () => {
    if (!report) return;
    const rows = [['Section', 'Date or name', 'Charges', 'Access fees', 'Gold included', 'Final revenue'],
      ['Monthly total', report.period, report.other + report.gold, report.fees, report.gold, report.total],
      ...report.daily.map(item => ['Daily', item.date, item.charges, item.fees, item.gold, item.total]),
      ...report.applications.map(item => ['Project type', item.name, '', '', '', item.total]),
      ...report.pis.map(item => ['PI', item.name, '', '', item.gold, item.total])];
    const csv = rows.map(row => row.map(value => {
      let cell = String(value);
      if (/^[=+@\-\t\r]/.test(cell)) cell = `'${cell}`;
      return `"${cell.replaceAll('"', '""')}"`;
    }).join(',')).join('\r\n');
    const url = URL.createObjectURL(new Blob([csv], {type: 'text/csv;charset=utf-8;'}));
    const link = document.createElement('a'); link.href = url; link.download = `revenue-${report.period}.csv`;
    link.click(); window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  form.addEventListener('submit', analyze);
  const query = new URL(location.href).searchParams;
  if (/^\d{4}-\d{2}$/.test(query.get('month') || '')) form.elements.billing_month.value = query.get('month');
  form.elements.apply_hourly_caps.checked = query.get('caps') === '1';
  syncMonthNavigation();
  const existing = query.get('analysis');
  if (existing && /^[a-zA-Z0-9-]+$/.test(existing)) {
    button.disabled = true; showButton.disabled = true;
    syncMonthNavigation();
    poll(`/tools/revenue/${encodeURIComponent(existing)}/status`, ++generation)
      .catch(error => { status.className = 'revenue-status error'; status.textContent = error.message; })
      .finally(() => { button.disabled = false; showButton.disabled = false; syncMonthNavigation(); });
  } else {
    analyze();
  }
})();

(() => {
  const root = document.querySelector('[data-status-url]');
  const summary = document.getElementById('summary');
  const results = document.getElementById('missed-results');
  const charts = [];
  const count = row => Number(row['Missed Reservations']) || 0;

  function table(id, rows, columns) {
    const body = document.getElementById(id);
    const elements = rows.map(row => {
      const tr = document.createElement('tr');
      columns.forEach(column => {
        const td = document.createElement('td');
        td.textContent = row[column] ?? '';
        if (column === 'Missed Reservations') td.className = 'numeric';
        tr.append(td);
      });
      return tr;
    });
    if (!elements.length) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = columns.length;
      td.textContent = 'No matching records.';
      tr.append(td);
      elements.push(tr);
    }
    body.replaceChildren(...elements);
  }

  function chart(id, rows, label, color) {
    document.getElementById(`${id}-wrap`).hidden = !rows.length;
    if (!rows.length) return;
    const top = rows.slice(0, 10);
    charts.push(new Chart(document.getElementById(id), {
      type: 'bar',
      data: {
        labels: top.map(row => row[label]),
        datasets: [{label: 'Missed reservations', data: top.map(count), backgroundColor: color}],
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: {legend: {display: false}},
        scales: {
          x: {beginAtZero: true, ticks: {precision: 0}, title: {display: true, text: 'Misses'}},
          y: {grid: {display: false}, ticks: {
            autoSkip: false,
            callback(value) {
              const name = String(this.getLabelForValue(value));
              return name.length > 24 ? `${name.slice(0, 21)}...` : name;
            },
          }},
        },
      },
    }));
  }

  function render(data) {
    const users = [...(data.users || [])].sort((a, b) => count(b) - count(a));
    const tools = [...(data.tools || [])].sort((a, b) => count(b) - count(a));
    results.hidden = false;
    document.getElementById('total-users').textContent = data.total_users ?? 0;
    document.getElementById('flagged-users').textContent = users.length;
    document.getElementById('empty-report').hidden = Number(data.total_users) !== 0;
    document.getElementById('empty-users').hidden = users.length > 0;
    document.getElementById('empty-tools').hidden = tools.length > 0;
    table('users-body', users, ['User', 'Username', 'Missed Reservations', 'Top Missed Tools']);
    table('tools-body', tools, ['Tool', 'Missed Reservations']);
    charts.splice(0).forEach(chart => chart.destroy());
    chart('users-chart', users, 'User', '#0f766e');
    chart('tools-chart', tools, 'Tool', '#b45309');
  }

  async function poll() {
    try {
      const response = await fetch(root.dataset.statusUrl, {cache: 'no-store'});
      if (!response.ok) throw new Error('Job status unavailable');
      const job = await response.json();
      const state = document.getElementById('state');
      state.textContent = job.status.charAt(0).toUpperCase() + job.status.slice(1);
      state.className = `pill ${job.status}`;
      summary.textContent = job.error || job.summary || 'Waiting for a worker...';
      document.getElementById('logs').textContent = (job.logs || []).join('\n');
      document.getElementById('job-progress').hidden = job.finished;
      document.getElementById('bar').style.width = `${job.total ? Math.min(100, 100 * job.current / job.total) : 0}%`;
      document.getElementById('downloads').replaceChildren(...(job.files || []).map(file => {
        const a = document.createElement('a');
        a.className = 'button';
        a.href = file.url;
        a.textContent = file.label;
        return a;
      }));
      if (job.finished) {
        if (job.status === 'completed') render(job.data || {});
        return;
      }
    } catch (error) {
      summary.textContent = `${error.message}; retrying...`;
    }
    window.setTimeout(poll, 1200);
  }
  poll();
})();

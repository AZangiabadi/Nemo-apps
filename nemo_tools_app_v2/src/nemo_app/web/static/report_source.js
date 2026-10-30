document.querySelectorAll('[data-report-source]').forEach(fieldset => {
  const update = () => {
    const api = fieldset.querySelector('input[name="source"]:checked').value === 'api';
    fieldset.querySelector('[data-api-source]').hidden = !api;
    fieldset.querySelector('[data-csv-source]').hidden = api;
    fieldset.querySelector('[name="billing_month"]').required = api;
    fieldset.querySelector('input[type="file"]').required = !api;
    const token = fieldset.querySelector('[data-report-token]');
    if (token) {
      const needed = api || fieldset.dataset.needsMetadata === 'true';
      token.hidden = !needed;
      token.querySelector('input').required = needed;
      token.querySelector('input').disabled = !needed;
    }
  };
  fieldset.querySelectorAll('[name="source"]').forEach(input => {
    input.addEventListener('change', update);
  });
  update();
});

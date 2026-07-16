from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.billing.prepare import prepare_usage_dataframe
from nemo_app.config import AppConfig
from nemo_app.invoices.excel_renderer import render_invoice_workbook
from nemo_app.jobs.handlers import run_job
from nemo_app.jobs.store import create_job_store
from tests.fixtures import usage_frame


class WorkerIntegrationTests(unittest.TestCase):
    def test_worker_claims_job_and_verifies_generated_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                asset_dir=root,
                access_password="",
                jumbotron_api_token="",
                job_secret_key="",
            )
            store = create_job_store(config)
            job_id = "excel-pdf-test"
            input_dir = config.jobs_dir / job_id / "input"
            input_dir.mkdir(parents=True)
            prepared = prepare_usage_dataframe(usage_frame(), apply_caps=False)
            document = InvoiceDocument.from_frame(
                prepared.loc[~prepared["IsMissedReservation"]],
                pi_key="unsafe",
                pi_name="../../Unsafe PI",
                pi_email="pi@example.edu",
                period="2026-04",
                invoice_number="TEST-1",
            )
            render_invoice_workbook(document, input_dir / "input_1.xlsx")
            store.enqueue(
                "excel_pdf",
                title="Excel invoice to PDF",
                payload={"input": "input_1.xlsx"},
                job_id=job_id,
            )

            claimed = store.claim_next("integration-worker")
            self.assertIsNotNone(claimed)
            assert claimed is not None
            run_job(config, store, claimed)

            completed = store.get(job_id)
            self.assertIsNotNone(completed)
            assert completed is not None
            self.assertEqual(completed.status, "completed")
            relative = completed.result["files"][0]
            output = (config.jobs_dir / job_id / relative).resolve()
            self.assertTrue(output.is_file())
            self.assertTrue(output.is_relative_to((config.jobs_dir / job_id).resolve()))


if __name__ == "__main__":
    unittest.main()

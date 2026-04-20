import os
import shutil
import tempfile
import uuid
from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from nemo_invoice_generator_with_pdf import (
    generate_invoices,
    create_invoice_zip,
    NEMO_BASE_URL,
)

app = FastAPI()
JOBS = {}  # job_id -> {"zip_path": ..., "workdir": ...}

HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CNI NEMO Invoice Tool</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 760px; margin: 40px auto; padding: 0 16px; }
    label { display:block; margin-top: 14px; font-weight: 600; }
    input[type="text"], input[type="password"], input[type="file"] { width: 100%; padding: 8px; margin-top: 6px; }
    button { margin-top: 18px; padding: 10px 16px; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 18px; }
  </style>
</head>
<body>
  <h1>CNI NEMO Invoice Generator</h1>
  <div class="card">
    <form action="/generate" method="post" enctype="multipart/form-data">
      <label>Usage CSV</label>
      <input type="file" name="csv_file" required>

      <label>NEMO Base URL</label>
      <input type="text" name="nemo_base" value="https://nemo.cni.columbia.edu">

      <label>API Token (required)</label>
      <input type="password" name="api_token" placeholder="Enter API token" required>

      <label>
        <input type="checkbox" name="generate_pdf" checked> Generate PDF
      </label>

      <label>
        <input type="checkbox" name="bypass_cache" checked> Bypass cache and use live API data
      </label>

      <button type="submit">Generate ZIP</button>
    </form>
  </div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home():
    return HTML_PAGE


@app.post("/generate")
async def generate(
    request: Request,
    csv_file: UploadFile = File(...),
    api_token: str = Form(...),
    nemo_base: str = Form(NEMO_BASE_URL),
    generate_pdf: str = Form("on"),  # checkbox sends "on" when checked
    bypass_cache: str | None = Form(None),
):
    api_token = api_token.strip()
    if not api_token:
        raise HTTPException(status_code=400, detail="API token is required.")

    job_id = str(uuid.uuid4())
    workdir = tempfile.mkdtemp(prefix=f"invoice_{job_id}_")
    csv_path = os.path.join(workdir, csv_file.filename or "input.csv")

    with open(csv_path, "wb") as f:
        f.write(await csv_file.read())

    outdir = os.path.join(workdir, "invoices")
    os.makedirs(outdir, exist_ok=True)

    try:
        _, pdf_created, df, _generated_paths = generate_invoices(
            csv_path=csv_path,
            outdir=outdir,
            nemo_base=(nemo_base or NEMO_BASE_URL).rstrip("/"),
            api_token=api_token,
            generate_pdf=(generate_pdf == "on"),
            logo_path=os.path.join(os.path.dirname(__file__), "columbia_logo.png"),
            use_cache=(bypass_cache != "on"),
        )

        if generate_pdf == "on" and pdf_created == 0:
            raise HTTPException(
                status_code=400,
                detail="PDF generation was requested, but no PDFs were created. Check that reportlab is installed and that PDF generation did not hit an error.",
            )

        zip_path = create_invoice_zip(outdir, df, remove_members=True)
        if not zip_path:
            raise HTTPException(status_code=400, detail="No invoices generated.")
        JOBS[job_id] = {"zip_path": zip_path, "workdir": workdir}

        return HTMLResponse(
            f"""
            <html><body style="font-family: Arial; max-width:760px; margin:40px auto;">
            <h2>Done</h2>
            <p>Your ZIP is ready.</p>
            <p><a href="/download/{job_id}">Download Invoice ZIP</a></p>
            <p><a href="/">Generate another</a></p>
            </body></html>
            """
        )
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/download/{job_id}")
def download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return FileResponse(job["zip_path"], filename=os.path.basename(job["zip_path"]))

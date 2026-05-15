# Nemo Apps Hub

Internal Columbia Nano Initiative web app for launching NEMO-related tools from one place.

Current apps:
- `User/Account/Project Batch Import From Excel`
- `NEMO Invoice Generator`
- `Excel Invoice to PDF`
- `Missed Reservation Report`
- `Active Lab Users`
- `Account/Project Replacement`
- `Jumbotron`

## Project Structure

- `main_app.py`: main Flask web app, routes, background job coordination, and page content builders
- `templates/`: shared Flask/Jinja page shells
- `static/css/`: shared app and jumbotron styles
- `nemo_user_importer.py`: batch user import logic
- `nemo_invoice_generator_with_pdf.py`: invoice generation logic

## Requirements

- Python `3.12`
- [`uv`](https://docs.astral.sh/uv/)

## Setup

Create or refresh the local virtual environment with:

```bash
uv sync
```

If you use the Jumbotron app, create a local `.env` file first:

```bash
cp .env.example .env
```

Then edit `.env` and set:

```bash
NEMO_JUMBOTRON_API_TOKEN=your-token-here
```

If the environment is already installed and you just want to run the app:

```bash
uv run --no-sync main_app.py
```

## Run The App

From the project folder:

```bash
uv run --no-sync main_app.py
```

Then open:

```text
https://127.0.0.1:8000
```

Flask uses an ad-hoc self-signed certificate in this mode, so the browser warning is expected.

## Run With Docker

This project includes a Caddy-based deployment for later public-domain HTTPS.

Set your domain in `.env`:

```bash
TOOLS_NEMO_DOMAIN=toolsnemo.cni.columbia.edu
```

Then make sure:

- `toolsnemo.cni.columbia.edu` points to this machine's public IP address
- inbound TCP ports `80` and `443` are open to this machine
- no other service is already using ports `80` or `443`

Build and start the app:

```bash
docker compose up -d --build
```

Then open your public site:

```text
https://toolsnemo.cni.columbia.edu
```

Useful notes:

- generated invoice ZIPs are persisted to the local `generated_invoices/` folder through a Docker volume mount
- `nemo-app` uses `gunicorn` for production-style serving
- `caddy` terminates TLS and renews certificates automatically once the domain resolves correctly
- the image is based on `python:3.12-slim`, which supports ARM64

If you use the Jumbotron app, the simplest option is to store the token in `.env` and then start Docker:

```bash
docker compose up -d --build
```

Docker Compose automatically reads the local `.env` file in this project folder.

To run in the background on the Mac mini:

```bash
docker compose up -d --build
```

To view logs:

```bash
docker compose logs -f
```

To stop:

```bash
docker compose down
```

## Features

### 1. User/Account/Project Batch Import From Excel

- upload Excel or CSV files
- enter a NEMO API token
- optional dry-run mode
- includes a downloadable Excel template

### 2. NEMO Invoice Generator

- upload a NEMO usage CSV
- enter a NEMO API token
- generate invoice ZIP files
- optional PDF generation when `reportlab` is installed

### 3. Excel Invoice to PDF

- upload an edited NEMO invoice workbook
- generate a matching PDF invoice
- uses the PI email stored in the Excel invoice header

### 4. Missed Reservation Report

- upload a NEMO usage CSV
- list users with 5 or more missed reservation rows
- include usernames when present in the CSV

### 5. Active Lab Users

- enter a NEMO API token
- exports Clean Room, SMCL, and Electron Microscopy by default
- match those labs to tools from the tools API
- export users with qualifications from the past year to an Excel workbook
- writes a combined `All Labs` sheet, one sheet per selected lab, and a summary sheet

### 6. Account/Project Replacement

- enter a NEMO API token plus old and new account/project numbers
- clone the old account and project metadata to the new records
- set today's date as the new `start_date`
- deactivate the old account and project after creating the new records
- dry-run mode is enabled by default

### 7. Jumbotron

- reads its NEMO API token from the `NEMO_JUMBOTRON_API_TOKEN` environment variable
- local development can load that token from a project `.env` file
- view tools currently in use from live usage events
- view upcoming reservations for today and tomorrow
- view today's cancellations, including auto-cancelled missed reservations
- auto-scrolls the page and polls for updates

Example launch:

```bash
uv run --no-sync main_app.py
```

The app will read `.env` automatically if the file exists.

## Notes

- The project uses a local `.venv` managed by `uv`.
- The recommended interpreter for your IDE is:

```text
.venv/bin/python
```

- If plain `uv run` attempts to sync packages, use:

```bash
uv run --no-sync main_app.py
```

## GitHub

Repository:

```text
https://github.com/AZangiabadi/Nemo-apps.git
```

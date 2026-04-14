# Nemo Apps Hub

Internal Columbia Nano Initiative web app for launching NEMO-related tools from one place.

Current apps:
- `User Batch Import From Excel`
- `NEMO Invoice Generator`
- `Jumbotron`

## Project Structure

- [main_app.py](/Users/amiralizangiabadi/Documents/Python%20Codes/General%20Usage%20Nemo%20App/main_app.py): main Flask web app and landing page
- [nemo_user_importer.py](/Users/amiralizangiabadi/Documents/Python%20Codes/General%20Usage%20Nemo%20App/nemo_user_importer.py): batch user import logic
- [nemo_invoice_generator_with_pdf.py](/Users/amiralizangiabadi/Documents/Python%20Codes/General%20Usage%20Nemo%20App/nemo_invoice_generator_with_pdf.py): invoice generation logic

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
http://127.0.0.1:8000
```

## Run With Docker

This project now includes a `Dockerfile` and `docker-compose.yml`, which work well on Apple Silicon including a Mac mini M4.

Build and start the app:

```bash
docker compose up --build
```

Then open:

```text
http://127.0.0.1:8000
```

Useful notes:

- generated invoice ZIPs are persisted to the local `generated_invoices/` folder through a Docker volume mount
- the container uses `gunicorn` for production-style serving
- the image is based on `python:3.12-slim`, which supports ARM64

If you use the Jumbotron app, the simplest option is to store the token in `.env` and then start Docker:

```bash
docker compose up --build
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

### 1. User Batch Import From Excel

- upload Excel or CSV files
- enter a NEMO API token
- optional dry-run mode
- includes a downloadable Excel template

### 2. NEMO Invoice Generator

- upload a NEMO usage CSV
- enter a NEMO API token
- generate invoice ZIP files
- optional PDF generation when `reportlab` is installed

### 3. Jumbotron

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

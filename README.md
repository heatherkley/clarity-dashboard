# Clarity Multi-Project Dashboard

Pulls the last 7 days of data from all your Microsoft Clarity projects and generates a single HTML dashboard.

---

## Setup (one time only)

### Step 1 — Get an API token for each project

1. Go to [clarity.microsoft.com](https://clarity.microsoft.com)
2. Open a project
3. Click **Settings** (gear icon) → **Data Export**
4. Click **Generate new API token**
5. Give it any name (e.g. the project name)
6. **Copy the token immediately** — it's only shown once!
7. Repeat for every project

### Step 2 — Fill in config.json

Open `config.json` in any text editor and replace each `PASTE_YOUR_TOKEN_HERE` with the token you copied for that project.

Example:
```json
{
  "name": "P3 iOS",
  "api_token": "eyJhbGciOiJSUzI1NiIs..."
}
```

Save the file when done.

---

## Running the dashboard

Open **Terminal**, navigate to this folder, and run:

```bash
cd ~/Downloads/clarity-dashboard
python3 clarity_dashboard.py
```

It will fetch data from all your projects and save **clarity_dashboard.html** in the same folder.

Open that file in any browser to view your dashboard.

---

## Refreshing data

Just run `python3 clarity_dashboard.py` again any time — it always pulls the latest 7 days.

---

## Tips

- Projects with no data will show a **yellow dot** — this usually means the token is wrong or the project has no recent traffic
- Click **"Show raw API response"** on any card to see exactly what Clarity returned (useful for troubleshooting)
- The `config.json` file contains your API tokens — keep it private and don't share it

---

## Files

| File | Purpose |
|------|---------|
| `clarity_dashboard.py` | Main script |
| `config.json` | Your project names and API tokens |
| `clarity_dashboard.html` | Generated dashboard (created when you run the script) |

# hikebuddy-weather

Weather backend of the HikeBuddy app. A GitHub Actions job fetches the
[Open-Meteo](https://open-meteo.com/) forecast for a fixed grid of points 5 times a day and
publishes one bundle on GitHub Pages; the app only downloads that bundle and never calls
Open-Meteo itself (the free tier is per IP, and a single search would use it up).

- `cells.json`: the forecast points (written from the app data by `pipeline/weather_cells.py` in the
  app repo; update it after every data export).
- `fetch.py`: the job (Python standard library only). `python fetch.py --limit 3` tries it locally on
  3 cells per level (writes `site/` and `stats/`; don't commit a local run's stats).
- `.github/workflows/fetch.yml`: schedule 04, 08, 12, 16, 20 UTC (05-21 Italian winter time),
  plus a manual "Run workflow" button.
- Published: `https://alessandro-lica.github.io/hikebuddy-weather/latest.json` and the bundle it names.
  A failed run publishes nothing: the previous bundle stays online.

## Monitoring

Every run commits its call stats (this also keeps the schedule alive: GitHub disables scheduled
workflows in repos without activity for 60 days).

- `stats/daily.csv`: per date (Europe/Rome) runs, requests, calls, **max calls in any minute / any
  hour**, refused requests (HTTP 429). Limits of the free tier: 600 / minute, 5,000 / hour,
  10,000 / day. Note: runner IPs are shared with other GitHub users, so a 429 can come from them.
- `stats/runs.csv`: one row per run (status ok / limit / error, calls, bundle size, error note).
- `stats/requests.csv`: every request.

A failed scheduled run also sends GitHub's usual failure e-mail.

Weather data by Open-Meteo.com, CC BY 4.0.

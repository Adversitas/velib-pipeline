"""Static dashboard built from the gold layer (published to GitHub Pages by CI)."""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .quality import run_checks


def _rows(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _pct(x) -> str:
    return "–" if x is None else f"{x * 100:.1f} %".replace(".", ",")


def build(db_path: str | Path, out: str | Path) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    snap = _rows(con, "SELECT * FROM gold.network_snapshot ORDER BY published_at")
    latest = snap[-1]
    hourly = _rows(
        con,
        """SELECT date_trunc('hour', published_at) AS hour, avg(share_empty) AS empty,
                  avg(share_full) AS full, avg(ebikes::DOUBLE / nullif(bikes_typed, 0)) AS ebike_share
           FROM gold.network_snapshot GROUP BY 1 ORDER BY 1""",
    )
    worst = _rows(
        con,
        """SELECT name, capacity, observations, share_empty, share_full, avg_bikes
           FROM gold.station_service WHERE capacity > 0 AND observations >= 3
           ORDER BY share_empty DESC, avg_bikes LIMIT 15""",
    )
    stale = _rows(con, "SELECT name, capacity, days_silent FROM gold.stale_stations LIMIT 20")
    checks = run_checks(con)
    period = (snap[0]["published_at"], latest["published_at"])
    con.close()

    series = {
        "x": [h["hour"].isoformat() for h in hourly],
        "empty": [h["empty"] for h in hourly],
        "full": [h["full"] for h in hourly],
    }
    badge = {"ok": "✅", "warn": "⚠️", "fail": "❌"}
    checks_html = "".join(
        f"<tr><td>{badge[c.status]} {html.escape(c.name)}</td><td>{html.escape(str(c.value))}</td>"
        f"<td>{html.escape(c.rule)}</td></tr>"
        for c in checks
    )
    worst_html = "".join(
        f"<tr><td>{html.escape(w['name'])}</td><td>{w['capacity']}</td><td>{_pct(w['share_empty'])}</td>"
        f"<td>{_pct(w['share_full'])}</td><td>{w['avg_bikes']}</td></tr>"
        for w in worst
    )
    stale_html = "".join(
        f"<tr><td>{html.escape(s['name'] or '?')}</td><td>{s['capacity']}</td>"
        f"<td>{s['days_silent']}</td></tr>"
        for s in stale
    )
    page = TEMPLATE.format(
        start=period[0].strftime("%d/%m/%Y %H:%M"),
        end=period[1].strftime("%d/%m/%Y %H:%M"),
        publications=len(snap),
        stations=latest["stations"],
        bikes=f"{latest['bikes']:,}".replace(",", " "),
        ebike=_pct(latest["ebikes"] / latest["bikes_typed"] if latest["bikes_typed"] else None),
        empty=_pct(latest["share_empty"]),
        full=_pct(latest["share_full"]),
        series=json.dumps(series),
        checks=checks_html,
        worst=worst_html,
        stale=stale_html,
        generated=datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC"),
    )
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page, encoding="utf-8")
    return out


TEMPLATE = """<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vélib' en continu</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2.35.2/plotly.min.js"></script>
<style>
:root {{ color-scheme: light; --bg:#f5f4f1; --card:#fcfcfb; --line:#e3e1dc; --ink:#0b0b0b; --ink2:#52514e;
  --a:#2a78d6; --b:#eb6834; }}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{ color-scheme: dark; --bg:#111110;
  --card:#1a1a19; --line:#2e2d2b; --ink:#f0efec; --ink2:#c3c2b7; --a:#3987e5; --b:#d95926; }} }}
:root[data-theme="dark"] {{ color-scheme: dark; --bg:#111110; --card:#1a1a19; --line:#2e2d2b; --ink:#f0efec;
  --ink2:#c3c2b7; --a:#3987e5; --b:#d95926; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font:14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ max-width:1100px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ font-size:22px; margin:0 0 4px; }} h2 {{ font-size:16px; margin:0 0 8px; }}
.sub {{ color:var(--ink2); margin:0 0 20px; }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(170px,1fr)); gap:12px; margin-bottom:16px; }}
.tile, .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px 16px; }}
.card {{ margin-bottom:16px; min-width:0; }}
.tile .l {{ color:var(--ink2); font-size:13px; }} .tile .v {{ font-size:24px; font-weight:600; font-variant-numeric:tabular-nums; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; font-variant-numeric:tabular-nums; }}
th, td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }}
th {{ color:var(--ink2); }} .scroll {{ overflow-x:auto; }}
.grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }} .grid .card {{ margin:0; }}
@media (max-width:760px) {{ .grid {{ grid-template-columns:1fr; }} }}
footer {{ color:var(--ink2); font-size:12px; margin-top:16px; }} a {{ color:var(--a); }}
</style></head><body><main>
<h1>Vélib' Métropole, suivi en continu</h1>
<p class="sub">{publications} publications collectées du {start} au {end} (UTC) par le pipeline planifié.</p>
<section class="tiles">
  <div class="tile"><div class="l">Stations publiées</div><div class="v">{stations}</div></div>
  <div class="tile"><div class="l">Vélos disponibles</div><div class="v">{bikes}</div></div>
  <div class="tile"><div class="l">Part de vélos électriques</div><div class="v">{ebike}</div></div>
  <div class="tile"><div class="l">Stations vides</div><div class="v">{empty}</div></div>
  <div class="tile"><div class="l">Stations pleines</div><div class="v">{full}</div></div>
</section>
<section class="card"><h2>Stations vides et pleines, par heure</h2><div id="ts"></div></section>
<div class="grid">
  <section class="card"><h2>Contrôles qualité du dernier chargement</h2>
    <div class="scroll"><table><tr><th>Contrôle</th><th>Valeur</th><th>Règle</th></tr>{checks}</table></div></section>
  <section class="card"><h2>Stations muettes depuis plus d'un jour</h2>
    <p class="sub" style="margin:0 0 8px">Toujours publiées par le flux, sans relevé récent.</p>
    <div class="scroll"><table><tr><th>Station</th><th>Capacité</th><th>Jours sans relevé</th></tr>{stale}</table></div></section>
</div>
<section class="card" style="margin-top:16px"><h2>Stations le plus souvent vides</h2>
  <div class="scroll"><table><tr><th>Station</th><th>Capacité</th><th>Temps vide</th><th>Temps pleine</th><th>Vélos en moyenne</th></tr>{worst}</table></div></section>
<footer>Généré le {generated}. Données : flux GBFS ouvert de Vélib' Métropole (Smovengo). Code :
<a href="https://github.com/Adversitas/velib-pipeline">github.com/Adversitas/velib-pipeline</a>.</footer>
</main><script>
const s = {series};
const css = getComputedStyle(document.documentElement);
Plotly.newPlot("ts", [
  {{x: s.x, y: s.empty, name: "Stations vides", mode: "lines+markers", line: {{color: css.getPropertyValue("--a"), width: 2}},
    hovertemplate: "%{{x}}<br>vides : %{{y:.1%}}<extra></extra>"}},
  {{x: s.x, y: s.full, name: "Stations pleines", mode: "lines+markers", line: {{color: css.getPropertyValue("--b"), width: 2}},
    hovertemplate: "%{{x}}<br>pleines : %{{y:.1%}}<extra></extra>"}}
], {{height: 300, margin: {{l: 40, r: 10, t: 10, b: 40}}, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
    font: {{color: css.getPropertyValue("--ink2")}}, yaxis: {{tickformat: ".0%", gridcolor: css.getPropertyValue("--line")}},
    xaxis: {{gridcolor: "rgba(0,0,0,0)"}}, legend: {{orientation: "h", y: 1.1}}}},
  {{displayModeBar: false, responsive: true}});
</script></body></html>
"""

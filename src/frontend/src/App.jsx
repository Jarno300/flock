import { useEffect, useMemo, useState } from "react";

const defaultHistorical = {
  start_year: 2020,
  end_year: new Date().getFullYear(),
  max_per_day: 10000,
};

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";

async function api(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  return response.json();
}

function fmtDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

export default function App() {
  const [status, setStatus] = useState(null);
  const [summary, setSummary] = useState(null);
  const [species, setSpecies] = useState([]);
  const [daily, setDaily] = useState([]);
  const [historicalForm, setHistoricalForm] = useState(defaultHistorical);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const latestDailyRun = useMemo(
    () => status?.daily?.runs?.[0] || null,
    [status]
  );
  const historicalStatus = useMemo(() => status?.historical || null, [status]);

  async function refresh() {
    setError("");
    try {
      const [statusResp, summaryResp, speciesResp, dailyResp] = await Promise.all([
        api("/api/pipeline/status"),
        api("/api/data/summary"),
        api("/api/data/species?limit=15"),
        api("/api/data/daily?days=120"),
      ]);
      setStatus(statusResp);
      setSummary(summaryResp);
      setSpecies(speciesResp);
      setDaily(dailyResp);
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    refresh();
    const handle = setInterval(refresh, 20000);
    return () => clearInterval(handle);
  }, []);

  async function triggerHistoricalRun(event) {
    event.preventDefault();
    setBusy(true);
    setError("");
    setMessage("");
    try {
      const run = await api("/api/historical/run", {
        method: "POST",
        body: JSON.stringify(historicalForm),
      });
      setMessage(`Historical run queued: ${run.job_id}`);
      await refresh();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  async function setDailySchedule(isPaused) {
    setBusy(true);
    setError("");
    setMessage("");
    try {
      await api("/api/daily/schedule", {
        method: "POST",
        body: JSON.stringify({ is_paused: isPaused }),
      });
      setMessage(isPaused ? "Daily schedule paused." : "Daily schedule activated.");
      await refresh();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="layout">
      <header className="header">
        <div>
          <h1>Flock Data Pipeline Console</h1>
          <p>Control ingestion and monitor pipeline + DuckDB analytics.</p>
        </div>
        <button onClick={refresh} disabled={busy}>Refresh</button>
      </header>

      {message ? <div className="notice ok">{message}</div> : null}
      {error ? <div className="notice error">{error}</div> : null}

      <section className="grid two">
        <article className="card">
          <h2>Historical Bulk Run</h2>
          <form onSubmit={triggerHistoricalRun} className="form">
            <label>
              Start Year
              <input
                type="number"
                value={historicalForm.start_year}
                onChange={(e) =>
                  setHistoricalForm({ ...historicalForm, start_year: Number(e.target.value) })
                }
              />
            </label>
            <label>
              End Year
              <input
                type="number"
                value={historicalForm.end_year}
                onChange={(e) =>
                  setHistoricalForm({ ...historicalForm, end_year: Number(e.target.value) })
                }
              />
            </label>
            <label>
              Max Per Day
              <input
                type="number"
                value={historicalForm.max_per_day}
                onChange={(e) =>
                  setHistoricalForm({ ...historicalForm, max_per_day: Number(e.target.value) })
                }
              />
            </label>
            <p className="meta">
              Fixed scope: GBIF occurrence download for Belgium (`BE`), birds (`classKey=212`).
            </p>
            <button type="submit" disabled={busy}>Launch historical run</button>
          </form>
          <p className="meta">
            Historical job status:{" "}
            <strong>{historicalStatus?.status || "idle"}</strong>{" "}
            ({fmtDate(historicalStatus?.started_at)})
          </p>
          {historicalStatus?.error ? (
            <p className="meta">Last error: {historicalStatus.error}</p>
          ) : null}
        </article>

        <article className="card">
          <h2>Daily Schedule</h2>
          <p>
            Status:{" "}
            <strong>{status?.daily?.is_paused ? "Paused" : "Active"}</strong>
          </p>
          <p>Next run: {fmtDate(status?.daily?.next_dagrun)}</p>
          <p>
            Latest run: <strong>{latestDailyRun?.state || "none"}</strong>{" "}
            ({fmtDate(latestDailyRun?.start_date)})
          </p>
          <div className="button-row">
            <button disabled={busy} onClick={() => setDailySchedule(false)}>
              Activate Daily Schedule
            </button>
            <button disabled={busy} onClick={() => setDailySchedule(true)}>
              Pause Daily Schedule
            </button>
          </div>
        </article>
      </section>

      <section className="grid three">
        <article className="card">
          <h3>Raw Rows</h3>
          <div className="stat">{summary?.raw_row_count ?? "-"}</div>
        </article>
        <article className="card">
          <h3>Staging Rows</h3>
          <div className="stat">{summary?.stg_row_count ?? "-"}</div>
        </article>
        <article className="card">
          <h3>Mart Daily Rows</h3>
          <div className="stat">{summary?.mart_daily_rows ?? "-"}</div>
        </article>
      </section>

      <section className="grid two">
        <article className="card">
          <h2>Top Species (Individuals)</h2>
          <table>
            <thead>
              <tr>
                <th>Species</th>
                <th>Individuals</th>
                <th>Obs Records</th>
              </tr>
            </thead>
            <tbody>
              {species.map((row) => (
                <tr key={row.species_id}>
                  <td>{row.species_name || row.species_id}</td>
                  <td>{row.individuals_observed}</td>
                  <td>{row.observation_records}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
        <article className="card">
          <h2>Daily Trend (120 days)</h2>
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Obs Records</th>
                <th>Individuals</th>
              </tr>
            </thead>
            <tbody>
              {daily.slice(-20).map((row) => (
                <tr key={row.observation_date}>
                  <td>{row.observation_date}</td>
                  <td>{row.observation_records}</td>
                  <td>{row.individuals_observed}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      </section>
    </main>
  );
}

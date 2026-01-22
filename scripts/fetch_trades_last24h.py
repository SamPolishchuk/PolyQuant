"""Fetch last 24 hours of trades for conditionIds listed in a CSV.

This script reads a CSV (default: data/market_id.cleaned.csv), extracts unique
values from the `conditionId` column, and queries a Polymarket-style API to
retrieve trades that occurred in the last 24 hours for each conditionId.

Usage:
    python scripts\fetch_trades_last24h.py

Configuration (environment variables / CLI args):
- --input / -i : input CSV path (default: data/market_id.cleaned.csv)
- --out-dir / -o : output directory for JSON results and summary (default: data/trades_last24h)
- --base-url : API base URL (default: https://api.polymarket.com)
- --endpoint-template : endpoint template formatted with {base_url} and {condition_id}
    Default template: {base_url}/conditions/{condition_id}/trades
- --api-key : optional API key (or set POLY_API_KEY env var)
- --workers : number of parallel workers (default 8)

Notes:
- Polymarket's public API shapes and params may differ. This script is written
  to be easily adaptable: change --endpoint-template to match the real endpoint
  and adjust the parsing in `process_response()` if needed.
- The script attempts to fetch trades using a 'since' query parameter with an
  ISO8601 timestamp (start of 24-hour window). If the real API expects different
  params, update `params` construction in `fetch_for_condition()`.

Output:
- For each conditionId, saves a JSON file with the retrieved trades at
  {out_dir}/{condition_id}.json
- A summary CSV at {out_dir}/summary.csv with columns: conditionId, trade_count

"""

from __future__ import annotations
import argparse
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# NOTE: base_url should not include the trailing resource path; endpoint
# template will append the correct path. Historically the template
# "{base_url}/conditions/{condition_id}/trades" caused 404s for our data
# because the real API exposes a consolidated trades endpoint. The probe
# logic will still detect other shapes, but use the empirically-working
# default below so regular runs succeed without --probe.
DEFAULT_BASE_URL = "https://data-api.polymarket.com"
DEFAULT_ENDPOINT_TEMPLATE = "{base_url}/trades"


def probe_endpoint_templates(session: requests.Session, base_url: str, condition_id: str, candidate_templates: List[str], candidate_time_params: List[Optional[str]], candidate_condition_params: List[Optional[str]]):
    """Try several endpoint templates and param names and return the first that yields HTTP 200.

    Returns a tuple (endpoint_template, time_param_name, condition_param_name, tried_list) or (None, None, None, tried_list) if none matched.
    tried_list entries are (url, time_param, condition_param, status_or_error)
    """
    # Helper to build endpoint URL robustly from a template that might
    # contain {base_url} or be an absolute or relative path.
    def build_endpoint(tpl: str) -> str:
        if '{base_url}' in tpl:
            return tpl.format(base_url=base_url.rstrip('/'), condition_id=condition_id)
        if tpl.startswith('http://') or tpl.startswith('https://'):
            return tpl.format(condition_id=condition_id)
        # relative path
        return base_url.rstrip('/') + '/' + tpl.lstrip('/').format(condition_id=condition_id)

    tried = []
    for tpl in candidate_templates:
        endpoint = build_endpoint(tpl)
        # Try without params first (many endpoints include condition in path)
        try_params_list = [None]
        # then try with each time param and condition param combo
        for tparam in candidate_time_params:
            for cparam in candidate_condition_params:
                try_params_list.append((tparam, cparam))

        for entry in try_params_list:
            params = {}
            tparam = None
            cparam = None
            if entry is not None:
                tparam, cparam = entry
                if tparam:
                    # use a placeholder ISO timestamp for probing (UTC)
                    params[tparam] = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
                if cparam:
                    params[cparam] = condition_id
            try:
                if params:
                    resp = session.get(endpoint, params=params, timeout=10)
                else:
                    resp = session.get(endpoint, timeout=10)
            except Exception as e:
                tried.append((endpoint, tparam, cparam, f'ERR: {e}'))
                continue
            tried.append((resp.url, tparam, cparam, resp.status_code))
            if resp.status_code == 200:
                return tpl, tparam, cparam, tried
    return None, None, None, tried


def make_session(retries: int = 3, backoff_factor: float = 0.5, status_forcelist=(429, 500, 502, 503, 504)) -> requests.Session:
    s = requests.Session()
    # allowed_methods accepts an iterable; use a frozenset for compatibility
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist, allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s


def read_unique_condition_ids(csv_path: Path, condition_field: str = 'conditionId') -> List[str]:
    ids = []
    seen = set()
    with csv_path.open('r', encoding='utf-8', errors='replace', newline='') as f:
        reader = csv.DictReader(f)
        if condition_field not in reader.fieldnames:
            raise ValueError(f"condition field '{condition_field}' not present in CSV headers: {reader.fieldnames}")
        for row in reader:
            cid = row.get(condition_field)
            if cid is None:
                continue
            cid = cid.strip()
            if cid == '':
                continue
            if cid not in seen:
                seen.add(cid)
                ids.append(cid)
    return ids


def process_response(data: Any) -> List[Dict[str, Any]]:
    """Extract a list of trades from the API response.

    This function may need adjustment depending on the actual API response
    structure. By default it expects either a top-level list or a JSON object
    with a 'trades' key.
    """
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if 'trades' in data and isinstance(data['trades'], list):
            return data['trades']
        # common patterns
        if 'data' in data and isinstance(data['data'], list):
            return data['data']
    # fallback: wrap single object
    return [data]


def fetch_for_condition(session: requests.Session, base_url: str, endpoint_template: str, condition_id: str, start_iso: str, api_key: Optional[str] = None, since_param: Optional[str] = 'since', condition_param: Optional[str] = None) -> List[Dict[str, Any]]:
    # Build endpoint robustly: template may include {base_url}, be an absolute
    # URL, or be a relative path. Avoid duplicating path segments.
    def build_endpoint(tpl: str) -> str:
        if '{base_url}' in tpl:
            return tpl.format(base_url=base_url.rstrip('/'), condition_id=condition_id)
        if tpl.startswith('http://') or tpl.startswith('https://'):
            return tpl.format(condition_id=condition_id)
        return base_url.rstrip('/') + '/' + tpl.lstrip('/').format(condition_id=condition_id)

    endpoint = build_endpoint(endpoint_template)
    params = {}
    if since_param:
        params[since_param] = start_iso
    if condition_param:
        params[condition_param] = condition_id
    headers = {
        'Accept': 'application/json',
    }
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'

    all_trades: List[Dict[str, Any]] = []
    url = endpoint
    # We'll attempt a single GET and rely on `process_response` to extract trades.
    # If the real API uses pagination, extend this logic (look for 'next' in response).
    resp = session.get(url, params=params, headers=headers, timeout=30)
    # handle 404s explicitly to give a clearer message to the user
    if resp.status_code == 404:
        # provide guidance: often caused by an incorrect base_url or endpoint template
        raise RuntimeError(
            f"404 Not Found for URL: {resp.url}.\n"
            "This usually means the base URL or endpoint template is incorrect.\n"
            "Try using --base-url https://data-api.polymarket.com and the default "
            "--endpoint-template '{base_url}/conditions/{condition_id}/trades', or "
            "update --endpoint-template to match the API you are calling."
        )
    resp.raise_for_status()
    payload = resp.json()
    trades = process_response(payload)

    # filter trades to the last 24 hours defensively (in case the API returns more)
    start_dt = datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
    filtered = []
    for t in trades:
        # try common timestamp fields
        ts = None
        for key in ('createdAt', 'timestamp', 'time', 'created_at'):
            if isinstance(t, dict) and key in t:
                ts = t[key]
                break
        if ts is None:
            # if no timestamp, keep the trade (can't verify)
            filtered.append(t)
            continue
        try:
            # normalize to datetime
            tdt = None
            if isinstance(ts, (int, float)):
                # assume epoch seconds
                tdt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            else:
                tstr = str(ts)
                if tstr.endswith('Z'):
                    tstr = tstr.replace('Z', '+00:00')
                tdt = datetime.fromisoformat(tstr)
            if tdt >= start_dt:
                filtered.append(t)
        except Exception:
            # keep if we can't parse timestamp
            filtered.append(t)
    return filtered


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', default='data/market_id.cleaned.csv', help='Input cleaned CSV (must contain conditionId column)')
    parser.add_argument('-o', '--out-dir', default='data/trades_last24h', help='Directory to write per-condition JSON and summary')
    parser.add_argument('--base-url', default=DEFAULT_BASE_URL, help='API base URL')
    parser.add_argument('--endpoint-template', default=DEFAULT_ENDPOINT_TEMPLATE, help='Endpoint template for fetching trades per condition')
    parser.add_argument('--api-key', default=os.environ.get('POLY_API_KEY'), help='Optional API key for the Polymarket API (or set POLY_API_KEY env var)')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers')
    parser.add_argument('--condition-field', default='conditionId', help='CSV column name that contains condition IDs')
    parser.add_argument('--probe', action='store_true', help='Try to auto-detect the correct endpoint template and param by probing a few patterns')
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f'Reading condition IDs from {input_path}...')
    condition_ids = read_unique_condition_ids(input_path, condition_field=args.condition_field)
    print(f'Found {len(condition_ids)} unique condition IDs')

    # time window: last 24 hours
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(hours=24)
    # ISO format with Z
    start_iso = start_dt.isoformat().replace('+00:00', 'Z')

    session = make_session()

    # If probe mode is requested, attempt to discover a working endpoint
    since_param = 'since'
    condition_param_detected = None
    if args.probe:
        print('Probing common endpoint shapes to discover a working API route...')
        # candidate templates to try (order matters)
        candidates = [
            "{base_url}/conditions/{condition_id}/trades",
            "{base_url}/trades/conditions/{condition_id}",
            "{base_url}/trades/{condition_id}",
            "{base_url}/markets/{condition_id}/trades",
            "{base_url}/conditions/{condition_id}/executions",
            # templates where condition is passed as a query param
            "{base_url}/trades",
            "{base_url}/v1/trades",
            "{base_url}/api/trades",
        ]
        # common param names for time windows
        time_param_names = ['since', 'start', 'from', 'after', None]
        # common param names for the condition id when passed as a query parameter
        condition_param_names = ['conditionId', 'condition', 'market', 'marketId', 'id', None]

        probe_ids = condition_ids[:3] if len(condition_ids) > 0 else []
        found = False
        tried_all = []
        for pid in probe_ids:
            tpl, tparam, cparam, tried = probe_endpoint_templates(session, args.base_url, pid, candidates, time_param_names, condition_param_names)
            tried_all.extend(tried)
            if tpl:
                print(f'Probe discovered working template: {tpl} with time param {tparam} and condition param {cparam}')
                args.endpoint_template = tpl
                since_param = tparam
                condition_param_detected = cparam
                found = True
                break
        if not found:
            print('Probe did not find a working template. Attempts:')
            for u, tpn, cpn, status in tried_all:
                print(f'  tried: {u} time_param={tpn} condition_param={cpn} -> {status}')
            print('You may need to supply --endpoint-template matching the API or run with correct --base-url')


    results: Dict[str, List[Dict[str, Any]]] = {}

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {}
        # If probe detected a condition param, pass it; otherwise condition_param is None
        condition_param = condition_param_detected
        for cid in condition_ids:
            fut = ex.submit(fetch_for_condition, session, args.base_url, args.endpoint_template, cid, start_iso, args.api_key, since_param, condition_param)
            futures[fut] = cid

        for fut in as_completed(futures):
            cid = futures[fut]
            try:
                trades = fut.result()
                results[cid] = trades
                out_file = out_dir / f"{cid}.json"
                with out_file.open('w', encoding='utf-8') as f:
                    json.dump(trades, f, ensure_ascii=False, indent=2)
                print(f'[{cid}] {len(trades)} trades')
            except Exception as e:
                print(f'Error fetching for {cid}: {e}')
                results[cid] = []

    # write summary CSV
    summary_path = out_dir / 'summary.csv'
    with summary_path.open('w', encoding='utf-8', newline='') as sf:
        w = csv.writer(sf)
        w.writerow(['conditionId', 'trade_count'])
        for cid in condition_ids:
            w.writerow([cid, len(results.get(cid, []))])

    print(f'Wrote results to {out_dir} (summary: {summary_path})')


if __name__ == '__main__':
    main()

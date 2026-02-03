"""Compute Black-Litterman P, Q, Omega and posterior returns for prediction markets.

Usage examples:
  python scripts/black_litterman.py --markets 10 --tau 0.025 --delta 2.5
  python scripts/black_litterman.py --views-file views.json

The script:
- Loads trade CSVs from `data/poc_trades/` and builds a per-market price time series.
- Converts probabilities to logit-space and computes returns (first differences).
- Computes covariance `Sigma`, market-implied prior `pi = delta * Sigma * w` (w from volume).
- Builds pick matrix `P`, view vector `Q`, and view uncertainty `Omega` from a JSON views file
  or from a simple example hard-coded view (see README in help output).
- Solves the closed-form Black-Litterman posterior mean `mu_bl`.

Views file format (JSON): list of view objects, each:
  {
    "type": "absolute" | "relative",
    "assets": ["slugA"] or ["slugA","slugB"],
    "weights": [1] or [1,-1],          # optional, default for absolute is [1]
    "value": 0.7,                      # probability for absolute, or numeric relative
    "confidence": 0.5                  # 0-1, optional (default 0.5)
  }

Notes:
- The script uses logit transform: x = log(p/(1-p)). For an absolute probability view
  the Q entry will be set to logit(target_prob) - current_logit, i.e. an expected change
  in logit. This matches the return units used for `pi` and `Sigma`.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def logit(p: float) -> float:
    p = float(p)
    eps = 1e-12
    p = min(max(p, eps), 1 - eps)
    return math.log(p / (1 - p))


def load_trade_price_series(poc_dir: str) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Load all CSVs in `poc_dir` and produce a DataFrame prices[timestamp, market_slug].

    Returns (prices_df, volume_by_slug).
    - prices_df: index=timestamp (sorted), columns=slug, values=last-known price (forward-filled)
    - volume_by_slug: sum of trade `size` per slug (used as proxy for market weight)
    """
    files = [os.path.join(poc_dir, f) for f in os.listdir(poc_dir) if f.endswith('.csv')]
    if not files:
        raise FileNotFoundError(f'No CSVs found in {poc_dir}')

    series_frames = []
    volumes: Dict[str, float] = {}

    for f in files:
        try:
            df = pd.read_csv(f, parse_dates=['timestamp'])
        except Exception:
            # try without parse
            df = pd.read_csv(f)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        if 'price' not in df.columns or 'slug' not in df.columns:
            continue

        # market slug
        slug = df['slug'].iloc[0]
        # drop rows with NaN price
        df = df.dropna(subset=['price'])
        if df.empty:
            continue

        # accumulate volume
        volumes[slug] = float(df['size'].fillna(0).abs().sum())

        # keep timestamp and price; take last price per timestamp
        df = df.sort_values('timestamp')
        # create series of last price per timestamp
        s = df.groupby('timestamp')['price'].last()
        s.name = slug
        series_frames.append(s)

    # combine into a single DataFrame by outer join on timestamps
    if not series_frames:
        raise RuntimeError('No usable trade series found')

    prices = pd.concat(series_frames, axis=1).sort_index()
    # forward-fill last known price
    prices = prices.ffill()

    return prices, volumes


def compute_logit_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """Convert probability prices to logit and compute period returns (first differences).

    Returns a DataFrame of returns aligned by timestamp (index) and columns equal to assets.
    """
    # clip probabilities and compute logit
    probs = prices.clip(1e-9, 1 - 1e-9)
    logits = np.log(probs / (1 - probs))
    returns = logits.diff().dropna(how='all')
    return returns


def market_weights_from_volume(volumes: Dict[str, float], assets: List[str]) -> np.ndarray:
    vals = np.array([volumes.get(a, 0.0) for a in assets], dtype=float)
    if vals.sum() <= 0:
        # fallback to equal weights
        return np.ones(len(assets)) / len(assets)
    return vals / vals.sum()


def build_views(views: List[dict], assets: List[str], current_logits: pd.Series, tauSigma: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build P (k x n), Q (k), Omega (k x k) from a list of view dicts.

    - If view['type']=='absolute' and view['value'] is a probability, Q will be set to
      logit(value) - current_logit_for_asset (i.e., expected change in logit space).
    - For relative views, value is the expected difference in logit-return units.
    - If confidence provided (0..1), omega diagonal will be scaled accordingly.
    """
    n = len(assets)
    P_rows = []
    Q = []
    confidences = []

    for v in views:
        typ = v.get('type', 'absolute')
        a_list = v.get('assets', [])
        w_list = v.get('weights')
        if w_list is None:
            # default: absolute -> [1], relative between two assets -> [1,-1]
            if typ == 'absolute':
                w_list = [1]
            else:
                if len(a_list) >= 2:
                    w_list = [1] + [-1] * (len(a_list) - 1)
                else:
                    w_list = [1]

        row = np.zeros(n, dtype=float)
        for a, w in zip(a_list, w_list):
            if a not in assets:
                raise KeyError(f'View references unknown asset {a}')
            row[assets.index(a)] = float(w)

        # compute Q entry
        val = v.get('value')
        if typ == 'absolute':
            # if value is probability (0..1), convert to logit change relative to current
            if 0 <= float(val) <= 1:
                # find the single asset in view (assume one)
                idxs = np.where(row != 0)[0]
                if len(idxs) != 1:
                    # if multiple assets specified for absolute, treat value as logit target for weighted sum
                    target_logit = logit(val)
                    current = np.dot(row, current_logits.fillna(0.0).values)
                    qval = target_logit - current
                else:
                    i = idxs[0]
                    cur = current_logits.get(assets[i], 0.0)
                    qval = logit(val) - cur
            else:
                qval = float(val)
        else:
            # relative view: user should supply value in logit-return units or as probability delta
            qval = float(val)

        P_rows.append(row)
        Q.append(qval)
        confidences.append(float(v.get('confidence', 0.5)))

    P = np.vstack(P_rows)
    Q = np.array(Q, dtype=float)

    # Omega default: diagonal of P (tauSigma) P^T
    implied = np.diag(P @ tauSigma @ P.T)
    # convert implied to full Omega then scale by confidence: smaller confidence->larger variance
    Omega = np.diag(implied)
    # apply confidence scaling: map c in (0,1) to multiplier (1-c)/c (higher c -> smaller var)
    for i, c in enumerate(confidences):
        c = min(max(c, 1e-6), 1 - 1e-6)
        scale = (1 - c) / c
        Omega[i, i] = Omega[i, i] * scale

    return P, Q, Omega


def black_litterman_posterior(pi: np.ndarray, Sigma: np.ndarray, P: np.ndarray, Q: np.ndarray, Omega: np.ndarray, tau: float) -> Tuple[np.ndarray, np.ndarray]:
    """Compute BL posterior mean and posterior covariance.

    Returns (mu_bl, Sigma_bl)
    """
    n = Sigma.shape[0]
    tauSigma = tau * Sigma

    # use matrix identities for numerical stability
    inv_tauSigma = np.linalg.inv(tauSigma)
    inv_Omega = np.linalg.inv(Omega)

    mid = inv_tauSigma + P.T @ inv_Omega @ P
    # posterior covariance term
    M = np.linalg.inv(mid)
    mu = M @ (inv_tauSigma @ pi + P.T @ inv_Omega @ Q)

    # posterior full covariance (optional)
    Sigma_post = Sigma + M
    return mu, Sigma_post


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--poc-dir', default='data/poc_trades', help='directory with per-market trade CSVs')
    ap.add_argument('--markets', type=int, default=50, help='max number of top markets by volume to include')
    ap.add_argument('--tau', type=float, default=0.025)
    ap.add_argument('--delta', type=float, default=2.5)
    ap.add_argument('--views-file', help='JSON file with views (optional)')
    ap.add_argument('--top-by-volume', type=int, default=50)
    args = ap.parse_args(argv)

    prices, volumes = load_trade_price_series(args.poc_dir)
    # choose top markets by volume
    assets = sorted(prices.columns.tolist(), key=lambda s: -volumes.get(s, 0.0))[: args.top_by_volume]
    prices = prices[assets]

    returns = compute_logit_returns(prices)
    # drop columns with no returns
    returns = returns.dropna(axis=1, how='all')
    assets = [a for a in assets if a in returns.columns]

    Sigma = returns[assets].cov().values
    # market weights from volume
    w = market_weights_from_volume(volumes, assets)
    # prior expected returns (in logit-return units)
    pi = args.delta * Sigma @ w

    tauSigma = args.tau * Sigma

    # current logits (last available)
    last_probs = prices[assets].iloc[-1]
    current_logits = last_probs.apply(lambda p: logit(p) if not pd.isna(p) else 0.0)

    if args.views_file:
        with open(args.views_file, 'r') as fh:
            views = json.load(fh)
    else:
        # example: absolute view that first asset moves to prob 0.8 with confidence 0.6
        if len(assets) == 0:
            raise RuntimeError('No assets with returns to build views')
        views = [
            {
                'type': 'absolute',
                'assets': [assets[0]],
                'value': 0.8,
                'confidence': 0.6,
            }
        ]

    P, Q, Omega = build_views(views, assets, current_logits, tauSigma)

    mu_bl, Sigma_bl = black_litterman_posterior(pi, Sigma, P, Q, Omega, args.tau)

    # print results succinctly
    print('assets (n) =', len(assets))
    print('P shape', P.shape, 'Q shape', Q.shape, 'Omega shape', Omega.shape)
    print('\nSample posterior expected returns (mu_bl) in logit-return units:')
    for a, val in zip(assets, mu_bl):
        print(f'- {a}: {val:.6g}')


if __name__ == '__main__':
    main()

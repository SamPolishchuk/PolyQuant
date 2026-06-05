"""
Black-Litterman Uncertainty Matrix (Ω) Construction
Using Polymarket-Implied Probabilities

Version 1: Probability-Scaled Diagonal  (Wu & Sun, 2025)
Version 2: Logit-Space Covariance Matrix (Deng, Zhou & Cao, 2025)
"""

import numpy as np
from scipy.special import logit


# ─────────────────────────────────────────────────────────────────────────────
# Toy Setup  (2 views, 4 assets — replace with real data)
# ─────────────────────────────────────────────────────────────────────────────

N = 4   # number of assets
K = 2   # number of views / Polymarket events

# Covariance matrix of asset returns  (annualised, e.g. from daily returns)
Sigma = np.array([
    [0.0400, 0.0120, 0.0080, 0.0060],
    [0.0120, 0.0225, 0.0070, 0.0050],
    [0.0080, 0.0070, 0.0196, 0.0040],
    [0.0060, 0.0050, 0.0040, 0.0144],
])

# Pick matrix  (K × N): each row links a view to the assets it affects
# Row 0: long asset 0 vs short asset 1  ("Fed cuts → TLT up, XLF down")
# Row 1: long asset 2                   ("Nvidia earnings beat → NVDA up")
P = np.array([
    [ 1.0, -1.0,  0.0,  0.0],
    [ 0.0,  0.0,  1.0,  0.0],
])

# Confidence scalar τ  (small → trust prior more)
tau = 0.05

# Current Polymarket "Yes" probabilities for each event
p_PM = np.array([0.72, 0.45])   # shape (K,)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helper: prior view covariance  (P τΣ Pᵀ)
# ─────────────────────────────────────────────────────────────────────────────

PtauSigPT = P @ (tau * Sigma) @ P.T          # shape (K, K)
prior_view_var = np.diag(PtauSigPT)          # diagonal entries only, shape (K,)

print("=" * 60)
print("PτΣPᵀ  (prior view covariance):")
print(np.round(PtauSigPT, 6))
print()


# ═════════════════════════════════════════════════════════════════════════════
# VERSION 1 — Probability-Scaled Diagonal  (Wu & Sun, 2025)
#
#   ω_kk = (PτΣPᵀ)_kk / p_PM,k
#
# Intuition: as consensus builds (p → 1), idiosyncratic noise shrinks,
# so the view's influence on the posterior increases.
# Returns a DIAGONAL K×K matrix.
# ═════════════════════════════════════════════════════════════════════════════

def omega_v1(P, Sigma, tau, p_PM):
    """
    Diagonal uncertainty matrix scaled by Polymarket probability.

    Parameters
    ----------
    P      : (K, N) pick matrix
    Sigma  : (N, N) asset covariance
    tau    : scalar confidence in equilibrium
    p_PM   : (K,) current 'Yes' probability for each event

    Returns
    -------
    Omega  : (K, K) diagonal matrix
    """
    PtauSigPT = P @ (tau * Sigma) @ P.T       # (K, K)
    prior_var  = np.diag(PtauSigPT)           # (K,)

    # Guard: clamp p_PM away from 0 to avoid division by zero
    p_safe = np.clip(p_PM, 1e-6, 1.0)

    omega_diag = prior_var / p_safe            # (K,)
    return np.diag(omega_diag)


Omega_v1 = omega_v1(P, Sigma, tau, p_PM)

print("─" * 60)
print("VERSION 1  —  Probability-Scaled Diagonal")
print("─" * 60)
print(f"  Prior view variances  (diag PτΣPᵀ) : {np.round(prior_view_var, 6)}")
print(f"  Polymarket probs       p_PM          : {p_PM}")
print(f"  ω_kk = prior_var / p_PM             : {np.round(np.diag(Omega_v1), 6)}")
print()
print("  Ω (V1):")
print(np.round(Omega_v1, 6))
print()


# ═════════════════════════════════════════════════════════════════════════════
# VERSION 2 — Logit-Space Covariance  (Deng, Zhou & Cao, 2025)
#
#   Ω = Cov( Δlogit(pᵢ), Δlogit(pⱼ) )   over a rolling window
#
# Intuition: mapping bounded [0,1] probs to ℝ via logit(p) = ln(p/(1−p))
# properly amplifies volatility near 0 or 1, reflecting high informational
# value of shifts in high-conviction states.
# Returns a FULL K×K covariance matrix (non-diagonal in general).
# ═════════════════════════════════════════════════════════════════════════════

# Simulated time-series of Polymarket probabilities  (T × K)
# In production: pull historical contract prices from Polymarket API.
np.random.seed(42)
T = 60   # e.g. 60 trading days of observations

# Simulate correlated random-walk probabilities (logit scale → back to [0,1])
# Event 0 (Fed cut) and Event 1 (Nvidia beat) mildly correlated (ρ ≈ 0.3)
cov_sim = np.array([[1.0, 0.3], [0.3, 1.0]]) * 0.04
innovations = np.random.multivariate_normal([0, 0], cov_sim, size=T)

# Start near current probs, walk in logit space
logit_0 = np.log(p_PM[0] / (1 - p_PM[0]))   # initial logit for event 0
logit_1 = np.log(p_PM[1] / (1 - p_PM[1]))   # initial logit for event 1

logit_series = np.zeros((T, K))
logit_series[0] = [logit_0, logit_1]
for t in range(1, T):
    logit_series[t] = logit_series[t-1] + innovations[t]

# Back to [0,1] — clamp to avoid exact 0/1
prob_series = 1 / (1 + np.exp(-logit_series))   # shape (T, K)


def omega_v2(prob_series):
    """
    Full logit-space covariance matrix.

    Parameters
    ----------
    prob_series : (T, K) array of historical Polymarket probabilities

    Returns
    -------
    Omega : (K, K) covariance matrix of logit-space first differences
    """
    # Clamp strictly inside (0, 1) before logit
    p_safe = np.clip(prob_series, 1e-6, 1 - 1e-6)

    logit_probs = np.log(p_safe / (1 - p_safe))      # (T, K) logit-transformed

    delta_logit = np.diff(logit_probs, axis=0)        # (T-1, K) first differences

    # Sample covariance  (ddof=1 for unbiased estimate)
    Omega = np.cov(delta_logit, rowvar=False, ddof=1) # (K, K)
    return Omega, delta_logit


Omega_v2, delta_logit = omega_v2(prob_series)

print("─" * 60)
print("VERSION 2  —  Logit-Space Covariance")
print("─" * 60)
print(f"  Window length T = {T} observations,  K = {K} events")
print()
print(f"  Sample mean  Δlogit(p):  {np.round(delta_logit.mean(axis=0), 5)}")
print(f"  Sample std   Δlogit(p):  {np.round(delta_logit.std(axis=0),  5)}")
print()
print("  Ω (V2)  — full covariance in logit space:")
print(np.round(Omega_v2, 6))
print()
print(f"  Off-diagonal correlation:  {Omega_v2[0,1] / np.sqrt(Omega_v2[0,0]*Omega_v2[1,1]):.4f}")
print()


# ─────────────────────────────────────────────────────────────────────────────
# Summary comparison
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 60)
print("SUMMARY  —  diagonal entries ω_kk")
print("=" * 60)
print(f"  {'View':<8} {'V1 (prob-scaled)':<22} {'V2 (logit-cov)'}")
for k in range(K):
    print(f"  View {k}   {Omega_v1[k,k]:<22.6f} {Omega_v2[k,k]:.6f}")
print()
print("Note: V1 is always diagonal; V2 captures cross-view correlation.")
print("      Lower ω → higher confidence → stronger posterior pull.")

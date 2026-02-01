from __future__ import annotations

import math


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def digital_prob_lognormal(S: float, K: float, sigma_T: float) -> float:
    """
    P(ST > K) under lognormal diffusion with drift ~ 0,
    using sigma_T = stddev of log-price over horizon (dimensionless).
    """
    S = float(S)
    K = float(K)
    sigma_T = float(sigma_T)

    if S <= 0.0 or K <= 0.0:
        return 0.0
    if sigma_T <= 1e-12:
        return 1.0 if S > K else 0.0

    d2 = (math.log(S / K) - 0.5 * sigma_T * sigma_T) / sigma_T
    return norm_cdf(d2)


def digital_prob_normal_points(S: float, K: float, sigma_pts_T: float) -> float:
    """
    P(ST > K) under normal-in-points approximation:
    sigma_pts_T = stddev of price (points) over horizon.
    """
    S = float(S)
    K = float(K)
    sigma_pts_T = float(sigma_pts_T)

    if sigma_pts_T <= 1e-12:
        return 1.0 if S > K else 0.0

    z = (S - K) / sigma_pts_T
    return norm_cdf(z)

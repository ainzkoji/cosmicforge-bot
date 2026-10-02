"""Fixed-window, closed-candle V4 context. No fitted statistics or outcome inputs."""
from __future__ import annotations
import numpy as np

CONTEXT_NAMES=("return_4", "return_24", "realized_vol_24", "atr_14_fraction",
               "ma_distance_24", "trend_slope_24", "log_volume_ratio_24",
               "btc_return_4", "btc_return_24", "eth_return_4", "eth_return_24")


def rolling_mean(x,n):
    x=np.asarray(x,dtype=float); out=np.full(len(x),np.nan)
    c=np.r_[0.,np.cumsum(x)]
    out[n-1:]=(c[n:]-c[:-n])/n
    return out


def closed_candle_context(open_times,opens,highs,lows,closes,volumes,*,span=900000):
    t=np.asarray(open_times,dtype=np.int64)
    if len(t)<25 or np.any(np.diff(t)!=span):
        raise ValueError("contiguous native closed-candle history required")
    c=np.asarray(closes,dtype=float); h=np.asarray(highs); l=np.asarray(lows); v=np.asarray(volumes)
    if np.any(c<=0) or not all(np.all(np.isfinite(a)) for a in (c,h,l,v)):
        raise ValueError("invalid source context")
    log=np.log(c); ret=np.r_[0.,np.diff(log)]
    r4=np.r_[np.full(4,np.nan),log[4:]-log[:-4]]
    r24=np.r_[np.full(24,np.nan),log[24:]-log[:-24]]
    vol=np.sqrt(np.maximum(0.,rolling_mean(ret*ret,24)-rolling_mean(ret,24)**2))
    previous=np.r_[c[0],c[:-1]]
    tr=np.maximum(h-l,np.maximum(abs(h-previous),abs(l-previous)))
    atr=rolling_mean(tr,14)/c; ma=rolling_mean(c,24); distance=c/ma-1
    # Least-squares slope over exactly the closed 24-bar trailing window.
    slope=np.full(len(c),np.nan)
    weights=np.arange(24)-11.5
    slope[23:]=np.convolve(c,weights[::-1],mode='valid')/np.sum(weights**2)/ma[23:]
    vm=rolling_mean(v,24)
    volume=np.log1p(v)-np.log1p(vm)
    return t+span-1,np.column_stack((r4,r24,vol,atr,distance,slope,volume))


def align_context(close_times,values,decision_times):
    """Exact closed-candle alignment; no stale/future or silently neutral inputs."""
    t=np.asarray(close_times); decisions=np.asarray(decision_times)
    i=np.searchsorted(t,decisions,side='right')-1
    if np.any(i<0) or np.any(t[i]!=decisions) or not np.all(np.isfinite(values[i])):
        raise ValueError("causal context missing or timestamp misaligned")
    return values[i],t[i]

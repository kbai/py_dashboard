#!/usr/bin/env python3
"""
Flask dashboard for order analysis with price visualization.
Access at: http://localhost:5050
"""

import os
import io
import base64
from pathlib import Path
import argparse
import re
import time
import shlex
import signal
import subprocess
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session, flash, has_request_context, Response
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import traceback
import socket
import smtplib
import threading
from email.message import EmailMessage

# Bokeh (interactive plots rendered in-browser)
from bokeh.embed import json_item
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure
from bokeh.resources import INLINE, CDN
from typing import Optional, List, Dict, Set, Any


def _bokeh_json_response(payload: Any, status: int = 200) -> Response:
    """Return JSON using Bokeh's encoder (avoids corrupted json_item payloads)."""
    try:
        from bokeh.core.json_encoder import serialize_json
    except Exception:
        # Fallback: standard json (may break some Bokeh payloads)
        import json as _json
        return Response(_json.dumps(payload), status=status, mimetype='application/json')
    return Response(serialize_json(payload), status=status, mimetype='application/json')

# Matplotlib is used for PNG plot generation returned to the browser.
# Force a headless backend so this works on servers without a display.
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Import from grep_to_pandas module
from grep_to_pandas import log_to_csv, parse_order_update_log, parse_grep_output


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, '')).strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _json_safe(v: Any) -> Any:
    """Best-effort conversion to JSON-serializable primitives."""
    try:
        if v is None:
            return None

        # bytes-like values (e.g., q symbols)
        if isinstance(v, (bytes, bytearray)):
            try:
                return bytes(v).decode('utf-8', errors='replace')
            except Exception:
                return str(v)

        # Pandas / numpy missing
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass

        if isinstance(v, (str, int, float, bool)):
            return v

        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode('utf-8', errors='replace')
            except Exception:
                return str(v)

        if isinstance(v, (datetime,)):
            # Keep ISO-8601, assume already timezone-aware or local.
            return v.isoformat()

        # Pandas Timestamp
        try:
            if isinstance(v, pd.Timestamp):
                if v.tzinfo is None:
                    return v.to_pydatetime().replace(tzinfo=None).isoformat()
                return v.to_pydatetime().isoformat()
        except Exception:
            pass

        # numpy scalar
        try:
            if isinstance(v, np.generic):
                return v.item()
        except Exception:
            pass

        # numpy datetime64
        try:
            if isinstance(v, np.datetime64):
                return pd.to_datetime(v).to_pydatetime().isoformat()
        except Exception:
            pass

        if isinstance(v, (list, tuple)):
            return [_json_safe(x) for x in v]

        if isinstance(v, dict):
            return {str(k): _json_safe(val) for k, val in v.items()}

        return str(v)
    except Exception:
        return str(v)


def _timespan_for_minutes(minutes: int) -> str:
    minutes = int(minutes)
    if minutes < 0:
        minutes = 0
    total_s = minutes * 60
    hh = total_s // 3600
    mm = (total_s % 3600) // 60
    ss = total_s % 60
    return f"0D{hh:02d}:{mm:02d}:{ss:02d}"


def _kdb_force_orders_last_minutes(
    minutes: int,
    host: str,
    port: int,
    table: str = 'forceOrder',
    ts_col: str = 'ts',
    timeout_s: float = 1.5,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[List[Dict[str, Any]], str]:
    """Query KDB+ for force orders in the last N minutes.

    Requires a q process listening on host:port with a table named `table`.
    """
    try:
        from qpython import qconnection  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "qpython is not installed in this Python environment. "
            "Use ./start_dashboard.sh to create .venv and install requirements, "
            "or install into your active interpreter with: python3 -m pip install -r requirements.txt"
        ) from e

    span = _timespan_for_minutes(minutes)
    # q: `.z.p` is current timestamp; subtract a timespan.
    # Use a guarded string for table name: restrict to alnum/underscore.
    t = str(table or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', t):
        raise ValueError('invalid kdb table name')

    c = str(ts_col or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', c):
        raise ValueError('invalid kdb timestamp column name')

    # Try the preferred timestamp column; if KDB reports it missing (often surfaced as b'ts'),
    # fall back to common alternatives.
    candidate_cols: List[str] = []
    for col in [c, 'ts', 'time', 'exchangeTime']:
        if col and col not in candidate_cols:
            candidate_cols.append(col)

    q = qconnection.QConnection(
        host=host,
        port=int(port),
        username=username,
        password=password,
        timeout=timeout_s,
    )

    res = None
    last_exc: Optional[Exception] = None
    used_col: Optional[str] = None
    try:
        q.open()
        for col in candidate_cols:
            query = f"select from {t} where {col} > .z.p - {span}"
            try:
                res = q(query)
                used_col = col
                break
            except Exception as e:
                # If the error looks like a missing name (e.g. b'ts' or 'ts), try next.
                msg = str(e).strip()
                if msg in (col, f"b'{col}'", f"'{col}"):
                    last_exc = e
                    continue
                raise
        else:
            if last_exc is not None:
                raise last_exc
    finally:
        try:
            q.close()
        except Exception:
            pass

    if res is None:
        return [], (used_col or c)

    def _col_key(name: Any) -> str:
        if isinstance(name, (bytes, bytearray)):
            try:
                return name.decode('utf-8', errors='replace')
            except Exception:
                return str(name)
        return str(name)

    # qpython typically returns a QTable which behaves like a numpy recarray.
    try:
        names = getattr(getattr(res, 'dtype', None), 'names', None)
        if names:
            nrows = len(res)
            out: List[Dict[str, Any]] = []
            for i in range(nrows):
                row: Dict[str, Any] = {}
                for nm in names:
                    key = _col_key(nm)
                    try:
                        row[key] = _json_safe(res[nm][i])
                    except Exception:
                        # Fallback if indexing by name fails.
                        row[key] = None
                out.append(row)
            return out, (used_col or c)
    except Exception:
        pass

    # Fallback: list of dicts or other structures.
    if isinstance(res, list):
        out: List[Dict[str, Any]] = []
        for item in res:
            if isinstance(item, dict):
                out.append({_col_key(k): _json_safe(v) for k, v in item.items()})
            else:
                out.append({'value': _json_safe(item)})
        return out, (used_col or c)

    if isinstance(res, dict):
        return [{_col_key(k): _json_safe(v) for k, v in res.items()}], (used_col or c)

    return [{'value': _json_safe(res)}], (used_col or c)


def _kdb_force_orders_notional_by_minute(
    minutes: int,
    host: str,
    port: int,
    table: str = 'forceOrder',
    ts_col: str = 'ts',
    timeout_s: float = 1.5,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[List[Dict[str, Any]], str]:
    """Query KDB+ for per-minute BUY/SELL notional sums in the last N minutes.

    Notional is computed as lastPx * lastQty (with nulls treated as 0).
    Returns rows like: {minute: <timestamp>, buy: <float>, sell: <float>}.
    """
    try:
        from qpython import qconnection  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "qpython is not installed in this Python environment. "
            "Use ./start_dashboard.sh to create .venv and install requirements, "
            "or install into your active interpreter with: python3 -m pip install -r requirements.txt"
        ) from e

    span = _timespan_for_minutes(minutes)

    t = str(table or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', t):
        raise ValueError('invalid kdb table name')

    c = str(ts_col or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', c):
        raise ValueError('invalid kdb timestamp column name')

    candidate_cols: List[str] = []
    for col in [c, 'ts', 'time', 'exchangeTime']:
        if col and col not in candidate_cols:
            candidate_cols.append(col)

    q = qconnection.QConnection(
        host=host,
        port=int(port),
        username=username,
        password=password,
        timeout=timeout_s,
    )

    res = None
    last_exc: Optional[Exception] = None
    used_col: Optional[str] = None
    try:
        q.open()
        for col in candidate_cols:
            # Bucket timestamps by minute on the KDB side and compute BUY/SELL sums.
            # 00:01 is a timespan literal (1 minute). xbar floors to bucket.
            query = (
                f"select "
                f"  buy:sum (((0^lastPx)*(0^lastQty))*(side=`BUY)), "
                f"  sell:sum (((0^lastPx)*(0^lastQty))*(side=`SELL)) "
                f"by minute:00:01 xbar {col} "
                f"from {t} "
                f"where {col} > .z.p - {span}"
            )
            try:
                res = q(query)
                used_col = col
                break
            except Exception as e:
                msg = str(e).strip()
                if msg in (col, f"b'{col}'", f"'{col}'"):
                    last_exc = e
                    continue
                last_exc = e
                # For other errors (e.g., missing lastPx/lastQty), stop here and let caller fallback.
                raise
        else:
            if last_exc is not None:
                raise last_exc
    finally:
        try:
            q.close()
        except Exception:
            pass

    if res is None:
        return [], (used_col or c)

    def _col_key(name: Any) -> str:
        if isinstance(name, (bytes, bytearray)):
            try:
                return name.decode('utf-8', errors='replace')
            except Exception:
                return str(name)
        return str(name)

    try:
        names = getattr(getattr(res, 'dtype', None), 'names', None)
        if names:
            nrows = len(res)
            out: List[Dict[str, Any]] = []
            for i in range(nrows):
                row: Dict[str, Any] = {}
                for nm in names:
                    key = _col_key(nm)
                    try:
                        row[key] = _json_safe(res[nm][i])
                    except Exception:
                        row[key] = None
                out.append(row)
            return out, (used_col or c)
    except Exception:
        pass

    if isinstance(res, list):
        out: List[Dict[str, Any]] = []
        for item in res:
            if isinstance(item, dict):
                out.append({_col_key(k): _json_safe(v) for k, v in item.items()})
            else:
                out.append({'value': _json_safe(item)})
        return out, (used_col or c)

    if isinstance(res, dict):
        return [{_col_key(k): _json_safe(v) for k, v in res.items()}], (used_col or c)

    return [{'value': _json_safe(res)}], (used_col or c)


def _q_ts_ns_to_datetime(v: Any) -> Optional[datetime]:
    """Convert q timestamp nanos since 2000-01-01 to a timezone-aware datetime (UTC)."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)
    try:
        # q timestamp is nanoseconds since 2000-01-01.
        if isinstance(v, (int,)):
            ns = int(v)
        elif 'numpy' in globals() and isinstance(v, np.integer):
            ns = int(v)
        else:
            return None
        epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
        return epoch + timedelta(microseconds=(ns // 1000))
    except Exception:
        return None


def _kdb_table_max_timestamp(
    host: str,
    port: int,
    table: str,
    ts_col: str = 'ts',
    timeout_s: float = 1.5,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> tuple[Optional[datetime], str]:
    """Return max timestamp for a table.

    Tries a preferred column name first, then common fallbacks.
    """
    try:
        from qpython import qconnection  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "qpython is not installed in this Python environment. "
            "Use ./start_dashboard.sh to create .venv and install requirements, "
            "or install into your active interpreter with: python3 -m pip install -r requirements.txt"
        ) from e

    t = str(table or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', t):
        raise ValueError('invalid kdb table name')
    c = str(ts_col or '').strip()
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', c):
        raise ValueError('invalid kdb timestamp column name')

    candidate_cols: List[str] = []
    # Common timestamp column names we see in kdb schemas.
    # Muse2 KdbPublisher commonly prepends `ts`, but some deployments use `timestamp`.
    for col in [c, 'ts', 'timestamp', 'Timestamp', 'time', 'exchangeTime', 'ExchangeTime']:
        if col and col not in candidate_cols:
            candidate_cols.append(col)

    q = qconnection.QConnection(
        host=str(host),
        port=int(port),
        username=username,
        password=password,
        timeout=timeout_s,
    )
    used_col: str = c
    try:
        q.open()
        last_exc: Optional[Exception] = None
        for col in candidate_cols:
            try:
                used_col = col
                res = q(f"exec max {col} from {t}")
                dt: Optional[datetime] = None
                if isinstance(res, datetime):
                    dt = res if res.tzinfo is not None else res.replace(tzinfo=timezone.utc)
                else:
                    # qpython may return temporal wrapper types (e.g. QTemporal) for `timestamp` columns.
                    # Those carry an ISO string in `.raw` (e.g. 2026-02-10T05:35:31.325000000).
                    try:
                        from qpython.qtemporal import QTemporal  # type: ignore

                        if isinstance(res, QTemporal) and hasattr(res, 'raw'):
                            raw = getattr(res, 'raw', None)
                            if raw is not None:
                                dt = pd.to_datetime(str(raw)).to_pydatetime()
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                    except Exception:
                        pass

                    if dt is None:
                        dt = _q_ts_ns_to_datetime(res)
                    if dt is None and res is not None:
                        try:
                            dt = pd.to_datetime(res).to_pydatetime()
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            dt = None
                return dt, used_col
            except Exception as e:
                msg = str(e).strip()
                if msg in (col, f"b'{col}'", f"'{col}'"):
                    last_exc = e
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return None, used_col
    finally:
        try:
            q.close()
        except Exception:
            pass


def _tcp_connect_ok(host: str, port: int, timeout_s: float = 1.0) -> tuple[bool, str]:
    try:
        import socket as _socket

        with _socket.create_connection((str(host), int(port)), timeout=float(timeout_s)):
            return True, ''
    except Exception as e:
        return False, str(e)


def _engine_pidfile_path() -> Path:
    raw = str(os.environ.get('MUSE2_ENGINE_PIDFILE', '')).strip()
    if raw:
        return Path(raw).expanduser().resolve()
    # Default to /tmp so it survives cwd changes.
    return Path('/tmp/muse2_engine.pid')


def _engine_log_path() -> Path:
    raw = str(os.environ.get('MUSE2_ENGINE_LOG', '')).strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path('/tmp/muse2_engine.log')


def _engine_read_pid() -> Optional[int]:
    p = _engine_pidfile_path()
    try:
        txt = p.read_text(encoding='utf-8').strip()
        if not txt:
            return None
        pid = int(txt)
        return pid if pid > 1 else None
    except Exception:
        return None


def _engine_is_running(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _engine_status() -> Dict[str, Any]:
    pidfile = _engine_pidfile_path()
    pid = _engine_read_pid()
    running = bool(pid and _engine_is_running(pid))
    cmd = str(os.environ.get('MUSE2_ENGINE_CMD', '')).strip()
    cwd = str(os.environ.get('MUSE2_ENGINE_CWD', '')).strip()
    log_path = _engine_log_path()
    return {
        'running': running,
        'pid': pid,
        'pidfile': str(pidfile),
        'cmdConfigured': bool(cmd),
        'cmd': cmd,
        'cwd': cwd,
        'log': str(log_path),
    }


def _engine_start() -> Dict[str, Any]:
    st = _engine_status()
    if st.get('running'):
        raise RuntimeError('engine already running')

    cmd_raw = str(os.environ.get('MUSE2_ENGINE_CMD', '')).strip()
    if not cmd_raw:
        raise RuntimeError('MUSE2_ENGINE_CMD is not set')

    argv = shlex.split(cmd_raw)
    if not argv:
        raise RuntimeError('invalid MUSE2_ENGINE_CMD')

    cwd_raw = str(os.environ.get('MUSE2_ENGINE_CWD', '')).strip()
    cwd = str(Path(cwd_raw).expanduser().resolve()) if cwd_raw else str(Path(__file__).resolve().parent)

    pidfile = _engine_pidfile_path()
    log_path = _engine_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Start a new session so we can terminate the whole process group.
    with open(log_path, 'ab', buffering=0) as lf:
        p = subprocess.Popen(
            argv,
            cwd=cwd,
            stdout=lf,
            stderr=lf,
            start_new_session=True,
        )

    pidfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.write_text(str(int(p.pid)), encoding='utf-8')
    return _engine_status()


def _engine_kill() -> Dict[str, Any]:
    pid = _engine_read_pid()
    if not pid:
        raise RuntimeError('engine pid not found (no pidfile)')
    if not _engine_is_running(pid):
        # Clean up stale pidfile.
        try:
            _engine_pidfile_path().unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        raise RuntimeError('engine not running (stale pidfile)')

    # Best effort: kill process group first (engine started by dashboard), then pid.
    try:
        os.killpg(int(pid), signal.SIGTERM)
    except Exception:
        os.kill(int(pid), signal.SIGTERM)

    # Wait up to a few seconds, then SIGKILL.
    t0 = time.time()
    while time.time() - t0 < 5.0:
        if not _engine_is_running(pid):
            break
        time.sleep(0.1)
    if _engine_is_running(pid):
        try:
            os.killpg(int(pid), signal.SIGKILL)
        except Exception:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass

    try:
        _engine_pidfile_path().unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        pass
    return _engine_status()


_LATEST_TS_ALERT_LOCK = threading.Lock()
_LATEST_TS_ALERT_STATE: Dict[str, Any] = {
    # Whether the last check observed a stale condition.
    'was_stale': False,
    # When we last sent an alert email (epoch seconds).
    'last_sent_s': 0.0,
}

_LATEST_TS_EMAIL_LOOP_STARTED = False
_LATEST_TS_EMAIL_LOOP_LOCK = threading.Lock()


def _send_email_alert(subject: str, body: str) -> None:
    """Send an email using SMTP settings from env vars.

    Required env vars:
      - DASHBOARD_ALERT_EMAIL_TO (comma-separated)
      - DASHBOARD_ALERT_SMTP_HOST
    Optional:
      - DASHBOARD_ALERT_EMAIL_FROM (default: DASHBOARD_ALERT_SMTP_USER or dashboard@localhost)
      - DASHBOARD_ALERT_SMTP_PORT (default: 587)
      - DASHBOARD_ALERT_SMTP_USER / DASHBOARD_ALERT_SMTP_PASSWORD
      - DASHBOARD_ALERT_SMTP_TLS (default: 1)
    """
    to_raw = str(os.environ.get('DASHBOARD_ALERT_EMAIL_TO', '')).strip()
    smtp_host = str(os.environ.get('DASHBOARD_ALERT_SMTP_HOST', '')).strip()
    if not to_raw or not smtp_host:
        return

    to_addrs = [x.strip() for x in to_raw.split(',') if x.strip()]
    if not to_addrs:
        return

    smtp_port = _env_int('DASHBOARD_ALERT_SMTP_PORT', 587)
    smtp_user = str(os.environ.get('DASHBOARD_ALERT_SMTP_USER', '')).strip() or None
    smtp_pass = str(os.environ.get('DASHBOARD_ALERT_SMTP_PASSWORD', '')).strip() or None
    use_tls = _env_int('DASHBOARD_ALERT_SMTP_TLS', 1) != 0

    from_addr = str(os.environ.get('DASHBOARD_ALERT_EMAIL_FROM', '')).strip()
    if not from_addr:
        from_addr = smtp_user or 'dashboard@localhost'

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addrs)
    msg.set_content(body)

    with smtplib.SMTP(host=smtp_host, port=int(smtp_port), timeout=5) as s:
        if use_tls:
            s.starttls()
        if smtp_user and smtp_pass:
            s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def _maybe_alert_latest_timestamps(trade_dt: Optional[datetime], pb_dt: Optional[datetime]) -> None:
    """Send an email if latest timestamps are older than threshold.

    Controlled by env vars:
      - DASHBOARD_LATEST_TS_ALERT (0/1, default 0)
      - DASHBOARD_LATEST_TS_STALE_SECONDS (default 120)
      - DASHBOARD_LATEST_TS_ALERT_COOLDOWN_SECONDS (default 600)
    """
    if _env_int('DASHBOARD_LATEST_TS_ALERT', 0) == 0:
        return

    stale_s = float(_env_int('DASHBOARD_LATEST_TS_STALE_SECONDS', 120))
    cooldown_s = float(_env_int('DASHBOARD_LATEST_TS_ALERT_COOLDOWN_SECONDS', 600))
    now = datetime.now(timezone.utc)

    def age_seconds(dt: Optional[datetime]) -> Optional[float]:
        if dt is None:
            return None
        dtu = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        return (now - dtu).total_seconds()

    trade_age = age_seconds(trade_dt)
    pb_age = age_seconds(pb_dt)

    trade_stale = (trade_age is None) or (trade_age > stale_s)
    pb_stale = (pb_age is None) or (pb_age > stale_s)
    is_stale = trade_stale or pb_stale

    with _LATEST_TS_ALERT_LOCK:
        last_sent_s = float(_LATEST_TS_ALERT_STATE.get('last_sent_s') or 0.0)
        was_stale = bool(_LATEST_TS_ALERT_STATE.get('was_stale'))

        # Send at most once per stale episode, plus a cooldown (for long outages).
        should_send = False
        if is_stale:
            if (not was_stale) or ((time.time() - last_sent_s) >= cooldown_s):
                should_send = True
        # Update state regardless.
        _LATEST_TS_ALERT_STATE['was_stale'] = bool(is_stale)
        if not should_send:
            return
        _LATEST_TS_ALERT_STATE['last_sent_s'] = float(time.time())

    # Build message outside the lock.
    subject = 'Muse2 Dashboard Alert: stale latest timestamps'
    lines = [
        f'UTC now: {now.isoformat()}',
        f'Threshold seconds: {stale_s}',
        '',
        f'Trade latest: {trade_dt.isoformat() if trade_dt else "n/a"} (age_s={trade_age if trade_age is not None else "n/a"})',
        f'PriceBook latest: {pb_dt.isoformat() if pb_dt else "n/a"} (age_s={pb_age if pb_age is not None else "n/a"})',
    ]
    try:
        _send_email_alert(subject=subject, body='\n'.join(lines))
    except Exception as e:
        # Never break the API response due to alerting problems.
        print(f'latest-ts alert email failed: {e}')


def _latest_timestamps_snapshot() -> Dict[str, Any]:
    """Query KDB for the latest timestamps for trade/pricebook.

    Uses the same env vars as /api/latest-timestamps.
    Returns a dict suitable for email formatting.
    """
    host = str(os.environ.get('MUSE2_KDB_QUERY_HOST', '127.0.0.1')).strip() or '127.0.0.1'
    port = _env_int('MUSE2_KDB_QUERY_PORT', 5011)
    username = str(os.environ.get('MUSE2_KDB_QUERY_USERNAME', 'kbai')).strip() or None
    password = str(os.environ.get('MUSE2_KDB_QUERY_PASSWORD', 'kbai123456')).strip() or None

    trade_table = str(os.environ.get('MUSE2_KDB_TRADE_TABLE', 'trade')).strip() or 'trade'
    pb_table = str(os.environ.get('MUSE2_KDB_PRICEBOOK_TABLE', 'priceBook')).strip() or 'priceBook'

    trade_ts_col = str(os.environ.get('MUSE2_KDB_TRADE_TS_COL', 'ts')).strip() or 'ts'
    pb_ts_col = str(os.environ.get('MUSE2_KDB_PRICEBOOK_TS_COL', 'ts')).strip() or 'ts'

    trade_dt, trade_used = _kdb_table_max_timestamp(
        host=host,
        port=port,
        table=trade_table,
        ts_col=trade_ts_col,
        username=username,
        password=password,
    )
    pb_dt, pb_used = _kdb_table_max_timestamp(
        host=host,
        port=port,
        table=pb_table,
        ts_col=pb_ts_col,
        username=username,
        password=password,
    )

    return {
        'host': host,
        'port': port,
        'tables': {
            'trade': {'table': trade_table, 'tsColRequested': trade_ts_col, 'tsColUsed': trade_used, 'latest': trade_dt},
            'pricebook': {'table': pb_table, 'tsColRequested': pb_ts_col, 'tsColUsed': pb_used, 'latest': pb_dt},
        },
    }


def _latest_timestamps_email_loop() -> None:
    """Send latest timestamps email periodically.

    Controlled by env vars:
      - DASHBOARD_LATEST_TS_EMAIL_EVERY_SECONDS (default 0 = disabled)
      - DASHBOARD_LATEST_TS_EMAIL_SUBJECT_PREFIX (default 'Muse2 Dashboard')
    SMTP settings are the same as _send_email_alert().
    """
    every_s = float(_env_int('DASHBOARD_LATEST_TS_EMAIL_EVERY_SECONDS', 0))
    if every_s <= 0:
        return

    subject_prefix = str(os.environ.get('DASHBOARD_LATEST_TS_EMAIL_SUBJECT_PREFIX', 'Muse2 Dashboard')).strip() or 'Muse2 Dashboard'

    while True:
        try:
            snap = _latest_timestamps_snapshot()
            now = datetime.now(timezone.utc)

            t = snap['tables']['trade']
            p = snap['tables']['pricebook']
            trade_dt = t.get('latest')
            pb_dt = p.get('latest')

            subject = f"{subject_prefix}: latest timestamps"
            body_lines = [
                f"UTC now: {now.isoformat()}",
                f"KDB: {snap.get('host')}:{snap.get('port')}",
                '',
                f"Trade table: {t.get('table')} (tsColUsed={t.get('tsColUsed')}, requested={t.get('tsColRequested')})",
                f"Trade latest: {trade_dt.isoformat() if isinstance(trade_dt, datetime) else 'n/a'}",
                '',
                f"PriceBook table: {p.get('table')} (tsColUsed={p.get('tsColUsed')}, requested={p.get('tsColRequested')})",
                f"PriceBook latest: {pb_dt.isoformat() if isinstance(pb_dt, datetime) else 'n/a'}",
            ]

            _send_email_alert(subject=subject, body='\n'.join(body_lines))
        except Exception as e:
            # Keep looping even if KDB/email temporarily fails.
            print(f'latest-ts periodic email failed: {e}')

        time.sleep(max(1.0, every_s))


def _start_latest_timestamps_email_loop_if_enabled() -> None:
    global _LATEST_TS_EMAIL_LOOP_STARTED
    every_s = float(_env_int('DASHBOARD_LATEST_TS_EMAIL_EVERY_SECONDS', 0))
    if every_s <= 0:
        return

    with _LATEST_TS_EMAIL_LOOP_LOCK:
        if _LATEST_TS_EMAIL_LOOP_STARTED:
            return
        _LATEST_TS_EMAIL_LOOP_STARTED = True

    t = threading.Thread(target=_latest_timestamps_email_loop, name='latest-ts-email', daemon=True)
    t.start()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = '/tmp/order_uploads'
app.secret_key = os.environ.get('DASHBOARD_SECRET_KEY', 'change_this_secret')

# --- Engine err file configuration ---
# Override via:
#   - env var: MUSE2_ERR_FILE
#   - CLI:     dashboard.py --err-file /path/to/err
_REPO_ROOT = Path(__file__).resolve().parent
_DEFAULT_ERR_PATH = _REPO_ROOT / 'src' / 'Muse2' / 'Engine' / 'err'
if not _DEFAULT_ERR_PATH.exists():
    _DEFAULT_ERR_PATH = Path('/home/ubuntu/Muse2_compile/src/Muse2/Engine/err')

app.config['ERR_FILE'] = os.environ.get('MUSE2_ERR_FILE', str(_DEFAULT_ERR_PATH))


def get_err_path() -> str:
    if has_request_context():
        p = str(session.get('ERR_FILE') or '').strip()
        if p:
            return p

    p = str(app.config.get('ERR_FILE') or '').strip()
    return p if p else str(_DEFAULT_ERR_PATH)


def _resolve_path(p: str) -> Path:
    return Path(p).expanduser().resolve()


def _is_under(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except Exception:
        return False


def _is_allowed_err_file(p: Path) -> bool:
    if os.environ.get('MUSE2_ALLOW_ANY_ERR_FILE', '').strip() in ('1', 'true', 'TRUE', 'yes', 'YES'):
        return True
    # By default, restrict to the repo tree (prevents arbitrary filesystem reads).
    return _is_under(p, _REPO_ROOT)


def list_err_candidates(limit: int = 40) -> List[Dict[str, Any]]:
    """Return candidate err/log files under common engine dirs."""
    patterns = (
        'err',
        'err*',
        '*.err',
        '*.log',
        '*.txt',
    )
    search_dirs = [
        _REPO_ROOT / 'src' / 'Muse2' / 'Engine',
        _REPO_ROOT / 'src' / 'Muse2' / 'Engine' / 'log',
        Path.cwd(),
    ]

    seen: Set[Path] = set()
    out: List[Dict[str, Any]] = []
    for d in search_dirs:
        try:
            d = d.resolve()
        except Exception:
            continue
        if not d.exists() or not d.is_dir():
            continue
        for pat in patterns:
            for f in d.glob(pat):
                try:
                    fp = f.resolve()
                except Exception:
                    continue
                if fp in seen:
                    continue
                seen.add(fp)
                if not fp.is_file():
                    continue
                if not _is_allowed_err_file(fp):
                    continue
                try:
                    st = fp.stat()
                    out.append({'path': str(fp), 'size': int(st.st_size), 'mtime': float(st.st_mtime)})
                except Exception:
                    out.append({'path': str(fp), 'size': None, 'mtime': None})

    out.sort(key=lambda x: (x.get('mtime') is not None, x.get('mtime') or 0.0), reverse=True)
    return out[: max(1, int(limit))]


def _normalize_symbol_pair(raw) -> Optional[str]:
    s = str(raw or '').strip().upper()
    if not s:
        return None
    # Strict on purpose: keeps matching predictable and avoids surprising shell/regex behavior.
    if not re.fullmatch(r'[A-Z0-9_]{2,32}', s):
        raise ValueError('invalid symbolPair (expected like ETHUSDT)')
    return s


def _symbol_pair_from_request() -> Optional[str]:
    data = request.get_json(silent=True) or {}
    raw = data.get('symbolPair')
    if raw is None:
        raw = data.get('symbol_pair')
    if raw is None:
        raw = request.args.get('symbolPair')
    if raw is None:
        raw = request.args.get('symbol_pair')
    return _normalize_symbol_pair(raw)


def _grep_fixed_lines(file_path: str, needle: str) -> str:
    """Return matching lines from file using grep -F when available.

    grep returns code 1 when no matches are found; treat that as empty output.
    """
    import subprocess

    try:
        r = subprocess.run(
            ['grep', '-a', '-F', needle, file_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
        )
    except FileNotFoundError:
        # Fallback if grep isn't available.
        out_lines = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    if needle in line:
                        out_lines.append(line.rstrip('\n'))
        except Exception:
            return ''
        return '\n'.join(out_lines)

    if r.returncode in (0, 1):
        return r.stdout or ''
    raise RuntimeError((r.stderr or '').strip() or 'grep failed')

# --- Trader on/off control (IPC via localhost UDP -> TraderControl node) ---
# The C++ TraderControl node listens on a UDP port and publishes ControlSignal.
# Env vars:
#   - MUSE2_TRADER_CTRL_PORT (default 9999)
#   - MUSE2_TRADER_CTRL_HOST (default 127.0.0.1)
TRADER_CTRL_HOST = os.environ.get('MUSE2_TRADER_CTRL_HOST', '127.0.0.1').strip() or '127.0.0.1'
TRADER_CTRL_PORT = int(os.environ.get('MUSE2_TRADER_CTRL_PORT', '9999'))


def _trader_rpc(cmd: str, timeout_s: float = 0.25) -> dict:
    """Send a command to the TraderControl UDP socket and return status.

    Commands:
      - 'get' / 'state' / '?'
      - '1' / 'on' / 'enable'
      - '0' / 'off' / 'disable'
            - 'volmult <float>' / 'volmult=<float>'
            - 'volintercept <float>' / 'volintercept=<float>'
            - 'positionslope <float>' / 'positionslope=<float>'
            - 'macdcoeff <float>' / 'macdcoeff=<float>'

        Response starts with a single byte: '1' or '0' (enabled), and may include
        additional fields like: "1 volmult=1.500000 volintercept=0.002000 positionslope=0.200000 macdcoeff=0.100000".
    """
    import socket

    if not isinstance(cmd, str) or not cmd.strip():
        raise ValueError('empty command')

    msg = cmd.strip().encode('utf-8')
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout_s)
        s.sendto(msg, (TRADER_CTRL_HOST, TRADER_CTRL_PORT))
        data, _addr = s.recvfrom(4096)

    raw = (data or b'').decode('utf-8', errors='replace').strip()

    enabled: Optional[bool] = None
    if raw.startswith('1'):
        enabled = True
    elif raw.startswith('0'):
        enabled = False

    vol_mult: Optional[float] = None
    vol_intercept: Optional[float] = None
    position_slope: Optional[float] = None
    btc_macd_coeff: Optional[float] = None
    try:
        low = raw.lower()
        if 'volmult=' in low:
            part = low.split('volmult=', 1)[1].strip().split()[0]
            vol_mult = float(part)
        if 'volintercept=' in low:
            part = low.split('volintercept=', 1)[1].strip().split()[0]
            vol_intercept = float(part)
        if 'positionslope=' in low:
            part = low.split('positionslope=', 1)[1].strip().split()[0]
            position_slope = float(part)
        if 'macdcoeff=' in low:
            part = low.split('macdcoeff=', 1)[1].strip().split()[0]
            btc_macd_coeff = float(part)
        else:
            # Fallback: response like "1 1.5"
            toks = low.split()
            if len(toks) >= 2 and toks[0] in ('0', '1'):
                vol_mult = float(toks[1])
    except Exception:
        vol_mult = None
        vol_intercept = None
        position_slope = None
        btc_macd_coeff = None

    return {'raw': raw, 'enabled': enabled, 'volMult': vol_mult, 'volIntercept': vol_intercept, 'positionSlope': position_slope, 'btcMacdCoeff': btc_macd_coeff}
# --- Login Required Decorator ---
from functools import wraps
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function
# --- Login Route ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        # Simple demo: hardcoded credentials
        if username == 'admin' and password == 'password':
            session['logged_in'] = True
            flash('Login successful!', 'success')
            next_url = request.args.get('next') or url_for('index')
            return redirect(next_url)
        else:
            flash('Invalid username or password', 'danger')
    return render_template('login.html')

# --- Logout Route ---
@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully.', 'info')
    return redirect(url_for('login'))

# Create upload folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


def _rename_dataview_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Rename generic parsed fields for UI display consistency."""
    if df is None or df.empty:
        return df
    rename_map = {}
    if 'field1' in df.columns:
        rename_map['field1'] = 'lastPx'
    if 'field2' in df.columns:
        rename_map['field2'] = 'lastQty'
    if not rename_map:
        return df
    return df.rename(columns=rename_map)


def _select_fill_price_qty_cols(df: pd.DataFrame) -> tuple[str, Optional[str]]:
    """Choose preferred fill price/qty columns (lastPx/lastQty first, with fallbacks)."""
    price_col = 'lastPx' if 'lastPx' in df.columns else ('price' if 'price' in df.columns else '')
    qty_col: Optional[str]
    if 'lastQty' in df.columns:
        qty_col = 'lastQty'
    elif 'qty' in df.columns:
        qty_col = 'qty'
    elif 'quantity' in df.columns:
        qty_col = 'quantity'
    else:
        qty_col = None
    return price_col, qty_col


def _fill_avg_price_by_side(df: pd.DataFrame, price_col: str, qty_col: Optional[str]) -> dict:
    """Return weighted average fill price for BUY and SELL separately."""
    if df is None or df.empty or 'side' not in df.columns or not price_col:
        return {}

    work = df.copy()
    work['side_norm'] = work['side'].astype(str).str.upper().str.strip()
    work[price_col] = pd.to_numeric(work[price_col], errors='coerce')
    if qty_col is not None and qty_col in work.columns:
        work[qty_col] = pd.to_numeric(work[qty_col], errors='coerce')
    else:
        qty_col = None

    out: dict = {}
    for side in ['BUY', 'SELL']:
        sdf = work[work['side_norm'] == side]
        if sdf.empty:
            continue
        if qty_col is None:
            avg_px = float(sdf[price_col].mean()) if sdf[price_col].notna().any() else None
            if avg_px is not None:
                out[side] = {'avg_price': avg_px, 'qty_total': None, 'qty_col': None, 'price_col': price_col}
            continue

        valid = sdf.dropna(subset=[price_col, qty_col])
        if valid.empty:
            continue
        qty_sum = float(valid[qty_col].sum())
        if qty_sum == 0.0:
            continue
        vwap = float((valid[price_col] * valid[qty_col]).sum() / qty_sum)
        out[side] = {'avg_price': vwap, 'qty_total': qty_sum, 'qty_col': qty_col, 'price_col': price_col}
    return out


def _extract_mybbo_from_err(err_path: str, limit: int = 2000, symbol_pair: Optional[str] = None) -> pd.DataFrame:
    """Extract MyBBO rows from the engine err file.

    Expected line form:
      [MyBBO:SYMBOL HH:MM:SS.mmm] v0,v1,...
    where bid is values[1] and ask is values[4] per existing dashboard logic.
    """
    import re

    if not os.path.exists(err_path):
        return pd.DataFrame()

    pattern = re.compile(r"\[MyBBO:(?P<symbol>\S+)\s+(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})\.(?P<ms>\d{3})\]\s+(?P<body>.*)")
    rows = []
    with open(err_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            m = pattern.search(line)
            if not m:
                continue
            if symbol_pair is not None and m.group('symbol') != symbol_pair:
                continue
            body = m.group('body')
            values = [v.strip() for v in body.split(',')]
            if len(values) < 5:
                continue
            try:
                time_str = f"{m.group('h')}:{m.group('m')}:{m.group('s')}.{m.group('ms')}"
                bid_price = float(values[1])
                ask_price = float(values[4])
            except Exception:
                continue
            rows.append({
                'symbol': m.group('symbol'),
                'time': time_str,
                'bid_price': bid_price,
                'ask_price': ask_price,
            })

    if not rows:
        return pd.DataFrame()

    if limit and len(rows) > limit:
        rows = rows[-limit:]
    df = pd.DataFrame(rows)
    df['time_parsed'] = _parse_time_series(df['time'].astype(str))
    df = df.dropna(subset=['time_parsed', 'bid_price', 'ask_price'])
    if df.empty:
        return df
    return df.sort_values('time_parsed')


def _extract_exbbo_from_err(err_path: str, limit: int = 3000, symbol_pair: Optional[str] = None) -> pd.DataFrame:
    """Extract EXBBO rows from the engine err file.

    Observed line form:
      [EXBBO:SYMBOL HH:MM:SS.mmm] TIME,BID_PX,BID_QTY,?,ASK_PX,ASK_QTY,?
    Example:
      [EXBBO:ETHUSDC 09:08:22.783] 09:08:22.783,2806.0200,138426,0,2806.1200,116024,0
    """
    import re

    if not os.path.exists(err_path):
        return pd.DataFrame()

    header_re = re.compile(r"\[EXBBO:(?P<symbol>\S+)\s+(?P<ts>\d{1,2}:\d{2}:\d{2}\.\d{3})\]\s+(?P<body>.*)")
    rows = []
    with open(err_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            m = header_re.search(line)
            if not m:
                continue
            if symbol_pair is not None and m.group('symbol') != symbol_pair:
                continue
            body = m.group('body').strip()
            parts = [p.strip() for p in body.split(',')]
            if len(parts) < 6:
                continue
            # parts[0] is time, but we also have ts in the header; use body time if present.
            time_str = parts[0] or m.group('ts')
            try:
                bid_price = float(parts[1])
                bid_qty = float(parts[2])
                ask_price = float(parts[4])
                ask_qty = float(parts[5])
            except Exception:
                continue
            rows.append({
                'symbol': m.group('symbol'),
                'time': time_str,
                'bid_price': bid_price,
                'bid_qty': bid_qty,
                'ask_price': ask_price,
                'ask_qty': ask_qty,
            })

    if not rows:
        return pd.DataFrame()
    if limit and len(rows) > limit:
        rows = rows[-limit:]

    df = pd.DataFrame(rows)
    df['time_parsed'] = _parse_time_series(df['time'].astype(str))
    df = df.dropna(subset=['time_parsed', 'bid_price', 'ask_price'])
    if df.empty:
        return df
    return df.sort_values('time_parsed')


def _parse_time_series(series: pd.Series) -> pd.Series:
    """Parse HH:MM:SS(.mmm) into pandas datetime; returns NaT for unparsable values."""
    parsed = pd.to_datetime(series, format='%H:%M:%S.%f', errors='coerce')
    if parsed.isna().all():
        parsed = pd.to_datetime(series, format='%H:%M:%S', errors='coerce')
    return parsed


def _extract_order_latency_from_err(err_path: str, limit: int = 5000, symbol_pair: Optional[str] = None) -> pd.DataFrame:
    """Extract pending_new -> new latency per order from err file."""
    import re

    if not os.path.exists(err_path):
        return pd.DataFrame()

    header_re = re.compile(
        r"\[(?P<tag>OrdReq|OrdUpdate):(?P<symbol>\S+)\s+(?P<ts>\d{1,2}:\d{2}:\d{2}\.\d{3})\]\s+(?P<body>.*)"
    )

    # Track first PENDING_NEW time per order and the file position where it occurred.
    pending_times = {}
    seen_new = set()
    rows = []

    with open(err_path, 'r', encoding='utf-8', errors='replace') as f:
        for line_no, line in enumerate(f):
            m = header_re.search(line)
            if not m:
                continue
            if symbol_pair is not None and m.group('symbol') != symbol_pair:
                continue
            body = m.group('body').strip()
            parts = [p.strip() for p in body.split(',')]
            if len(parts) < 7:
                continue
            order_id = parts[0]
            time_str = parts[2] if len(parts) > 2 and parts[2] else m.group('ts')
            status = parts[6].upper()

            if status == 'PENDING_NEW':
                if order_id not in pending_times:
                    pending_times[order_id] = (time_str, line_no)
                continue

            if status == 'NEW':
                if order_id in pending_times and order_id not in seen_new:
                    pending_time_str, pending_line_no = pending_times[order_id]
                    rows.append({
                        'symbol': m.group('symbol'),
                        'order_id': order_id,
                        'pending_new_time': pending_time_str,
                        'new_time': time_str,
                        'pending_line_no': pending_line_no,
                        'new_line_no': line_no,
                    })
                    seen_new.add(order_id)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df['pending_new_time_parsed'] = _parse_time_series(df['pending_new_time'].astype(str))
    df['new_time_parsed'] = _parse_time_series(df['new_time'].astype(str))
    df = df.dropna(subset=['pending_new_time_parsed', 'new_time_parsed'])
    if df.empty:
        return df

    df['latency_ms'] = (df['new_time_parsed'] - df['pending_new_time_parsed']).dt.total_seconds() * 1000.0
    df = df[df['latency_ms'].notna() & (df['latency_ms'] >= 0.0)]

    # Important: timestamps are HH:MM:SS(.mmm) without a date.
    # If the log crosses midnight, we want to keep some 23:xx (pre-midnight) and
    # some 00:xx (post-midnight) entries, without inventing dates.
    if 'pending_line_no' in df.columns:
        df = df.sort_values('pending_line_no')
    else:
        df = df.sort_values('pending_new_time_parsed')

    if limit and len(df) > limit and 'pending_line_no' in df.columns and 'pending_new_time' in df.columns:
        # Heuristic: if the most recent chunk is after midnight (00:xx) and would
        # exclude 23:xx entirely, split the window around the last non-00 record.
        time_str = df['pending_new_time'].astype(str)
        hour = pd.to_numeric(time_str.str.extract(r'^(\d{1,2}):', expand=False), errors='coerce')
        last_hour = hour.iloc[-1] if len(hour) else None

        if pd.notna(last_hour) and int(last_hour) == 0:
            # Find the boundary between the trailing 00:xx run and the preceding hours.
            boundary_idx = None
            for i in range(len(hour) - 1, -1, -1):
                if pd.isna(hour.iloc[i]):
                    continue
                if int(hour.iloc[i]) != 0:
                    boundary_idx = i
                    break

            if boundary_idx is not None and boundary_idx < (len(df) - 1):
                midnight_start_idx = boundary_idx + 1
                pre = df.iloc[:midnight_start_idx]
                post = df.iloc[midnight_start_idx:]

                # Keep a balanced window around midnight.
                pre_keep = min(len(pre), max(1, limit // 2))
                post_keep = min(len(post), limit - pre_keep)
                pre_slice = pre.tail(pre_keep)
                post_slice = post.tail(post_keep)
                df = pd.concat([pre_slice, post_slice], ignore_index=True)
                df = df.sort_values('pending_line_no')
            else:
                df = df.tail(limit)
        else:
            df = df.tail(limit)
    elif limit and len(df) > limit:
        df = df.tail(limit)
    return df


def bokeh_price_plot_figure(
    df: pd.DataFrame,
    symbol_col: str = 'symbol',
    price_col: str = 'price',
    time_col: str = 'time',
    title: str = 'Price vs Timestamp',
) -> Optional[figure]:
    """Build a Bokeh price-vs-time plot and return the Bokeh Figure."""
    if df is None or df.empty:
        return None
    if price_col not in df.columns or time_col not in df.columns:
        return None

    plot_df = df.copy()

    # Match previous behavior: only show fills when status exists.
    if 'status' in plot_df.columns:
        plot_df = plot_df[plot_df['status'].astype(str).str.contains('FILL', case=False, na=False)]
        if plot_df.empty:
            return None

    plot_df[price_col] = pd.to_numeric(plot_df[price_col], errors='coerce')
    plot_df['time_parsed'] = _parse_time_series(plot_df[time_col].astype(str))
    plot_df = plot_df.dropna(subset=['time_parsed', price_col])
    if plot_df.empty:
        return None

    plot_df = plot_df.sort_values('time_parsed')

    p = figure(
        title=title,
        x_axis_type='datetime',
        height=380,
        sizing_mode='stretch_width',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    p.xaxis.axis_label = 'Time'
    p.yaxis.axis_label = 'Price'

    # If we have side information, render BUY/SELL with requested arrow markers.
    if 'side' in plot_df.columns:
        side_series = plot_df['side'].astype(str).str.upper().str.strip()
        buy_df = plot_df[side_series == 'BUY']
        sell_df = plot_df[side_series == 'SELL']

        qty_col = 'lastQty' if 'lastQty' in plot_df.columns else ('qty' if 'qty' in plot_df.columns else ('quantity' if 'quantity' in plot_df.columns else None))

        def _time_str(s: pd.Series) -> pd.Series:
            # Render as HH:MM:SS.mmm for hover display (best-effort)
            try:
                return pd.to_datetime(s).dt.strftime('%H:%M:%S.%f').str.slice(0, 12)
            except Exception:
                return s.astype(str)

        def _num_str(s: pd.Series, decimals: int = 8) -> pd.Series:
            # Pre-format numbers server-side to avoid client-side numeral.js pattern errors.
            def _fmt(v) -> str:
                try:
                    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                        return ''
                    fv = float(v)
                    return f"{fv:.{decimals}f}"
                except Exception:
                    return str(v)
            return s.apply(_fmt)

        def _marker_size_from_qty(qty: pd.Series, min_size: int = 4, max_size: int = 14) -> pd.Series:
            """Map fill quantity to a marker size (screen units).

            Uses a linear mapping with a soft cap at the 95th percentile so outliers don't dominate.
            """
            q = pd.to_numeric(qty, errors='coerce').abs().fillna(0.0)
            if q.empty:
                return pd.Series([min_size] * 0)

            qcap = float(q.quantile(0.95)) if q.notna().any() else 0.0
            if not np.isfinite(qcap) or qcap <= 0.0:
                qcap = float(q.max())
            if not np.isfinite(qcap) or qcap <= 0.0:
                return pd.Series([min_size] * len(q), index=q.index)

            q = q.clip(lower=0.0, upper=qcap)
            return (min_size + (q / qcap) * (max_size - min_size)).astype(float)

        buy_renderer = None
        sell_renderer = None

        if not buy_df.empty:
            buy_data = {
                'time': buy_df['time_parsed'],
                'price': buy_df[price_col],
                'price_str': _num_str(buy_df[price_col], decimals=8),
                'side': ['BUY'] * len(buy_df),
                'time_str': _time_str(buy_df['time_parsed']),
            }
            if qty_col is not None and qty_col in buy_df.columns:
                buy_data['qty'] = buy_df[qty_col]
                buy_data['qty_str'] = _num_str(buy_df[qty_col], decimals=8)
                buy_data['marker_size'] = _marker_size_from_qty(buy_df[qty_col])
            buy_source = ColumnDataSource(buy_data)

            buy_renderer = p.triangle(
                x='time',
                y='price',
                source=buy_source,
                size=('marker_size' if 'marker_size' in buy_data else 8),
                color='red',
                legend_label='BUY (▲)',
            )
        if not sell_df.empty:
            sell_data = {
                'time': sell_df['time_parsed'],
                'price': sell_df[price_col],
                'price_str': _num_str(sell_df[price_col], decimals=8),
                'side': ['SELL'] * len(sell_df),
                'time_str': _time_str(sell_df['time_parsed']),
            }
            if qty_col is not None and qty_col in sell_df.columns:
                sell_data['qty'] = sell_df[qty_col]
                sell_data['qty_str'] = _num_str(sell_df[qty_col], decimals=8)
                sell_data['marker_size'] = _marker_size_from_qty(sell_df[qty_col])
            sell_source = ColumnDataSource(sell_data)

            sell_renderer = p.inverted_triangle(
                x='time',
                y='price',
                source=sell_source,
                size=('marker_size' if 'marker_size' in sell_data else 8),
                color='green',
                legend_label='SELL (▼)',
            )

        hover_tooltips = [
            ('Side', '@side'),
            ('lastPx', '@price_str'),
        ]
        if qty_col is not None:
            hover_tooltips.append(('lastQty', '@qty_str'))
        hover_tooltips.append(('Time', '@time_str'))

        hover = HoverTool(
            tooltips=hover_tooltips,
            mode='mouse',
            renderers=[r for r in [buy_renderer, sell_renderer] if r is not None],
        )
        p.add_tools(hover)

        if p.legend:
            for leg in p.legend:
                leg.location = 'bottom_left'
                leg.click_policy = 'hide'
        return p

    if symbol_col in plot_df.columns:
        for sym in plot_df[symbol_col].astype(str).fillna('UNKNOWN').unique().tolist():
            sdf = plot_df[plot_df[symbol_col].astype(str) == sym]
            p.line(sdf['time_parsed'], sdf[price_col], legend_label=str(sym), line_width=2)
            p.circle(sdf['time_parsed'], sdf[price_col], size=5, legend_label=str(sym))
        if p.legend:
            for leg in p.legend:
                leg.location = 'bottom_left'
                leg.click_policy = 'hide'
    else:
        p.line(plot_df['time_parsed'], plot_df[price_col], line_width=2)
        p.circle(plot_df['time_parsed'], plot_df[price_col], size=5)

    return p


def bokeh_price_plot_item(
    df: pd.DataFrame,
    symbol_col: str = 'symbol',
    price_col: str = 'price',
    time_col: str = 'time',
    title: str = 'Price vs Timestamp',
) -> Optional[dict]:
    """Build a Bokeh price-vs-time plot and return it as a JSON item."""
    fig = bokeh_price_plot_figure(
        df,
        symbol_col=symbol_col,
        price_col=price_col,
        time_col=time_col,
        title=title,
    )
    if fig is None:
        return None
    return json_item(fig)


def bokeh_quantity_per_minute_item(
    df: pd.DataFrame,
    time_col: str = 'time',
    side_col: str = 'side',
    qty_col_candidates: Optional[List[str]] = None,
    title: str = 'Notional per Minute',
) -> Optional[dict]:
    """Build a per-minute notional bar chart.

    Prefers notional = lastPx * lastQty when available; otherwise falls back to price * quantity/qty
    or raw quantity.

    If a BUY/SELL side column exists, plots two bars (BUY red, SELL green) per minute.
    Otherwise plots total per minute.
    """
    if df is None or df.empty:
        return None

    if time_col not in df.columns:
        return None

    plot_df = df.copy()
    plot_df['time_parsed'] = _parse_time_series(plot_df[time_col].astype(str))
    plot_df = plot_df.dropna(subset=['time_parsed'])
    if plot_df.empty:
        return None

    def _numify(col: str) -> None:
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors='coerce')

    _numify('lastPx')
    _numify('lastQty')
    _numify('price')
    _numify('quantity')
    _numify('qty')

    if 'lastPx' in plot_df.columns and 'lastQty' in plot_df.columns:
        plot_df['_value'] = plot_df['lastPx'] * plot_df['lastQty']
    elif 'price' in plot_df.columns and 'quantity' in plot_df.columns:
        plot_df['_value'] = plot_df['price'] * plot_df['quantity']
    elif 'price' in plot_df.columns and 'qty' in plot_df.columns:
        plot_df['_value'] = plot_df['price'] * plot_df['qty']
    else:
        if qty_col_candidates is None:
            qty_col_candidates = ['quantity', 'qty', 'lastQty', 'cumQty']
        qty_col = next((c for c in qty_col_candidates if c in plot_df.columns), None)
        if qty_col is None:
            return None
        plot_df['_value'] = pd.to_numeric(plot_df[qty_col], errors='coerce')

    plot_df['_value'] = pd.to_numeric(plot_df['_value'], errors='coerce')
    plot_df = plot_df.dropna(subset=['_value'])
    if plot_df.empty:
        return None

    # Floor to minute buckets
    try:
        plot_df['minute_dt'] = pd.to_datetime(plot_df['time_parsed']).dt.floor('min')
    except Exception:
        plot_df['minute_dt'] = plot_df['time_parsed'].apply(lambda d: d.replace(second=0, microsecond=0) if hasattr(d, 'replace') else d)

    # Aggregate
    has_side = side_col in plot_df.columns
    if has_side:
        plot_df[side_col] = plot_df[side_col].astype(str).str.upper().str.strip()
        grouped = (
            plot_df.groupby(['minute_dt', side_col], as_index=False)['_value']
            .sum()
        )
        pivot = grouped.pivot(index='minute_dt', columns=side_col, values='_value').fillna(0.0)
        buy_series = pivot['BUY'] if 'BUY' in pivot.columns else pd.Series(0.0, index=pivot.index)
        sell_series = pivot['SELL'] if 'SELL' in pivot.columns else pd.Series(0.0, index=pivot.index)
        minutes_dt = pivot.index.to_list()
        buy_vals = [float(v) for v in buy_series.to_list()]
        sell_vals = [float(v) for v in sell_series.to_list()]
    else:
        grouped = plot_df.groupby('minute_dt', as_index=False)['_value'].sum().sort_values('minute_dt')
        minutes_dt = grouped['minute_dt'].to_list()
        buy_vals = [float(v) for v in grouped['_value'].to_list()]
        sell_vals = None

    if not minutes_dt:
        return None

    # Ensure we have continuous minute buckets for readability
    try:
        start_dt = min(minutes_dt)
        end_dt = max(minutes_dt)
        bucket_dts = []
        cur = start_dt
        while cur <= end_dt:
            bucket_dts.append(cur)
            cur = cur + timedelta(minutes=1)
    except Exception:
        bucket_dts = minutes_dt

    minute_labels = [pd.to_datetime(d).strftime('%Y-%m-%d %H:%M') for d in bucket_dts]

    if has_side:
        # Re-index to continuous buckets
        idx_map = {pd.to_datetime(d): i for i, d in enumerate(minutes_dt)}
        buy_full: List[float] = []
        sell_full: List[float] = []
        for d in bucket_dts:
            i = idx_map.get(pd.to_datetime(d))
            buy_full.append(float(buy_vals[i]) if i is not None else 0.0)
            sell_full.append(float(sell_vals[i]) if (i is not None and sell_vals is not None) else 0.0)
        source = ColumnDataSource(data={'minute': minute_labels, 'buy': buy_full, 'sell': sell_full})
    else:
        idx_map = {pd.to_datetime(d): i for i, d in enumerate(minutes_dt)}
        total_full: List[float] = []
        for d in bucket_dts:
            i = idx_map.get(pd.to_datetime(d))
            total_full.append(float(buy_vals[i]) if i is not None else 0.0)
        source = ColumnDataSource(data={'minute': minute_labels, 'total': total_full})

    p = figure(
        x_range=minute_labels,
        height=320,
        sizing_mode='stretch_width',
        title=title,
        toolbar_location=None,
    )

    if has_side:
        from bokeh.transform import dodge
        r_buy = p.vbar(
            x=dodge('minute', -0.18, range=p.x_range),
            top='buy',
            width=0.35,
            source=source,
            color='#e74c3c',
            legend_label='BUY',
        )
        r_sell = p.vbar(
            x=dodge('minute', 0.18, range=p.x_range),
            top='sell',
            width=0.35,
            source=source,
            color='#27ae60',
            legend_label='SELL',
        )
        hover = HoverTool(
            tooltips=[('Minute', '@minute'), ('BUY', '@buy{0.00}'), ('SELL', '@sell{0.00}')],
            mode='vline',
            renderers=[r_buy, r_sell],
        )
        p.add_tools(hover)
        p.legend.location = 'top_left'
    else:
        r_total = p.vbar(x='minute', top='total', width=0.8, source=source, color='#3498db', legend_label='Total')
        hover = HoverTool(tooltips=[('Minute', '@minute'), ('Total', '@total{0.00}')], mode='vline', renderers=[r_total])
        p.add_tools(hover)
        p.legend.location = 'top_left'

    p.xaxis.major_label_orientation = 1.1
    p.xgrid.grid_line_color = None
    p.y_range.start = 0
    p.yaxis.axis_label = 'Notional'
    return json_item(p)


def bokeh_mybbo_plot_item(df: pd.DataFrame) -> Optional[dict]:
    if df is None or df.empty:
        return None
    if not {'time', 'bid_price', 'ask_price', 'spread'}.issubset(df.columns):
        return None

    plot_df = df.copy()
    plot_df['time_parsed'] = _parse_time_series(plot_df['time'].astype(str))
    plot_df = plot_df.dropna(subset=['time_parsed'])
    if plot_df.empty:
        return None

    plot_df = plot_df.sort_values('time_parsed')

    p1 = figure(
        title='MyBBO: Bid and Ask Prices',
        x_axis_type='datetime',
        height=320,
        sizing_mode='stretch_width',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    p1.xaxis.axis_label = 'Time'
    p1.yaxis.axis_label = 'Price (USDC)'
    p1.step(plot_df['time_parsed'], plot_df['bid_price'], mode='after', line_width=2, color='#1f77b4', legend_label='Bid')
    p1.step(plot_df['time_parsed'], plot_df['ask_price'], mode='after', line_width=2, color='#ff7f0e', legend_label='Ask')
    p1.legend.click_policy = 'hide'

    p2 = figure(
        title='MyBBO: Spread Over Time',
        x_axis_type='datetime',
        height=320,
        sizing_mode='stretch_width',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    p2.xaxis.axis_label = 'Time'
    p2.yaxis.axis_label = 'Spread (USDC)'
    p2.line(plot_df['time_parsed'], plot_df['spread'], line_width=2, color='#2ca02c', legend_label='Spread')

    return json_item(column(p1, p2, sizing_mode='stretch_width'))


def bokeh_position_plot_figure(
    df: pd.DataFrame,
    symbol_col: str = 'symbol',
    position_col: str = 'position',
    time_col: str = 'time',
    title: str = 'Position vs Time',
) -> Optional[figure]:
    if df is None or df.empty:
        return None
    if position_col not in df.columns or time_col not in df.columns:
        return None

    plot_df = df.copy()
    plot_df[position_col] = pd.to_numeric(plot_df[position_col], errors='coerce')
    plot_df['time_parsed'] = _parse_time_series(plot_df[time_col].astype(str))
    plot_df = plot_df.dropna(subset=['time_parsed', position_col])
    if plot_df.empty:
        return None
    plot_df = plot_df.sort_values('time_parsed')

    def _time_str(s: pd.Series) -> pd.Series:
        try:
            return pd.to_datetime(s).dt.strftime('%H:%M:%S.%f').str.slice(0, 12)
        except Exception:
            return s.astype(str)

    def _num_str(s: pd.Series, decimals: int = 8) -> pd.Series:
        def _fmt(v) -> str:
            try:
                if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                    return ''
                fv = float(v)
                return f"{fv:.{decimals}f}"
            except Exception:
                return str(v)
        return s.apply(_fmt)

    source = ColumnDataSource({
        'time': plot_df['time_parsed'],
        'position': plot_df[position_col],
        'position_str': _num_str(plot_df[position_col], decimals=8),
        'time_str': _time_str(plot_df['time_parsed']),
        'symbol': plot_df[symbol_col].astype(str) if symbol_col in plot_df.columns else [''] * len(plot_df),
    })

    p = figure(
        title=title,
        x_axis_type='datetime',
        height=380,
        sizing_mode='stretch_width',
        tools='pan,wheel_zoom,box_zoom,reset,save',
    )
    p.xaxis.axis_label = 'Time'
    p.yaxis.axis_label = 'Position'

    line_r = p.line('time', 'position', source=source, line_width=2, color='#2c3e50', legend_label='Position')
    circle_r = p.circle('time', 'position', source=source, size=6, color='#3498db', legend_label='Position')

    hover = HoverTool(
        tooltips=[
            ('Symbol', '@symbol'),
            ('Position', '@position_str'),
            ('Time', '@time_str'),
        ],
        mode='mouse',
        renderers=[line_r, circle_r],
    )
    p.add_tools(hover)
    p.legend.click_policy = 'hide'
    return p


def bokeh_position_plot_item(
    df: pd.DataFrame,
    symbol_col: str = 'symbol',
    position_col: str = 'position',
    time_col: str = 'time',
    title: str = 'Position vs Time',
) -> Optional[dict]:
    fig = bokeh_position_plot_figure(
        df,
        symbol_col=symbol_col,
        position_col=position_col,
        time_col=time_col,
        title=title,
    )
    if fig is None:
        return None
    return json_item(fig)


def plot_to_base64(df: pd.DataFrame, symbol_col: str = "symbol", 
                   price_col: str = "price", time_col: str = "time") -> str:
    """
    Convert matplotlib plot to base64 string for embedding in HTML.
    
    Args:
        df: DataFrame containing order data
        symbol_col: Column name containing symbol
        price_col: Column name containing price
        time_col: Column name containing timestamp
    
    Returns:
        Base64 encoded image string
    """
    if df.empty or price_col not in df.columns or time_col not in df.columns:
        return ""
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Convert price and time to numeric types
    df_plot = df.copy()
    df_plot[price_col] = pd.to_numeric(df_plot[price_col], errors='coerce')

    # Only plot rows whose status contains 'FILL' (e.g., FILLED, PARTIALLY_FILLED)
    if 'status' in df_plot.columns:
        df_plot = df_plot[df_plot['status'].astype(str).str.contains('FILL', case=False, na=False)]
        if df_plot.empty:
            plt.close(fig)
            return ""
    
    # Parse time column - handle HH:MM:SS.mmm format
    try:
        df_plot['time_parsed'] = pd.to_datetime(df_plot[time_col], format='%H:%M:%S.%f')
    except:
        try:
            df_plot['time_parsed'] = pd.to_datetime(df_plot[time_col], format='%H:%M:%S')
        except:
            plt.close(fig)
            return ""
    
    # Group by symbol if available
    if symbol_col in df_plot.columns:
        symbols = df_plot[symbol_col].unique()
        colors = plt.cm.tab10(range(len(symbols)))
        
        for idx, symbol in enumerate(symbols):
            symbol_data = df_plot[df_plot[symbol_col] == symbol].sort_values('time_parsed')
            x_vals = symbol_data['time_parsed'].to_numpy()
            y_vals = symbol_data[price_col].to_numpy()
            ax.plot(x_vals, y_vals, 
                   marker='o', label=symbol, color=colors[idx], linewidth=2, markersize=5)
    else:
        df_plot_sorted = df_plot.sort_values('time_parsed')
        x_vals = df_plot_sorted['time_parsed'].to_numpy()
        y_vals = df_plot_sorted[price_col].to_numpy()
        ax.plot(x_vals, y_vals, 
               marker='o', label='Price', linewidth=2, markersize=5)
    
    # Format plot
    ax.set_xlabel('Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Price (USD)', fontsize=12, fontweight='bold')
    ax.set_title('Price vs Timestamp', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=10)
    
    # Format x-axis with time
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=45, ha='right')
    
    # Tight layout
    plt.tight_layout()
    
    # Convert to base64
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
    img_buffer.seek(0)
    img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
    plt.close(fig)
    
    return img_base64


@app.route('/')
@login_required
def index():
    """Serve the main dashboard page."""
    return render_template('dashboard.html')


@app.route('/force-orders')
@login_required
def force_orders_page():
    """Serve the force orders page."""
    # Use INLINE resources here to ensure the browser loads the exact BokehJS
    # version that matches the server-generated json_item payload.
    resources = INLINE
    return render_template(
        'force_orders.html',
        bokeh_js=resources.render_js(),
        bokeh_css=resources.render_css(),
    )


@app.route('/latest')
@login_required
def latest_page():
    """Page showing latest timestamps in KDB for trade/pricebook."""
    _start_latest_timestamps_email_loop_if_enabled()
    return render_template('latest_timestamps.html')


@app.route('/monitor')
@login_required
def monitor_page():
    """Job monitor page for KDB + engine."""
    return render_template('monitor.html')


@app.route('/api/monitor', methods=['GET'])
@login_required
def monitor_api():
    # --- Engine status ---
    engine = _engine_status()
    err_path = get_err_path()
    engine['errFile'] = err_path
    try:
        p = _resolve_path(err_path)
        if p.exists():
            st = p.stat()
            engine['errMTimeUtc'] = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
            engine['errAgeSeconds'] = round(float(time.time() - st.st_mtime), 3)
        else:
            engine['errMTimeUtc'] = None
            engine['errAgeSeconds'] = None
    except Exception as e:
        engine['errMTimeUtc'] = None
        engine['errAgeSeconds'] = None
        engine['errStatError'] = str(e)

    # --- KDB status ---
    host = str(os.environ.get('MUSE2_KDB_QUERY_HOST', '127.0.0.1')).strip() or '127.0.0.1'
    port = _env_int('MUSE2_KDB_QUERY_PORT', 5011)
    username = str(os.environ.get('MUSE2_KDB_QUERY_USERNAME', 'kbai')).strip() or None
    password = str(os.environ.get('MUSE2_KDB_QUERY_PASSWORD', 'kbai123456')).strip() or None

    kdb: Dict[str, Any] = {
        'host': host,
        'port': port,
    }
    ok, err = _tcp_connect_ok(host, port, timeout_s=1.0)
    kdb['tcpOk'] = ok
    kdb['tcpError'] = err if not ok else None

    tables_raw = str(os.environ.get('MUSE2_KDB_MONITOR_TABLES', 'trade,priceBook,order,forceOrder')).strip()
    table_list = [t.strip() for t in tables_raw.split(',') if t.strip()]
    kdb['tablesRequested'] = table_list
    kdb['tables'] = {}

    if ok:
        try:
            from qpython import qconnection  # type: ignore

            q = qconnection.QConnection(host=host, port=int(port), username=username, password=password, timeout=1.5)
            try:
                q.open()
                try:
                    srv_time = q('.z.p')
                    kdb['serverTimeRaw'] = _json_safe(srv_time)
                except Exception as e:
                    kdb['serverTimeRaw'] = None
                    kdb['serverTimeError'] = str(e)

                for tname in table_list:
                    info: Dict[str, Any] = {'table': tname}
                    try:
                        cnt = q(f'count {tname}')
                        info['count'] = _json_safe(cnt)
                    except Exception as e:
                        info['count'] = None
                        info['countError'] = str(e)

                    try:
                        dt, used = _kdb_table_max_timestamp(
                            host=host,
                            port=port,
                            table=tname,
                            ts_col='ts',
                            username=username,
                            password=password,
                        )
                        info['latest'] = dt.isoformat() if dt else None
                        info['tsColUsed'] = used
                    except Exception as e:
                        info['latest'] = None
                        info['latestError'] = str(e)

                    kdb['tables'][tname] = info
            finally:
                try:
                    q.close()
                except Exception:
                    pass
        except Exception as e:
            kdb['ipcError'] = str(e)

    return jsonify({'engine': engine, 'kdb': kdb, 'nowUtc': datetime.now(timezone.utc).isoformat()})


@app.route('/api/engine/start', methods=['POST'])
@login_required
def engine_start_api():
    try:
        st = _engine_start()
        return jsonify({'ok': True, 'engine': st})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'engine': _engine_status()}), 400


@app.route('/api/engine/kill', methods=['POST'])
@login_required
def engine_kill_api():
    try:
        st = _engine_kill()
        return jsonify({'ok': True, 'engine': st})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'engine': _engine_status()}), 400


@app.route('/api/latest-timestamps', methods=['GET'])
@login_required
def latest_timestamps_api():
    _start_latest_timestamps_email_loop_if_enabled()
    host = str(os.environ.get('MUSE2_KDB_QUERY_HOST', '127.0.0.1')).strip() or '127.0.0.1'
    port = _env_int('MUSE2_KDB_QUERY_PORT', 5011)
    username = str(os.environ.get('MUSE2_KDB_QUERY_USERNAME', 'kbai')).strip() or None
    password = str(os.environ.get('MUSE2_KDB_QUERY_PASSWORD', 'kbai123456')).strip() or None

    trade_table = str(os.environ.get('MUSE2_KDB_TRADE_TABLE', 'trade')).strip() or 'trade'
    pb_table = str(os.environ.get('MUSE2_KDB_PRICEBOOK_TABLE', 'priceBook')).strip() or 'priceBook'

    trade_ts_col = str(os.environ.get('MUSE2_KDB_TRADE_TS_COL', 'ts')).strip() or 'ts'
    pb_ts_col = str(os.environ.get('MUSE2_KDB_PRICEBOOK_TS_COL', 'ts')).strip() or 'ts'

    try:
        t0 = time.perf_counter()
        trade_dt, trade_used = _kdb_table_max_timestamp(
            host=host,
            port=port,
            table=trade_table,
            ts_col=trade_ts_col,
            username=username,
            password=password,
        )
        pb_dt, pb_used = _kdb_table_max_timestamp(
            host=host,
            port=port,
            table=pb_table,
            ts_col=pb_ts_col,
            username=username,
            password=password,
        )

        _maybe_alert_latest_timestamps(trade_dt=trade_dt, pb_dt=pb_dt)
        total_ms = (time.perf_counter() - t0) * 1000.0
        return jsonify(
            {
                'host': host,
                'port': port,
                'tables': {
                    'trade': {'table': trade_table, 'tsColRequested': trade_ts_col, 'tsColUsed': trade_used, 'latest': trade_dt.isoformat() if trade_dt else None},
                    'pricebook': {'table': pb_table, 'tsColRequested': pb_ts_col, 'tsColUsed': pb_used, 'latest': pb_dt.isoformat() if pb_dt else None},
                },
                'timing': {'total_ms': round(float(total_ms), 2)},
            }
        )
    except (ConnectionRefusedError, socket.timeout) as e:
        return jsonify({'error': f'KDB not reachable at {host}:{port}: {e}', 'host': host, 'port': port}), 503
    except Exception as e:
        return jsonify({'error': str(e), 'host': host, 'port': port}), 503


@app.route('/api/force-orders/bars', methods=['GET'])
@login_required
def force_orders_bars_api():
    """Return a Bokeh bar chart (json_item) of BUY/SELL force orders per minute."""
    from bokeh.models import ColumnDataSource
    from bokeh.transform import dodge

    host = str(os.environ.get('MUSE2_KDB_QUERY_HOST', '127.0.0.1')).strip() or '127.0.0.1'
    port = _env_int('MUSE2_KDB_QUERY_PORT', 5011)
    table = str(os.environ.get('MUSE2_KDB_FORCEORDER_TABLE', 'forceOrder')).strip() or 'forceOrder'
    ts_col = str(os.environ.get('MUSE2_KDB_FORCEORDER_TS_COL', 'time')).strip() or 'time'

    username = str(os.environ.get('MUSE2_KDB_QUERY_USERNAME', 'kbai')).strip() or None
    password = str(os.environ.get('MUSE2_KDB_QUERY_PASSWORD', 'kbai123456')).strip() or None

    minutes_default = _env_int('MUSE2_FORCE_ORDERS_MINUTES', 30)
    try:
        minutes = int(request.args.get('minutes', str(minutes_default)))
    except Exception:
        minutes = minutes_default
    minutes = max(1, min(240, minutes))

    t_total0 = time.perf_counter()
    used_ts_col = ts_col
    buy_by_min: Dict[str, float] = {}
    sell_by_min: Dict[str, float] = {}
    observed_max_dt: Optional[datetime] = None
    mode = 'kdb_agg'
    kdb_ms: Optional[float] = None
    agg_error: Optional[str] = None
    rows: List[Dict[str, Any]] = []

    # Prefer KDB-side aggregation (much faster / smaller payload)
    try:
        t_kdb0 = time.perf_counter()
        agg_rows, used_ts_col = _kdb_force_orders_notional_by_minute(
            minutes=minutes,
            host=host,
            port=port,
            table=table,
            ts_col=ts_col,
            username=username,
            password=password,
        )
        kdb_ms = (time.perf_counter() - t_kdb0) * 1000.0

        for r in agg_rows:
            if not isinstance(r, dict):
                continue
            minute_val = r.get('minute')

            dt = None
            if isinstance(minute_val, datetime):
                dt = minute_val if minute_val.tzinfo is not None else minute_val.replace(tzinfo=timezone.utc)
            else:
                dt = _q_ts_ns_to_datetime(minute_val)
                if dt is None:
                    try:
                        dt = pd.to_datetime(minute_val).to_pydatetime().replace(tzinfo=timezone.utc)
                    except Exception:
                        dt = None
            if dt is None:
                continue
            if observed_max_dt is None or dt > observed_max_dt:
                observed_max_dt = dt
            minute_label = dt.replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M')

            try:
                buy_by_min[minute_label] = float(r.get('buy') or 0.0)
            except Exception:
                buy_by_min[minute_label] = 0.0
            try:
                sell_by_min[minute_label] = float(r.get('sell') or 0.0)
            except Exception:
                sell_by_min[minute_label] = 0.0

    except Exception as e:
        # Fallback: fetch raw rows and bucket in Python
        mode = 'python_bucket'
        agg_error = str(e)
        t_kdb0 = time.perf_counter()
        rows, used_ts_col = _kdb_force_orders_last_minutes(
            minutes=minutes,
            host=host,
            port=port,
            table=table,
            ts_col=ts_col,
            username=username,
            password=password,
        )
        kdb_ms = (time.perf_counter() - t_kdb0) * 1000.0

    def _to_float(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None

    if mode == 'python_bucket':
        for r in rows:
            if not isinstance(r, dict):
                continue
            side = str(r.get('side') or '').upper().strip()

            last_px = _to_float(r.get('lastPx'))
            last_qty = _to_float(r.get('lastQty'))
            px = _to_float(r.get('price'))
            qty = _to_float(r.get('quantity'))

            notional: Optional[float] = None
            if last_px is not None and last_qty is not None:
                notional = float(last_px) * float(last_qty)
            elif px is not None and qty is not None:
                notional = float(px) * float(qty)
            elif px is not None and last_qty is not None:
                notional = float(px) * float(last_qty)
            elif last_px is not None and qty is not None:
                notional = float(last_px) * float(qty)

            if notional is None:
                notional = _to_float(r.get('quantity'))
                if notional is None:
                    notional = _to_float(r.get('lastQty'))
                if notional is None:
                    notional = 0.0

            ts_val = r.get(used_ts_col)
            dt = _q_ts_ns_to_datetime(ts_val)
            if dt is None:
                continue
            if observed_max_dt is None or dt > observed_max_dt:
                observed_max_dt = dt
            minute_dt = dt.replace(second=0, microsecond=0)
            minute_label = minute_dt.strftime('%Y-%m-%d %H:%M')
            if side == 'BUY':
                buy_by_min[minute_label] = float(buy_by_min.get(minute_label, 0.0)) + float(notional)
            elif side == 'SELL':
                sell_by_min[minute_label] = float(sell_by_min.get(minute_label, 0.0)) + float(notional)

    # Always generate buckets for the entire window so the chart is not blank
    # when there are simply no force orders in the last N minutes.
    end_dt = (observed_max_dt or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(minutes=max(0, minutes - 1))
    minutes_sorted: List[str] = []
    cur = start_dt
    while cur <= end_dt:
        minutes_sorted.append(cur.strftime('%Y-%m-%d %H:%M'))
        cur = cur + timedelta(minutes=1)

    buy_vol = [float(buy_by_min.get(m, 0.0)) for m in minutes_sorted]
    sell_vol = [float(sell_by_min.get(m, 0.0)) for m in minutes_sorted]

    t_plot0 = time.perf_counter()
    source = ColumnDataSource(data=dict(minute=minutes_sorted, buy=buy_vol, sell=sell_vol))
    p = figure(
        x_range=minutes_sorted,
        height=320,
        sizing_mode='stretch_width',
        title='Force Orders Notional per Minute',
        toolbar_location=None,
    )
    p.vbar(
        x=dodge('minute', -0.18, range=p.x_range),
        top='buy',
        width=0.35,
        source=source,
        color='#e74c3c',
        legend_label='BUY',
    )
    p.vbar(
        x=dodge('minute', 0.18, range=p.x_range),
        top='sell',
        width=0.35,
        source=source,
        color='#27ae60',
        legend_label='SELL',
    )

    p.xaxis.major_label_orientation = 1.1
    p.xgrid.grid_line_color = None
    p.y_range.start = 0
    p.yaxis.axis_label = 'Notional'
    p.legend.location = 'top_left'

    item = json_item(p, 'forceOrdersBarChart')
    plot_ms = (time.perf_counter() - t_plot0) * 1000.0
    total_ms = (time.perf_counter() - t_total0) * 1000.0
    return _bokeh_json_response(
        {
            'minutes': minutes,
            'host': host,
            'port': port,
            'table': table,
            'tsColRequested': ts_col,
            'tsColUsed': used_ts_col,
            'timing': {
                'mode': mode,
                'kdb_ms': round(float(kdb_ms or 0.0), 2),
                'plot_ms': round(float(plot_ms or 0.0), 2),
                'total_ms': round(float(total_ms or 0.0), 2),
            },
            'agg_error': agg_error,
            'item': item,
        }
    )


@app.route('/api/force-orders', methods=['GET'])
@login_required
def force_orders_api():
    """Return force orders (liquidations) from KDB for the last N minutes."""
    t_total0 = time.perf_counter()
    host = str(os.environ.get('MUSE2_KDB_QUERY_HOST', '127.0.0.1')).strip() or '127.0.0.1'
    port = _env_int('MUSE2_KDB_QUERY_PORT', 5011)
    table = str(os.environ.get('MUSE2_KDB_FORCEORDER_TABLE', 'forceOrder')).strip() or 'forceOrder'
    ts_col = str(os.environ.get('MUSE2_KDB_FORCEORDER_TS_COL', 'time')).strip() or 'time'

    # KDB IPC credentials (optional). Defaults provided per request.
    username = str(os.environ.get('MUSE2_KDB_QUERY_USERNAME', 'kbai')).strip() or None
    password = str(os.environ.get('MUSE2_KDB_QUERY_PASSWORD', 'kbai123456')).strip() or None

    minutes_default = _env_int('MUSE2_FORCE_ORDERS_MINUTES', 30)
    try:
        minutes = int(request.args.get('minutes', str(minutes_default)))
    except Exception:
        minutes = minutes_default
    minutes = max(1, min(240, minutes))

    try:
        t_kdb0 = time.perf_counter()
        rows, used_ts_col = _kdb_force_orders_last_minutes(
            minutes=minutes,
            host=host,
            port=port,
            table=table,
            ts_col=ts_col,
            username=username,
            password=password,
        )
        kdb_ms = (time.perf_counter() - t_kdb0) * 1000.0
        # Split by side
        buys: List[Dict[str, Any]] = []
        sells: List[Dict[str, Any]] = []
        other: List[Dict[str, Any]] = []
        for r in rows:
            # Normalize symbol column: ensure `sym` exists and appears first.
            rr: Dict[str, Any] = dict(r) if isinstance(r, dict) else {'value': r}
            sym_val = rr.get('sym')
            if sym_val is None:
                sym_val = rr.get('symbol')
            out_row: Dict[str, Any] = {}
            if sym_val is not None:
                out_row['sym'] = sym_val
            for k, v in rr.items():
                if k == 'sym':
                    continue
                out_row[k] = v

            side = str(out_row.get('side') or '').upper()
            if side == 'BUY':
                buys.append(out_row)
            elif side == 'SELL':
                sells.append(out_row)
            else:
                other.append(out_row)

        total_ms = (time.perf_counter() - t_total0) * 1000.0
        return jsonify(
            {
                'minutes': minutes,
                'host': host,
                'port': port,
                'table': table,
                'tsColRequested': ts_col,
                'tsColUsed': used_ts_col,
                'timing': {
                    'kdb_ms': round(float(kdb_ms or 0.0), 2),
                    'total_ms': round(float(total_ms or 0.0), 2),
                },
                'counts': {'buy': len(buys), 'sell': len(sells), 'other': len(other), 'total': len(rows)},
                'buys': buys,
                'sells': sells,
                'other': other,
            }
        )
    except (ConnectionRefusedError, socket.timeout) as e:
        return jsonify({'error': f'KDB not reachable at {host}:{port}: {e}', 'host': host, 'port': port}), 503
    except Exception as e:
        return jsonify({'error': str(e), 'host': host, 'port': port}), 503


@app.route('/api/trader/state', methods=['GET'])
@login_required
def trader_state_get():
    """Return whether the C++ Trader should place/cancel orders."""
    try:
        resp = _trader_rpc('get')
        enabled = resp.get('enabled')
        vol_mult = resp.get('volMult')
        if enabled is None:
            enabled = True
        return jsonify({'enabled': enabled, 'volMult': vol_mult, 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/trader/state', methods=['POST'])
@login_required
def trader_state_set():
    """Set whether the C++ Trader should place/cancel orders."""
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get('enabled', True))
    try:
        resp = _trader_rpc('1' if enabled else '0')
        effective = resp.get('enabled')
        vol_mult = resp.get('volMult')
        if effective is None:
            effective = enabled
        return jsonify({'enabled': effective, 'volMult': vol_mult, 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/trader/volmult', methods=['POST'])
@login_required
def trader_volmult_set():
    """Set the runtime volMult override via TraderControl."""
    data = request.get_json(silent=True) or {}
    try:
        vol_mult = float(data.get('volMult'))
    except Exception:
        return jsonify({'error': 'invalid volMult'}), 400

    try:
        resp = _trader_rpc(f'volmult {vol_mult}')
        return jsonify({'enabled': resp.get('enabled'), 'volMult': resp.get('volMult'), 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/trader/volintercept', methods=['POST'])
@login_required
def trader_volintercept_set():
    """Set the runtime volatility intercept override via TraderControl."""
    data = request.get_json(silent=True) or {}
    try:
        vol_intercept = float(data.get('volIntercept'))
    except Exception:
        return jsonify({'error': 'invalid volIntercept'}), 400

    try:
        resp = _trader_rpc(f'volintercept {vol_intercept}')
        return jsonify({'enabled': resp.get('enabled'), 'volMult': resp.get('volMult'), 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/trader/positionslope', methods=['POST'])
@login_required
def trader_positionslope_set():
    """Set the runtime position slope override via TraderControl."""
    data = request.get_json(silent=True) or {}
    try:
        position_slope = float(data.get('positionSlope'))
    except Exception:
        return jsonify({'error': 'invalid positionSlope'}), 400

    try:
        resp = _trader_rpc(f'positionslope {position_slope}')
        return jsonify({'enabled': resp.get('enabled'), 'volMult': resp.get('volMult'), 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/trader/btcmacdcoeff', methods=['POST'])
@login_required
def trader_btcmacdcoeff_set():
    """Set the runtime BTCUSDT MACD coefficient via TraderControl."""
    data = request.get_json(silent=True) or {}
    try:
        btc_macd_coeff = float(data.get('btcMacdCoeff'))
    except Exception:
        return jsonify({'error': 'invalid btcMacdCoeff'}), 400

    try:
        resp = _trader_rpc(f'macdcoeff {btc_macd_coeff}')
        return jsonify({'enabled': resp.get('enabled'), 'volMult': resp.get('volMult'), 'volIntercept': resp.get('volIntercept'), 'positionSlope': resp.get('positionSlope'), 'btcMacdCoeff': resp.get('btcMacdCoeff'), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT, 'raw': resp.get('raw', '')})
    except Exception as e:
        return jsonify({'error': str(e), 'host': TRADER_CTRL_HOST, 'port': TRADER_CTRL_PORT}), 503


@app.route('/api/err-files', methods=['GET'])
@login_required
def err_files_list():
    """Return the current err file and a list of candidate err/log files."""
    return jsonify({'current': get_err_path(), 'candidates': list_err_candidates()})


@app.route('/api/err-file', methods=['POST'])
@login_required
def err_file_set():
    """Set the active err file for this browser session."""
    data = request.get_json(silent=True) or {}
    raw = str(data.get('path') or '').strip()
    if not raw:
        return jsonify({'error': 'missing path'}), 400

    try:
        p = _resolve_path(raw)
    except Exception:
        return jsonify({'error': 'invalid path'}), 400

    if not p.exists() or not p.is_file():
        return jsonify({'error': f'not a file: {p}'}), 400
    if not _is_allowed_err_file(p):
        return jsonify({'error': f'path not allowed: {p}'}), 403

    session['ERR_FILE'] = str(p)
    return jsonify({'ok': True, 'current': get_err_path()})


@app.route('/api/parse', methods=['POST'])
def parse_log():
    """
    Parse uploaded log file or text input.
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        log_text = data.get('log_text', '')
        log_format = data.get('log_format', 'auto')
        
        if not log_text.strip():
            return jsonify({'error': 'Empty log text'}), 400
        
        # Parse the log
        df = log_to_csv(log_text, '/tmp/temp.csv', log_format=log_format)
        
        if df.empty:
            return jsonify({'error': 'No data parsed from log'}), 400

        df = _rename_dataview_fields(df)
        
        # Generate statistics
        stats = {
            'total_records': len(df),
            'symbols': list(df['symbol'].unique()) if 'symbol' in df.columns else [],
            'date_range': f"{df['time'].min()} to {df['time'].max()}" if 'time' in df.columns else '',
        }
        
        if 'price' in df.columns:
            price_data = pd.to_numeric(df['price'], errors='coerce')
            stats['price_stats'] = {
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'mean': float(price_data.mean()),
                'std': float(price_data.std()),
            }
        
        if 'quantity' in df.columns:
            qty_data = pd.to_numeric(df['quantity'], errors='coerce')
            stats['quantity_stats'] = {
                'total': float(qty_data.sum()),
                'mean': float(qty_data.mean()),
            }
        
        if 'status' in df.columns:
            stats['status_counts'] = df['status'].value_counts().to_dict()
        
        # Generate plot (Bokeh)
        bokeh_item = bokeh_quantity_per_minute_item(df, title='Notional per Minute')
        if bokeh_item is None:
            bokeh_item = bokeh_price_plot_item(df)
        
        # Generate table preview
        table_html = df.head(10).to_html(classes='table table-striped table-sm', index=False)
        
        # Convert DataFrame to CSV for download
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()
        
        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': len(df),
            'column_count': len(df.columns),
        })
    
    except Exception as e:
        return jsonify({
            'error': f'Error processing log: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/position-updates', methods=['POST'])
@login_required
def position_updates_plot():
    """Grep for PositionUpdate entries in the err file and plot position vs time."""
    try:
        err_path = get_err_path()

        try:
            symbol_pair = _symbol_pair_from_request()
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        log_text = _grep_fixed_lines(err_path, '[PositionUpdate')
        if not log_text.strip():
            return jsonify({'error': 'No PositionUpdate entries found in err file'}), 400

        # Example line: [PositionUpdate:ETHUSDC 18:24:28.778] 0.3350
        pattern = re.compile(
            r"\[PositionUpdate:(?P<symbol>\S+)\s+(?P<time>\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?)\]\s+(?P<position>[-+]?\d+(?:\.\d+)?)"
        )
        rows = []
        for line in log_text.splitlines():
            m = pattern.search(line)
            if not m:
                continue
            if symbol_pair is not None and m.group('symbol') != symbol_pair:
                continue
            rows.append({
                'symbol': m.group('symbol'),
                'time': m.group('time'),
                'position': m.group('position'),
            })

        if not rows:
            return jsonify({'error': 'PositionUpdate entries found but none could be parsed'}), 400

        # Keep latest N entries to stay responsive
        rows = rows[-5000:]
        df = pd.DataFrame(rows)
        df['position'] = pd.to_numeric(df['position'], errors='coerce')
        df['time_parsed'] = _parse_time_series(df['time'].astype(str))
        df = df.dropna(subset=['position', 'time_parsed'])
        if df.empty:
            return jsonify({'error': 'No valid PositionUpdate entries after parsing'}), 400

        stats = {
            'total_records': int(len(df)),
            'symbols': list(df['symbol'].astype(str).unique()) if 'symbol' in df.columns else [],
        }
        pos = df['position']
        stats['position_stats'] = {
            'min': float(pos.min()),
            'max': float(pos.max()),
            'mean': float(pos.mean()),
            'last': float(pos.iloc[-1]),
        }

        bokeh_item = bokeh_position_plot_item(df, title='Position vs Time')

        table_html = df.head(10).to_html(classes='table table-striped table-sm', index=False)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()

        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': len(df),
            'column_count': len(df.columns),
            'plot_title': 'Position vs Time',
        })
    except Exception as e:
        return jsonify({
            'error': f'Error generating position updates plot: {str(e)}',
            'traceback': traceback.format_exc(),
        }), 500


@app.route('/api/upload', methods=['POST'])
def upload_file():
    """
    Handle file upload.
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Read file content
        try:
            log_text = file.read().decode('utf-8')
        except UnicodeDecodeError:
            return jsonify({'error': 'File must be UTF-8 encoded text'}), 400
        
        log_format = request.form.get('log_format', 'auto')
        
        # Parse the log
        df = log_to_csv(log_text, '/tmp/temp.csv', log_format=log_format)
        
        if df.empty:
            return jsonify({'error': 'No data parsed from file'}), 400

        df = _rename_dataview_fields(df)
        
        # Generate statistics
        stats = {
            'total_records': len(df),
            'symbols': list(df['symbol'].unique()) if 'symbol' in df.columns else [],
            'date_range': f"{df['time'].min()} to {df['time'].max()}" if 'time' in df.columns else '',
        }
        
        if 'price' in df.columns:
            price_data = pd.to_numeric(df['price'], errors='coerce')
            stats['price_stats'] = {
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'mean': float(price_data.mean()),
                'std': float(price_data.std()),
            }
        
        if 'quantity' in df.columns:
            qty_data = pd.to_numeric(df['quantity'], errors='coerce')
            stats['quantity_stats'] = {
                'total': float(qty_data.sum()),
                'mean': float(qty_data.mean()),
            }
        
        if 'status' in df.columns:
            stats['status_counts'] = df['status'].value_counts().to_dict()
        
        # Generate plot (Bokeh)
        bokeh_item = bokeh_quantity_per_minute_item(df, title='Notional per Minute')
        if bokeh_item is None:
            bokeh_item = bokeh_price_plot_item(df)
        
        # Generate table preview
        table_html = df.head(10).to_html(classes='table table-striped table-sm', index=False)
        
        # Convert DataFrame to CSV for download
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()
        
        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': len(df),
            'column_count': len(df.columns),
        })
    
    except Exception as e:
        return jsonify({
            'error': f'Error uploading file: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/refresh', methods=['POST'])
def refresh_from_err():
    """
    Grep the engine `err` file for OrderUpdate entries, parse and return plot/table.
    """
    try:
        err_path = get_err_path()

        try:
            symbol_pair = _symbol_pair_from_request()
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        log_text = _grep_fixed_lines(err_path, '[OrdUpdate')
        if symbol_pair is not None and log_text.strip():
            prefix = f"[OrdUpdate:{symbol_pair}"
            log_text = '\n'.join([ln for ln in log_text.splitlines() if prefix in ln])

        if not log_text.strip():
            return jsonify({'error': 'No OrderUpdate entries found in err file'}), 400

        df = log_to_csv(log_text, '/tmp/temp.csv', log_format='ordupdate')
        if df.empty:
            return jsonify({'error': 'No data parsed from err file'}), 400

        df = _rename_dataview_fields(df)

        # Generate stats
        stats = {
            'total_records': len(df),
            'symbols': list(df['symbol'].unique()) if 'symbol' in df.columns else [],
            'date_range': f"{df['time'].min()} to {df['time'].max()}" if 'time' in df.columns else '',
        }
        if 'price' in df.columns:
            price_data = pd.to_numeric(df['price'], errors='coerce')
            stats['price_stats'] = {
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'mean': float(price_data.mean()),
                'std': float(price_data.std()),
            }

        bokeh_item = bokeh_quantity_per_minute_item(df, title='Notional per Minute')
        if bokeh_item is None:
            bokeh_item = bokeh_price_plot_item(df)

        table_html = df.head(10).to_html(classes='table table-striped table-sm', index=False)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()

        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': len(df),
            'column_count': len(df.columns),
        })

    except RuntimeError as e:
        return jsonify({'error': f'Grep failed: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Error refreshing from err: {str(e)}', 'traceback': traceback.format_exc()}), 500


@app.route('/api/filled-orders', methods=['POST'])
@login_required
def filled_orders_plot():
    """
    Grep for OrdUpdate entries with FILLED or PARTIALLY_FILLED status and plot prices.
    """
    try:
        err_path = get_err_path()

        try:
            symbol_pair = _symbol_pair_from_request()
        except ValueError as e:
            return jsonify({'error': str(e)}), 400
        
        # Grep all OrdUpdate entries, then filter in Python.
        log_text = _grep_fixed_lines(err_path, '[OrdUpdate')
        if log_text.strip():
            log_text = '\n'.join([ln for ln in log_text.splitlines() if 'FILL' in ln.upper()])
        if symbol_pair is not None and log_text.strip():
            prefix = f"[OrdUpdate:{symbol_pair}"
            log_text = '\n'.join([ln for ln in log_text.splitlines() if prefix in ln])

        df = pd.DataFrame()
        if log_text.strip():
            # Fix: join lines that are split (embedded newlines in log)
            log_lines = log_text.splitlines()
            joined_lines = []
            buffer = ''
            for line in log_lines:
                if line.startswith('[OrdUpdate:') and buffer:
                    joined_lines.append(buffer)
                    buffer = line
                else:
                    buffer += line
            if buffer:
                joined_lines.append(buffer)
            joined_log_text = '\n'.join(joined_lines)

            # Use robust parser from grep_to_pandas
            from grep_to_pandas import parse_order_update_log
            _, rows = parse_order_update_log(joined_log_text)

            # Filter for FILLED or PARTIALLY_FILLED
            fill_rows = []
            for row in (rows or []):
                status = str(row.get('status', '')).upper().strip()
                if 'FILL' in status:
                    # Normalize side value
                    if 'side' in row and row['side'] is not None:
                        row['side'] = str(row['side']).upper().strip()
                    fill_rows.append(row)

            if fill_rows:
                # Take the last 1000 fill messages
                fill_rows = fill_rows[-1000:]
                df = _rename_dataview_fields(pd.DataFrame(fill_rows))

        if df is not None and not df.empty:
            # Convert price and qty columns to numeric, coerce errors
            for col in ['price', 'qty', 'quantity', 'lastPx', 'lastQty']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Parse time to datetime, fallback to alternative format if needed
            try:
                df['time_parsed'] = pd.to_datetime(df['time'], format='%H:%M:%S.%f')
            except Exception:
                try:
                    df['time_parsed'] = pd.to_datetime(df['time'], format='%H:%M:%S')
                except Exception:
                    df['time_parsed'] = pd.NaT
        
        price_col, qty_col = _select_fill_price_qty_cols(df)

        stats = {
            'total_records': len(df) if df is not None else 0,
            'fills_found': int(len(df)) if df is not None else 0,
            'symbols': list(df['symbol'].unique()) if (df is not None and 'symbol' in df.columns) else [],
        }
        if price_col and price_col in df.columns:
            price_data = pd.to_numeric(df[price_col], errors='coerce')
            stats['price_stats'] = {
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'mean': float(price_data.mean()),
                'std': float(price_data.std()),
            }
        if qty_col and qty_col in df.columns:
            qty_data = pd.to_numeric(df[qty_col], errors='coerce')
            stats['quantity_stats'] = {
                'total': float(qty_data.sum()),
                'mean': float(qty_data.mean()),
            }
        if 'status' in df.columns:
            stats['status_counts'] = df['status'].value_counts().to_dict()

        stats['avg_fill_price_by_side'] = _fill_avg_price_by_side(df, price_col=price_col, qty_col=qty_col)

        # Build plot:
        # - If fills exist, show fills and overlay MyBBO/EXBBO.
        # - If no fills exist, still show MyBBO/EXBBO (if present).
        fills_fig = None
        if df is not None and not df.empty:
            fills_fig = bokeh_price_plot_figure(
                df,
                price_col=price_col or 'price',
                title='Filled Orders: Price vs Time (with MyBBO/EXBBO overlay)',
            )

        mybbo_df = _extract_mybbo_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        exbbo_df = _extract_exbbo_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        latency_df = _extract_order_latency_from_err(err_path, limit=5000, symbol_pair=symbol_pair)

        target_symbol = None
        if df is not None and not df.empty and 'symbol' in df.columns:
            syms = df['symbol'].astype(str).unique().tolist()
            if len(syms) == 1:
                target_symbol = syms[0]

        if target_symbol is not None:
            if mybbo_df is not None and not mybbo_df.empty and 'symbol' in mybbo_df.columns:
                sym_df = mybbo_df[mybbo_df['symbol'].astype(str) == target_symbol]
                if not sym_df.empty:
                    mybbo_df = sym_df
            if exbbo_df is not None and not exbbo_df.empty and 'symbol' in exbbo_df.columns:
                sym_df = exbbo_df[exbbo_df['symbol'].astype(str) == target_symbol]
                if not sym_df.empty:
                    exbbo_df = sym_df

        if fills_fig is None and ((mybbo_df is not None and not mybbo_df.empty) or (exbbo_df is not None and not exbbo_df.empty)):
            fills_fig = figure(
                title='MyBBO/EXBBO: Bid and Ask Prices',
                x_axis_type='datetime',
                height=380,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            fills_fig.xaxis.axis_label = 'Time'
            fills_fig.yaxis.axis_label = 'Price'

        if fills_fig is not None and mybbo_df is not None and not mybbo_df.empty:
            fills_fig.step(
                mybbo_df['time_parsed'],
                mybbo_df['bid_price'],
                mode='after',
                line_width=2,
                color='#1f77b4',
                alpha=0.85,
                legend_label='MyBBO Bid',
            )
            fills_fig.step(
                mybbo_df['time_parsed'],
                mybbo_df['ask_price'],
                mode='after',
                line_width=2,
                color='#ff7f0e',
                alpha=0.85,
                legend_label='MyBBO Ask',
            )
            fills_fig.legend.click_policy = 'hide'

        if fills_fig is not None and exbbo_df is not None and not exbbo_df.empty:
            fills_fig.step(
                exbbo_df['time_parsed'],
                exbbo_df['bid_price'],
                mode='after',
                line_width=2,
                color='#9467bd',
                alpha=0.75,
                legend_label='EXBBO Bid',
            )
            fills_fig.step(
                exbbo_df['time_parsed'],
                exbbo_df['ask_price'],
                mode='after',
                line_width=2,
                color='#8c564b',
                alpha=0.75,
                legend_label='EXBBO Ask',
            )
            fills_fig.legend.click_policy = 'hide'

        latency_df = _extract_order_latency_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            # Share the same time axis range as the fills/BBO plot so zoom/pan/ticks align.
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                x_range=(fills_fig.x_range if fills_fig is not None else None),
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            if fills_fig is not None and getattr(fills_fig, 'xaxis', None):
                # Also share ticker/formatter to make labels identical.
                latency_fig.xaxis.ticker = fills_fig.xaxis[0].ticker
                latency_fig.xaxis.formatter = fills_fig.xaxis[0].formatter
                latency_fig.xaxis.major_label_orientation = fills_fig.xaxis[0].major_label_orientation
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        bokeh_item = json_item(fills_fig) if fills_fig is not None else None

        table_html = df.head(10).to_html(classes='table table-striped table-sm', index=False) if (df is not None and not df.empty) else '<div class="text-muted">No fills found. Showing MyBBO/EXBBO if available.</div>'

        csv_buffer = io.StringIO()
        if df is not None and not df.empty:
            df.to_csv(csv_buffer, index=False)
        else:
            csv_buffer.write('')
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()

        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': int(len(df)) if df is not None else 0,
            'column_count': int(len(df.columns)) if (df is not None and hasattr(df, 'columns')) else 0,
        })
    
    except Exception as e:
        return jsonify({
            'error': f'Error generating filled orders plot: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/fills-and-position', methods=['POST'])
@login_required
def fills_and_position_plot():
    """Generate a combined plot: Filled Orders (price vs time) + PositionUpdate (position vs time)."""
    try:
        err_path = get_err_path()

        try:
            symbol_pair = _symbol_pair_from_request()
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        # --- Filled orders (optional) ---
        fills_text = _grep_fixed_lines(err_path, '[OrdUpdate')
        if fills_text.strip():
            fills_text = '\n'.join([ln for ln in fills_text.splitlines() if 'FILL' in ln.upper()])
        if symbol_pair is not None and fills_text.strip():
            prefix = f"[OrdUpdate:{symbol_pair}"
            fills_text = '\n'.join([ln for ln in fills_text.splitlines() if prefix in ln])

        fills_df = pd.DataFrame()
        if fills_text.strip():
            log_lines = fills_text.splitlines()
            joined_lines = []
            buffer = ''
            for line in log_lines:
                if line.startswith('[OrdUpdate:') and buffer:
                    joined_lines.append(buffer)
                    buffer = line
                else:
                    buffer += line
            if buffer:
                joined_lines.append(buffer)
            joined_fills_text = '\n'.join(joined_lines)

            _, rows = parse_order_update_log(joined_fills_text)

            fill_rows = []
            for row in (rows or []):
                status = str(row.get('status', '')).upper().strip()
                if 'FILL' in status:
                    if 'side' in row and row['side'] is not None:
                        row['side'] = str(row['side']).upper().strip()
                    fill_rows.append(row)

            if fill_rows:
                fill_rows = fill_rows[-1000:]
                fills_df = _rename_dataview_fields(pd.DataFrame(fill_rows))
                for col in ['price', 'qty', 'quantity', 'lastPx', 'lastQty']:
                    if col in fills_df.columns:
                        fills_df[col] = pd.to_numeric(fills_df[col], errors='coerce')

        # --- Position updates (optional) ---
        pos_text = _grep_fixed_lines(err_path, '[PositionUpdate')

        pos_df = pd.DataFrame(columns=['symbol', 'time', 'position'])
        if pos_text.strip():
            pattern = re.compile(
                r"\[PositionUpdate:(?P<symbol>\S+)\s+(?P<time>\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?)\]\s+(?P<position>[-+]?\d+(?:\.\d+)?)"
            )
            pos_rows = []
            for line in pos_text.splitlines():
                m = pattern.search(line)
                if not m:
                    continue
                if symbol_pair is not None and m.group('symbol') != symbol_pair:
                    continue
                pos_rows.append({
                    'symbol': m.group('symbol'),
                    'time': m.group('time'),
                    'position': m.group('position'),
                })

            if pos_rows:
                pos_rows = pos_rows[-5000:]
                pos_df = pd.DataFrame(pos_rows)
                pos_df['position'] = pd.to_numeric(pos_df['position'], errors='coerce')

        # --- Combined bokeh layout ---
        fill_price_col, fill_qty_col = _select_fill_price_qty_cols(fills_df)

        # Build the top figure:
        # - If fills exist, plot fills (with quantity-scaled markers) then overlay MyBBO/EXBBO.
        # - If no fills exist, still create a figure and plot MyBBO/EXBBO.
        fills_fig = None
        if fills_df is not None and not fills_df.empty:
            fills_fig = bokeh_price_plot_figure(
                fills_df,
                price_col=fill_price_col or 'price',
                title='Filled Orders: Price vs Time (with MyBBO/EXBBO overlay)',
            )

        mybbo_df = _extract_mybbo_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        exbbo_df = _extract_exbbo_from_err(err_path, limit=5000, symbol_pair=symbol_pair)

        target_symbol = None
        if fills_df is not None and not fills_df.empty and 'symbol' in fills_df.columns:
            fill_symbols = fills_df['symbol'].astype(str).unique().tolist()
            if len(fill_symbols) == 1:
                target_symbol = fill_symbols[0]
        elif 'symbol' in pos_df.columns:
            pos_symbols = pos_df['symbol'].astype(str).unique().tolist()
            if len(pos_symbols) == 1:
                target_symbol = pos_symbols[0]

        if target_symbol is not None:
            if mybbo_df is not None and not mybbo_df.empty and 'symbol' in mybbo_df.columns:
                sym_df = mybbo_df[mybbo_df['symbol'].astype(str) == target_symbol]
                if not sym_df.empty:
                    mybbo_df = sym_df
            if exbbo_df is not None and not exbbo_df.empty and 'symbol' in exbbo_df.columns:
                sym_df = exbbo_df[exbbo_df['symbol'].astype(str) == target_symbol]
                if not sym_df.empty:
                    exbbo_df = sym_df

        if fills_fig is None and ((mybbo_df is not None and not mybbo_df.empty) or (exbbo_df is not None and not exbbo_df.empty)):
            fills_fig = figure(
                title='MyBBO/EXBBO: Bid and Ask Prices',
                x_axis_type='datetime',
                height=380,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            fills_fig.xaxis.axis_label = 'Time'
            fills_fig.yaxis.axis_label = 'Price'

        if fills_fig is not None and mybbo_df is not None and not mybbo_df.empty:
            fills_fig.step(
                mybbo_df['time_parsed'],
                mybbo_df['bid_price'],
                mode='after',
                line_width=2,
                color='#1f77b4',
                alpha=0.85,
                legend_label='MyBBO Bid',
            )
            fills_fig.step(
                mybbo_df['time_parsed'],
                mybbo_df['ask_price'],
                mode='after',
                line_width=2,
                color='#ff7f0e',
                alpha=0.85,
                legend_label='MyBBO Ask',
            )
            fills_fig.legend.click_policy = 'hide'

        if fills_fig is not None and exbbo_df is not None and not exbbo_df.empty:
            fills_fig.step(
                exbbo_df['time_parsed'],
                exbbo_df['bid_price'],
                mode='after',
                line_width=2,
                color='#9467bd',
                alpha=0.75,
                legend_label='EXBBO Bid',
            )
            fills_fig.step(
                exbbo_df['time_parsed'],
                exbbo_df['ask_price'],
                mode='after',
                line_width=2,
                color='#8c564b',
                alpha=0.75,
                legend_label='EXBBO Ask',
            )
            fills_fig.legend.click_policy = 'hide'

        latency_df = _extract_order_latency_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        latency_fig = None
        if latency_df is not None and not latency_df.empty:
            latency_fig = figure(
                title='Order Ack Latency: PENDING_NEW -> NEW',
                x_axis_type='datetime',
                height=280,
                sizing_mode='stretch_width',
                tools='pan,wheel_zoom,box_zoom,reset,save',
            )
            latency_fig.xaxis.axis_label = 'Time'
            latency_fig.yaxis.axis_label = 'Latency (ms)'
            latency_fig.circle(
                latency_df['pending_new_time_parsed'],
                latency_df['latency_ms'],
                size=4,
                color='#2ca02c',
                alpha=0.7,
                legend_label='Latency (ms)',
            )
            latency_fig.legend.click_policy = 'hide'

        pos_fig = bokeh_position_plot_figure(pos_df, title='Position vs Time') if (pos_df is not None and not pos_df.empty) else None
        if fills_fig is None and pos_fig is None and latency_fig is None:
            return jsonify({'error': 'No data available to plot'}), 400
        figs = [f for f in [fills_fig, latency_fig, pos_fig] if f is not None]
        bokeh_item = json_item(column(*figs, sizing_mode='stretch_width'))

        # --- Stats ---
        stats = {
            'total_records': int(len(fills_df) + len(pos_df) + (len(latency_df) if latency_df is not None else 0)),
            'symbols': sorted(set(
                (fills_df['symbol'].astype(str).unique().tolist() if 'symbol' in fills_df.columns else [])
                + (pos_df['symbol'].astype(str).unique().tolist() if 'symbol' in pos_df.columns else [])
            )),
        }

        if fill_price_col and fill_price_col in fills_df.columns:
            price_data = pd.to_numeric(fills_df[fill_price_col], errors='coerce')
            stats['price_stats'] = {
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'mean': float(price_data.mean()),
                'std': float(price_data.std()),
            }
        if fill_qty_col and fill_qty_col in fills_df.columns:
            qty_data = pd.to_numeric(fills_df[fill_qty_col], errors='coerce')
            stats['quantity_stats'] = {
                'total': float(qty_data.sum()),
                'mean': float(qty_data.mean()),
            }
        if 'status' in fills_df.columns:
            stats['status_counts'] = fills_df['status'].value_counts().to_dict()

        stats['avg_fill_price_by_side'] = _fill_avg_price_by_side(
            fills_df,
            price_col=fill_price_col,
            qty_col=fill_qty_col,
        )

        if 'position' in pos_df.columns:
            pos_series = pd.to_numeric(pos_df['position'], errors='coerce').dropna()
            if not pos_series.empty:
                stats['position_stats'] = {
                    'min': float(pos_series.min()),
                    'max': float(pos_series.max()),
                    'mean': float(pos_series.mean()),
                    'last': float(pos_series.iloc[-1]),
                }
        if latency_df is not None and not latency_df.empty:
            latency_series = pd.to_numeric(latency_df['latency_ms'], errors='coerce').dropna()
            if not latency_series.empty:
                stats['latency_ms_stats'] = {
                    'min': float(latency_series.min()),
                    'max': float(latency_series.max()),
                    'mean': float(latency_series.mean()),
                    'p50': float(latency_series.quantile(0.5)),
                    'p95': float(latency_series.quantile(0.95)),
                }

        # --- Table (preview both) ---
        fills_preview = fills_df.head(10)
        pos_preview = pos_df.head(10)
        latency_preview = pd.DataFrame()
        if latency_df is not None and not latency_df.empty:
            cols = [c for c in ['order_id', 'pending_new_time', 'new_time', 'latency_ms'] if c in latency_df.columns]
            latency_preview = latency_df[cols].head(10).copy() if cols else latency_df.head(10).copy()
        table_html = (
            '<div><h6>Filled Orders (preview)</h6></div>'
            + fills_preview.to_html(classes='table table-striped table-sm', index=False)
            + '<div class="mt-3"><h6>Order Ack Latency (preview)</h6></div>'
            + (latency_preview.to_html(classes='table table-striped table-sm', index=False) if (latency_preview is not None and not latency_preview.empty)
               else '<div class="text-muted">No pending_new → new latency data found.</div>')
            + '<div class="mt-3"><h6>Position Updates (preview)</h6></div>'
            + pos_preview.to_html(classes='table table-striped table-sm', index=False)
        )

        # --- CSV (merge into one) ---
        merged = pd.DataFrame({
            'record_type': [],
            'symbol': [],
            'time': [],
            'price': [],
            'qty': [],
            'side': [],
            'status': [],
            'position': [],
        })
        # Normalize fills
        fills_norm = pd.DataFrame({
            'record_type': ['FILL'] * len(fills_df),
            'symbol': fills_df['symbol'] if 'symbol' in fills_df.columns else [''] * len(fills_df),
            'time': fills_df['time'] if 'time' in fills_df.columns else [''] * len(fills_df),
            'price': fills_df[fill_price_col] if fill_price_col and fill_price_col in fills_df.columns else (fills_df['price'] if 'price' in fills_df.columns else [None] * len(fills_df)),
            'qty': fills_df[fill_qty_col] if fill_qty_col and fill_qty_col in fills_df.columns else [None] * len(fills_df),
            'side': fills_df['side'] if 'side' in fills_df.columns else [''] * len(fills_df),
            'status': fills_df['status'] if 'status' in fills_df.columns else [''] * len(fills_df),
            'position': [None] * len(fills_df),
        })
        pos_norm = pd.DataFrame({
            'record_type': ['POSITION'] * len(pos_df),
            'symbol': pos_df['symbol'] if 'symbol' in pos_df.columns else [''] * len(pos_df),
            'time': pos_df['time'] if 'time' in pos_df.columns else [''] * len(pos_df),
            'price': [None] * len(pos_df),
            'qty': [None] * len(pos_df),
            'side': [''] * len(pos_df),
            'status': [''] * len(pos_df),
            'position': pos_df['position'] if 'position' in pos_df.columns else [None] * len(pos_df),
        })
        merged = pd.concat([fills_norm, pos_norm], ignore_index=True)

        csv_buffer = io.StringIO()
        merged.to_csv(csv_buffer, index=False)
        csv_data = base64.b64encode(csv_buffer.getvalue().encode()).decode()

        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'csv_data': csv_data,
            'row_count': int(len(merged)),
            'column_count': int(len(merged.columns)),
            'plot_title': 'Filled Orders + Position Updates',
        })
    except Exception as e:
        return jsonify({
            'error': f'Error generating combined fills/position plot: {str(e)}',
            'traceback': traceback.format_exc(),
        }), 500

@app.route('/api/mybbo', methods=['POST'])
@login_required
def mybbo_plot():
    """
    Extract MyBBO data from err file and generate bid/ask price plot.
    """
    try:
        err_path = get_err_path()

        try:
            symbol_pair = _symbol_pair_from_request()
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

        df = _extract_mybbo_from_err(err_path, limit=5000, symbol_pair=symbol_pair)
        if df is None or df.empty:
            if symbol_pair is None:
                return jsonify({'error': 'No MyBBO data found in err file'}), 400
            return jsonify({'error': f'No MyBBO data found for {symbol_pair}'}), 400

        if 'spread' not in df.columns:
            df['spread'] = df['ask_price'] - df['bid_price']
        if 'mid_price' not in df.columns:
            df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2

        stats = {
            'total_records': int(len(df)),
            'bid_price_stats': {
                'min': float(df['bid_price'].min()),
                'max': float(df['bid_price'].max()),
                'mean': float(df['bid_price'].mean()),
                'std': float(df['bid_price'].std()),
            },
            'ask_price_stats': {
                'min': float(df['ask_price'].min()),
                'max': float(df['ask_price'].max()),
                'mean': float(df['ask_price'].mean()),
                'std': float(df['ask_price'].std()),
            },
            'spread_stats': {
                'min': float(df['spread'].min()),
                'max': float(df['spread'].max()),
                'mean': float(df['spread'].mean()),
            },
            'time_range': f"{df['time'].min()} to {df['time'].max()}",
            'symbols': list(df['symbol'].astype(str).unique()) if 'symbol' in df.columns else [],
            'symbol_pair': symbol_pair,
        }
        
        bokeh_item = bokeh_mybbo_plot_item(df)
        
        # Generate table preview
        table_cols = ['time', 'bid_price', 'ask_price', 'spread']
        if 'symbol' in df.columns:
            table_cols = ['symbol'] + table_cols
        table_df = df[table_cols].head(20)
        table_html = table_df.to_html(classes='table table-striped table-sm', index=False, float_format=lambda x: f'{x:.6f}')
        
        return _bokeh_json_response({
            'success': True,
            'stats': stats,
            'bokeh': bokeh_item,
            'plot': '',
            'table': table_html,
            'row_count': len(df),
            'column_count': int(len(df.columns)),
        })
    
    except Exception as e:
        return jsonify({
            'error': f'Error generating MyBBO plot: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/download-csv', methods=['POST'])
def download_csv():
    """
    Download parsed data as CSV.
    """
    try:
        data = request.get_json()
        csv_data = data.get('csv_data', '')
        
        if not csv_data:
            return jsonify({'error': 'No CSV data'}), 400
        
        csv_bytes = base64.b64decode(csv_data)
        return send_file(
            io.BytesIO(csv_bytes),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Order Analysis Dashboard')
    parser.add_argument(
        '--err-file',
        dest='err_file',
        default=None,
        help='Path to engine err log file (overrides MUSE2_ERR_FILE).',
    )
    parser.add_argument(
        '--port',
        dest='port',
        type=int,
        default=None,
        help='Port to bind (overrides DASHBOARD_PORT).',
    )
    args = parser.parse_args()

    if args.err_file is not None and str(args.err_file).strip():
        app.config['ERR_FILE'] = str(args.err_file).strip()

    print("=" * 60)
    print("Order Analysis Dashboard")
    print("=" * 60)
    print("\nStarting Flask server...")
    port = args.port if args.port is not None else int(os.environ.get('DASHBOARD_PORT', '5050'))
    print(f"Using err file: {get_err_path()}")
    print(f"Access the dashboard at: http://localhost:{port}")
    print("\nAvailable endpoints:")
    print("  GET  /               - Main dashboard")
    print("  POST /api/parse      - Parse log text")
    print("  POST /api/upload     - Upload log file")
    print("  POST /api/download-csv - Download parsed CSV")
    print("  GET  /health         - Health check")
    print("\nPress Ctrl+C to stop the server\n")
    
    app.run(host='0.0.0.0', port=port, debug=True)

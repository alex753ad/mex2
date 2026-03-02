"""MEXC Density Scanner v4.2 - fixed nav, autoscan, colors"""
import io, time, zipfile, math
from datetime import datetime
from collections import Counter
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from mexc_client import MexcClientSync
from analyzer import analyze_order_book
from history import DensityTracker

st.set_page_config(page_title="MEXC Scanner", page_icon="?", layout="wide", initial_sidebar_state="expanded")

# -- helpers --
def sf(v, d=0.0):
    if v is None or v == "": return d
    try: return float(v)
    except: return d
def si(v, d=0):
    try: return int(sf(v, d))
    except: return d
def parse_book(raw):
    out = []
    if not raw or not isinstance(raw, list): return out
    for e in raw:
        if not isinstance(e, (list, tuple)) or len(e) < 2: continue
        p, q = sf(e[0]), sf(e[1])
        if p > 0 and q > 0: out.append((p, q))
    return out
def extract_tc(td):
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): return 0
    for k in ("count","tradeCount","trades","txcnt"):
        v = td.get(k)
        if v and v != "" and v != 0 and v != "0":
            r = si(v)
            if r > 0: return r
    return 0
def parse_klines(raw):
    if not raw or not isinstance(raw, list): return pd.DataFrame()
    rows = []
    for k in raw:
        if not isinstance(k, (list, tuple)) or len(k) < 6: continue
        rows.append({"open_time": sf(k[0]), "open": sf(k[1]), "high": sf(k[2]),
            "low": sf(k[3]), "close": sf(k[4]), "volume": sf(k[5]),
            "close_time": sf(k[6]) if len(k)>6 else 0,
            "quote_volume": sf(k[7]) if len(k)>7 else 0,
            "trades": si(k[8]) if len(k)>8 else 0})
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows); df["time"] = pd.to_datetime(df["open_time"], unit="ms"); return df
def fmt_price(price):
    if price <= 0: return "0"
    if price >= 1000: return f"{price:,.0f}"
    if price >= 1: return f"{price:.2f}"
    if price >= 0.01: return f"{price:.4f}"
    exp = int(math.floor(math.log10(abs(price)))); m = price / (10 ** exp)
    return f"{m:.2f}e{exp}"
def fmt_usd(v):
    if v <= 0: return "-"
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:,.0f}"
def mexc_link(s): return f"https://www.mexc.com/exchange/{s.replace('USDT','_USDT')}"
def make_csv(df): return df.to_csv(index=False).encode("utf-8-sig")
def kline_stats(df, n=None):
    if df is None or df.empty: return {"volume": 0.0, "trades": 0}
    sub = df.tail(n) if n else df
    return {"volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
            "trades": int(sub["trades"].sum()) if "trades" in sub else 0}
def analyze_robots(trades_raw):
    if not trades_raw or not isinstance(trades_raw, list) or len(trades_raw) < 5: return None
    times = sorted([sf(t.get("time", 0)) for t in trades_raw if sf(t.get("time", 0)) > 0], reverse=True)
    if len(times) < 5: return None
    deltas = [abs(times[i] - times[i + 1]) / 1000 for i in range(len(times) - 1)]
    deltas = [d for d in deltas if 0 <= d < 600]
    if not deltas: return None
    volumes = [sf(t.get("price", 0)) * sf(t.get("qty", 0)) for t in trades_raw
               if sf(t.get("price", 0)) > 0 and sf(t.get("qty", 0)) > 0]
    avg_d, min_d, max_d = sum(deltas) / len(deltas), min(deltas), max(deltas)
    mc = Counter([round(d) for d in deltas])
    mode_val, mode_cnt = mc.most_common(1)[0]
    buckets = {}
    for i, d in enumerate(deltas):
        bk = int(d // 5) * 5
        if bk not in buckets: buckets[bk] = {"count": 0, "vols": []}
        buckets[bk]["count"] += 1
        if i < len(volumes): buckets[bk]["vols"].append(volumes[i])
    robots = []
    for bk, info in sorted(buckets.items()):
        pct = info["count"] / len(deltas) * 100
        if pct < 15 or info["count"] < 3: continue
        robots.append({"interval": f"{bk}-{bk + 5}s", "count": info["count"],
                        "pct": round(pct, 1),
                        "avg_vol": sum(info["vols"]) / len(info["vols"]) if info["vols"] else 0})
    return {"avg": avg_d, "min": min_d, "max": max_d, "mode": mode_val,
            "mode_count": mode_cnt, "mode_pct": round(mode_cnt / len(deltas) * 100, 1),
            "is_robot": avg_d < 30 and max_d < 120,
            "avg_vol": sum(volumes) / len(volumes) if volumes else 0, "robots": robots}

# -- charts v2.5 colors --
def build_candlestick(df, symbol, interval, cur_price=None):
    if df is None or df.empty or len(df) < 2: return None
    try:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.03, row_heights=[0.75, 0.25])
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            increasing_line_color="#00c853", decreasing_line_color="#ff1744",
            name="Price"), row=1, col=1)
        colors = ["#00c853" if c >= o else "#ff1744" for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(x=df["time"], y=df["volume"],
                             marker_color=colors, opacity=0.5, name="Vol"), row=2, col=1)
        if cur_price and cur_price > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color="#00d2ff",
                          line_width=1.5, annotation_text=f"  {cur_price:.8g}",
                          annotation_font_color="#00d2ff", annotation_font_size=11,
                          row=1, col=1)
        fig.update_layout(title=f"{symbol} {interval}", template="plotly_dark",
                          height=420, xaxis_rangeslider_visible=False,
                          showlegend=False, margin=dict(l=50, r=20, t=40, b=20))
        return fig
    except: return None

def build_orderbook_chart(bids, asks, cur_price, depth=50):
    try:
        b, a = bids[:depth], asks[:depth]
        if not b and not a: return None
        fig = go.Figure()
        if b:
            fig.add_trace(go.Bar(
                y=[float(p) for p, q in b], x=[float(p * q) for p, q in b],
                orientation="h", name="BID", marker_color="rgba(0,200,83,0.7)",
                hovertemplate="Price: %{y:.8g}<br>$%{x:,.0f}<extra>BID</extra>"))
        if a:
            fig.add_trace(go.Bar(
                y=[float(p) for p, q in a], x=[float(p * q) for p, q in a],
                orientation="h", name="ASK", marker_color="rgba(255,23,68,0.7)",
                hovertemplate="Price: %{y:.8g}<br>$%{x:,.0f}<extra>ASK</extra>"))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color="#00d2ff",
                          line_width=2, annotation_text=f"  <- {float(cur_price):.8g}",
                          annotation_font_color="#00d2ff")
        fig.update_layout(title="OB Orderbook", xaxis_title="Vol ($)",
                          yaxis_title="Price", template="plotly_dark",
                          height=max(500, depth * 12), barmode="relative",
                          showlegend=True, margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except: return None

def build_heatmap(bids, asks, cur_price, depth=30):
    try:
        levels = []
        for p, q in bids[:depth]: levels.append(("BID", float(p), float(p * q)))
        for p, q in asks[:depth]: levels.append(("ASK", float(p), float(p * q)))
        if not levels: return None
        levels.sort(key=lambda x: x[1])
        mx = max(v for _, _, v in levels) or 1.0
        prices, vols, colors, hovers = [], [], [], []
        for side, price, vol in levels:
            i = min(float(vol) / float(mx), 1.0)
            prices.append(price); vols.append(vol)
            if side == "BID":
                colors.append(f"rgba(0,{int(80 + 175 * i)},83,0.85)")
            else:
                colors.append(f"rgba({int(80 + 175 * i)},{int(60 * (1.0 - i))},68,0.85)")
            hovers.append(f"{side}: ${vol:,.0f} @ {price:.8g}")
        fig = go.Figure(go.Bar(y=prices, x=vols, orientation="h",
                                marker_color=colors, hovertext=hovers,
                                hoverinfo="text", showlegend=False))
        if cur_price and float(cur_price) > 0:
            fig.add_hline(y=float(cur_price), line_dash="dot", line_color="#00d2ff",
                          line_width=2, annotation_text=f"  <- {float(cur_price):.8g}",
                          annotation_font_color="#00d2ff")
        fig.update_layout(title="HM Heatmap", template="plotly_dark",
                          height=500, yaxis_title="Price", xaxis_title="Vol (USDT)",
                          margin=dict(l=80, r=20, t=40, b=30))
        return fig
    except: return None

# -- state --
for k, v in [("tracker", DensityTracker()), ("scan_results", []), ("last_scan", 0.0),
             ("total_pairs", 0), ("client", MexcClientSync()), ("detail_symbol", ""),
             ("favorites", set()), ("blacklist", set()), ("cancel_scan", False),
             ("current_page", 0)]:
    if k not in st.session_state: st.session_state[k] = v

def go_detail(sym):
    st.session_state.detail_symbol = sym
    st.session_state.current_page = 1

# -- scan --
def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol; cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread; cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd
    client = st.session_state.client; st.session_state.cancel_scan = False
    progress = st.progress(0, "Loading pairs...")
    try: info = client.get_exchange_info()
    except Exception as e: st.error(f"API: {e}"); progress.empty(); return
    if not info or "symbols" not in info:
        st.error(f"No data: {client.last_error}"); progress.empty(); return
    bl = st.session_state.blacklist; all_sym = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset") != "USDT": continue
            sym = s["symbol"]
            if sym in bl: continue
            st_ = s.get("status", "")
            if (str(st_) in ("1", "ENABLED", "True", "true") or st_ is True or st_ == 1) \
                    and s.get("isSpotTradingAllowed", True):
                all_sym.append(sym)
        except: continue
    if not all_sym:
        for s in info["symbols"]:
            try:
                sym = s.get("symbol", "")
                if s.get("quoteAsset") == "USDT" and sym not in bl: all_sym.append(sym)
            except: continue
    if not all_sym: st.error("0 pairs"); progress.empty(); return
    progress.progress(5)
    try: tickers = client.get_all_tickers_24h()
    except: st.error("Tickers error"); progress.empty(); return
    if not tickers: st.error(str(client.last_error)); progress.empty(); return
    tm = {t["symbol"]: t for t in tickers if "symbol" in t}
    cands = [(sym, tm[sym]) for sym in all_sym
             if sym in tm and min_vol <= sf(tm[sym].get("quoteVolume", 0)) <= max_vol]
    cands.sort(key=lambda x: sf(x[1].get("quoteVolume", 0)), reverse=True)
    if not cands: st.warning("0 in range"); progress.empty(); return
    progress.progress(15, f"Scanning {len(cands)}...")
    results, total = [], len(cands)
    for i, (sym, tk) in enumerate(cands):
        if st.session_state.cancel_scan:
            st.warning(f"Cancelled at {i}/{total}"); break
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, tk)
                if r and r.spread_pct >= min_spread:
                    r.trade_count_24h = extract_tc(tk); results.append(r)
        except: pass
        if (i + 1) % 8 == 0 or i == total - 1:
            progress.progress(15 + int((i + 1) / total * 80), f"{i + 1}/{total} > {len(results)}")
    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]
    progress.progress(96, "Getting trade counts...")
    for r in top[:10]:
        if r.trade_count_24h == 0:
            try:
                tc = extract_tc(client.get_ticker_24h(r.symbol))
                if tc > 0: r.trade_count_24h = tc
            except: pass
    st.session_state.tracker.update(top)
    st.session_state.scan_results = top
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total
    st.session_state.cancel_scan = False
    progress.progress(100, "Done!"); time.sleep(0.2); progress.empty()

# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("## CFG Settings")
    min_vol = st.number_input("Min vol 24h ($)", value=100, min_value=0, step=100,
                              help="Minimum daily trading volume in USDT")
    max_vol = st.number_input("Max vol 24h ($)", value=500_000, min_value=100, step=10000,
                              help="Maximum daily trading volume in USDT")
    min_spread = st.slider("Min spread %", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Wall sensitivity (x)", 2, 50, 5,
                          help="How many times a price level volume must exceed "
                               "the median orderbook level to count as a wall. "
                               "5x means the level is 5 times larger than average")
    min_wall_usd = st.number_input("Min wall $", value=50, min_value=1, step=10)
    top_n = st.slider("Max results", 5, 100, 30)
    st.markdown("---")
    auto_on = st.checkbox("(R) Auto-scan", value=True)
    auto_sec = st.select_slider("Interval (s)", [15, 20, 30, 45, 60, 90], value=30)
    if auto_on:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=auto_sec * 1000, key="ar")
        except ImportError:
            st.caption("(!) pip install streamlit-autorefresh")
    c1s, c2s = st.columns(2)
    scan_btn = c1s.button(">> Scan", use_container_width=True, type="primary")
    if c2s.button("X Stop", use_container_width=True):
        st.session_state.cancel_scan = True
    st.markdown("---")
    with st.expander("(X) Blacklist"):
        bl_inp = st.text_input("Add tokens", placeholder="XYZUSDT,ABCUSDT", key="bl_inp")
        if bl_inp:
            for s in bl_inp.upper().replace(" ", "").split(","):
                if s.endswith("USDT"): st.session_state.blacklist.add(s)
            st.rerun()
        if st.session_state.blacklist:
            st.caption(", ".join(sorted(st.session_state.blacklist)))
            if st.button("Clear"): st.session_state.blacklist = set(); st.rerun()
    with st.expander("* Favorites"):
        fav = st.session_state.favorites
        if fav: st.caption(", ".join(sorted(fav)))
        up = st.file_uploader("Import", type=["csv", "txt"], key="fi", label_visibility="collapsed")
        if up:
            new = {l.strip().upper() for l in up.getvalue().decode("utf-8").replace(",", "\n").split("\n")
                   if l.strip().upper().endswith("USDT") and len(l.strip()) > 4}
            if new: st.session_state.favorites.update(new); st.rerun()
        if fav:
            st.download_button("Export", data="\n".join(sorted(fav)).encode(),
                               file_name="fav.csv", mime="text/csv", use_container_width=True)
    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(f"Scans: {stats['total_scans']} | Movers: {stats['total_mover_events']}")
    if st.button("T Test API", use_container_width=True):
        ok, msg = st.session_state.client.ping()
        st.success(msg) if ok else st.error(msg)

# ==========================================
# AUTO-SCAN TRIGGER
# ==========================================
if scan_btn or (auto_on and time.time() - st.session_state.last_scan > max(auto_sec - 3, 10)):
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n)

# ==========================================
# NAVIGATION - buttons, not radio (radio key breaks on rerun)
# ==========================================
PAGES = ["# Scanner", "(S) Details", "^ Movers"]
cp = st.session_state.current_page
nav_cols = st.columns(len(PAGES))
for i, label in enumerate(PAGES):
    with nav_cols[i]:
        btype = "primary" if cp == i else "secondary"
        if st.button(label, key=f"nav_{i}", use_container_width=True, type=btype):
            st.session_state.current_page = i
            st.rerun()
st.markdown("---")
page = cp

# ==========================================
# PAGE 0: SCANNER
# ==========================================
if page == 0:
    results = st.session_state.scan_results
    tracker = st.session_state.tracker
    if not results:
        st.info("Waiting for first scan..." if auto_on else "Press >> Scan in sidebar")
    else:
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Found", len(results))
        mc2.metric("Checked", st.session_state.total_pairs)
        mc3.metric("Best score", f"{results[0].score}")
        mc4.metric("! Movers", sum(1 for r in results if r.has_movers))

        sort_by = st.selectbox("Sort by",
            ["Score v", "Wall size v", "Distance ^",
             "Lifetime v", "Spread v", "Vol 24h v"],
            key="sort_select")

        rows = []
        for r in results:
            if not r.all_walls: continue
            bw = r.biggest_wall
            tw_list = tracker.get_tracked_walls(r.symbol)
            tw_big = None
            if tw_list:
                for tw in tw_list:
                    if bw and abs(tw.price - bw.price) < bw.price * 0.001:
                        tw_big = tw; break
                if not tw_big: tw_big = tw_list[0]
            lt_s = tw_big.lifetime_sec if tw_big else 0
            lt_str = tw_big.lifetime_str if tw_big else "-"
            bt = max(r.bid_walls, key=lambda w: w.size_usdt) if r.bid_walls else None
            at = max(r.ask_walls, key=lambda w: w.size_usdt) if r.ask_walls else None
            rows.append({
                "Score": r.score, "Pair": r.symbol,
                "Spread%": round(r.spread_pct, 2),
                "Vol 24h": fmt_usd(r.volume_24h_usdt),
                "Trades": r.trade_count_24h if r.trade_count_24h > 0 else "-",
                "BID wall": f"{fmt_usd(bt.size_usdt)} ({bt.multiplier}x)" if bt else "-",
                "BID dist%": f"{bt.distance_pct}%" if bt else "-",
                "ASK wall": f"{fmt_usd(at.size_usdt)} ({at.multiplier}x)" if at else "-",
                "ASK dist%": f"{at.distance_pct}%" if at else "-",
                "Lifetime": lt_str,
                "!": "!" if r.has_movers else "",
                "_s": r.score, "_w": bw.size_usdt if bw else 0,
                "_d": bw.distance_pct if bw else 99,
                "_l": lt_s, "_sp": r.spread_pct, "_v": r.volume_24h_usdt,
            })
        if rows:
            df = pd.DataFrame(rows)
            if "Wall size" in sort_by: df = df.sort_values("_w", ascending=False)
            elif "Distance" in sort_by: df = df.sort_values("_d", ascending=True)
            elif "Lifetime" in sort_by: df = df.sort_values("_l", ascending=False)
            elif "Spread" in sort_by: df = df.sort_values("_sp", ascending=False)
            elif "Vol 24h" in sort_by: df = df.sort_values("_v", ascending=False)
            else: df = df.sort_values("_s", ascending=False)
            df = df.reset_index(drop=True)
            show_cols = ["Score", "Pair", "Spread%", "Vol 24h", "Trades",
                         "BID wall", "BID dist%", "ASK wall", "ASK dist%",
                         "Lifetime", "!"]
            st.dataframe(df[show_cols], hide_index=True,
                         use_container_width=True,
                         height=min(len(df) * 35 + 40, 700))

            st.markdown("##### -> Click pair for detailed analysis")
            syms = df["Pair"].tolist()
            nc = min(10, len(syms))
            btn_cols = st.columns(nc)
            for i, sym in enumerate(syms[:nc]):
                with btn_cols[i]:
                    if st.button(sym, key=f"go_{sym}", use_container_width=True):
                        go_detail(sym); st.rerun()
            if len(syms) > nc:
                sel_c, go_c = st.columns([3, 1])
                with sel_c:
                    chosen = st.selectbox("All pairs", [""] + syms, key="all_pairs")
                with go_c:
                    if chosen and st.button("(S) Go", key="go_chosen"):
                        go_detail(chosen); st.rerun()
            st.download_button("DL CSV", data=make_csv(df[show_cols]),
                               file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                               mime="text/csv")

# ==========================================
# PAGE 1: DETAILS
# ==========================================
elif page == 1:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []

    hdr = st.columns([1, 3, 2, 1, 1])
    with hdr[0]:
        if st.button("<- Back"):
            st.session_state.current_page = 0; st.rerun()
    with hdr[1]:
        idx = 0; ds = st.session_state.detail_symbol
        if ds and ds in sym_list: idx = sym_list.index(ds) + 1
        target = st.selectbox("Pair", [""] + sym_list, index=idx,
                              key="detail_sel", label_visibility="collapsed")
    with hdr[2]:
        manual = st.text_input("Manual", placeholder="XYZUSDT",
                               label_visibility="collapsed")
    symbol = manual.strip().upper() if manual.strip() else target
    with hdr[3]:
        if symbol:
            is_fav = symbol in st.session_state.favorites
            if st.button("*" if is_fav else "o", key="fav_detail"):
                (st.session_state.favorites.discard if is_fav else st.session_state.favorites.add)(symbol)
                st.rerun()
    with hdr[4]:
        if symbol and st.button("(X)", key="bl_detail", help="Add to blacklist"):
            st.session_state.blacklist.add(symbol); st.rerun()

    if not symbol:
        st.info("Select a pair from the dropdown or type manually")
        st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client
    tracker = st.session_state.tracker

    with st.spinner(f"Loading {symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            kl_1m = client.get_klines(symbol, "1m", 100)
            kl_5m = client.get_klines(symbol, "5m", 100)
            kl_1h = client.get_klines(symbol, "60m", 100)
            kl_4h = client.get_klines(symbol, "4h", 100)
            kl_1d = client.get_klines(symbol, "1d", 100)
        except Exception as e:
            st.error(str(e)); st.stop()

    if not book_raw or not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"No orderbook for {symbol}"); st.stop()

    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])
    if not bids or not asks: st.error("Empty book"); st.stop()

    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb + ba) / 2; spread = (ba - bb) / bb * 100
    bdepth = sum(float(p) * float(q) for p, q in bids)
    adepth = sum(float(p) * float(q) for p, q in asks)
    td = ticker_raw
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): td = {}
    tc24 = extract_tc(td); vol24 = sf(td.get("quoteVolume", 0))
    df_1m, df_5m, df_1h, df_4h, df_1d = [parse_klines(x) for x in
                                           [kl_1m, kl_5m, kl_1h, kl_4h, kl_1d]]

    st.markdown(f"### {symbol} * {fmt_price(mid)} * [MEXC ->]({mexc_link(symbol)})")
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Spread", f"{spread:.2f}%")
    m2.metric("Bid $", f"${bdepth:,.0f}")
    m3.metric("Ask $", f"${adepth:,.0f}")
    m4.metric("Trades 24h", f"{tc24:,}" if tc24 else "-")
    m5.metric("Vol 24h", f"${vol24:,.0f}")
    s4h = kline_stats(df_1h, 4)
    m6.metric("Vol 4h", f"${s4h['volume']:,.0f}")

    # -- Walls with lifetime --
    st.markdown("#### W Walls (density levels)")
    tw_list = tracker.get_tracked_walls(symbol)
    if tw_list:
        twr = [{"Side": "+ BID" if tw.side == "BID" else "- ASK",
                "Price": fmt_price(tw.price), "Size": fmt_usd(tw.size_usdt),
                "Mult": f"{tw.multiplier}x", "Dist%": f"{tw.distance_pct}%",
                "Lifetime": tw.lifetime_str, "Scans": tw.seen_count}
               for tw in tw_list]
        st.dataframe(pd.DataFrame(twr), hide_index=True, use_container_width=True)
    else:
        st.caption("Wall tracking requires multiple scans")

    # -- Movers history for this pair --
    sym_movers = tracker.get_symbol_movers(symbol)
    if sym_movers:
        st.markdown("#### ! Mover events (wall relocations)")
        mvr = [{"Time": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                "Side": e.side, "Size": fmt_usd(e.size_usdt),
                "From": fmt_price(e.old_price), "To": fmt_price(e.new_price),
                "Shift": f"{e.shift_pct:+.3f}%",
                "Dir": "+ LONG" if e.direction == "UP" else "- SHORT"}
               for e in reversed(sym_movers[-20:])]
        st.dataframe(pd.DataFrame(mvr), hide_index=True, use_container_width=True)

    # -- Volume & trades by timeframe --
    st.markdown("#### # Volume & trades")
    s5 = kline_stats(df_5m, 1); s15 = kline_stats(df_5m, 3); s60 = kline_stats(df_5m, 12)
    vc = st.columns(5)
    vc[0].metric("5m", f"${s5['volume']:,.0f}", f"{s5['trades']} tr")
    vc[1].metric("15m", f"${s15['volume']:,.0f}", f"{s15['trades']} tr")
    vc[2].metric("1h", f"${s60['volume']:,.0f}", f"{s60['trades']} tr")
    vc[3].metric("4h", f"${s4h['volume']:,.0f}", f"{s4h['trades']} tr")
    vc[4].metric("24h", f"${vol24:,.0f}", f"{tc24:,} tr")

    # -- Robot analysis --
    st.markdown("#### BOT Trading pattern analysis")
    robot = analyze_robots(trades_raw)
    if robot:
        ri = robot
        em = "BOT" if ri["is_robot"] else "USR"
        st.markdown(f"{em} **Intervals:** avg={ri['avg']:.1f}s  "
                    f"min={ri['min']:.1f}s  max={ri['max']:.1f}s")
        st.markdown(f"**Mode:** {ri['mode']}s ({ri['mode_count']}x, "
                    f"{ri['mode_pct']}% of trades) | Avg vol: {fmt_usd(ri['avg_vol'])}")
        if ri["robots"]:
            st.markdown(f"**Detected {len(ri['robots'])} bot(s):**")
            for j, bot in enumerate(ri["robots"]):
                st.markdown(f"  `Bot #{j + 1}`: interval **{bot['interval']}**, "
                            f"{bot['count']} trades ({bot['pct']}%), "
                            f"avg vol {fmt_usd(bot['avg_vol'])}")
        elif ri["is_robot"]:
            st.markdown("BOT **Single bot detected** - stable intervals")
    else:
        st.caption("Not enough trades for analysis")

    # -- Charts --
    st.markdown("#### ^ Charts")
    chart_tabs = st.tabs(["1m", "5m", "1h", "4h", "1d"])
    for tab, dfk, lbl in zip(chart_tabs,
                              [df_1m, df_5m, df_1h, df_4h, df_1d],
                              ["1m", "5m", "1h", "4h", "1d"]):
        with tab:
            f = build_candlestick(dfk, symbol, lbl, mid)
            if f: st.plotly_chart(f, use_container_width=True)
            else: st.warning(f"No data for {lbl}")

    # -- Orderbook + Heatmap --
    st.markdown("#### OB Orderbook / Heatmap")
    dv = st.select_slider("Depth", [20, 30, 50, 100], value=50, key="ob_depth")
    col_ob, col_hm = st.columns(2)
    with col_ob:
        fg = build_orderbook_chart(bids, asks, mid, dv)
        if fg: st.plotly_chart(fg, use_container_width=True)
    with col_hm:
        fh = build_heatmap(bids, asks, mid, 30)
        if fh: st.plotly_chart(fh, use_container_width=True)

    # -- Recent trades --
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### = Recent trades")
        trs = []
        for t in trades_raw[:50]:
            try:
                p = sf(t.get("price", 0)); q = sf(t.get("qty", 0))
                ts = sf(t.get("time", 0))
                trs.append({
                    "Time": pd.to_datetime(ts, unit="ms").strftime("%H:%M:%S") if ts > 0 else "-",
                    "Price": fmt_price(p), "Qty": q, "$": round(p * q, 2),
                    "Side": "+ BUY" if not t.get("isBuyerMaker") else "- SELL"})
            except: continue
        if trs:
            st.dataframe(pd.DataFrame(trs), hide_index=True,
                         use_container_width=True, height=300)

    # -- Export --
    st.markdown("---")
    export_data = {}
    ob_df = pd.DataFrame([{"Side": s, "Price": float(p), "Qty": float(q),
                            "$": round(float(p * q), 4)}
                           for s, data in [("BID", bids), ("ASK", asks)]
                           for p, q in data])
    export_data["orderbook"] = ob_df
    for lbl, kdf in [("1m", df_1m), ("5m", df_5m), ("1h", df_1h),
                      ("4h", df_4h), ("1d", df_1d)]:
        if kdf is not None and not kdf.empty:
            export_data[f"klines_{lbl}"] = kdf
    def make_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for n, d in export_data.items():
                zf.writestr(f"{symbol}_{n}.csv", d.to_csv(index=False))
        buf.seek(0); return buf.getvalue()
    st.download_button(f"ZIP Download {symbol} ZIP", data=make_zip(),
                       file_name=f"{symbol}.zip", mime="application/zip",
                       use_container_width=True)

# ==========================================
# PAGE 2: MOVERS
# ==========================================
elif page == 2:
    tracker = st.session_state.tracker
    st.markdown("### ^ Mover Monitor")
    st.caption("A mover = wall that relocates in the orderbook. Sign of bot/MM activity.")

    tab_log, tab_rank = st.tabs(["= Log", "TOP Ranking"])
    with tab_log:
        movers = tracker.get_active_movers(7200)
        if not movers:
            st.info("No movers detected yet. Run several scans first.")
        else:
            st.success(f"! {len(movers)} movers in last 2h")
            mr = [{"Time": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                   "Pair": e.symbol, "Side": e.side,
                   "Size": fmt_usd(e.size_usdt),
                   "From": fmt_price(e.old_price),
                   "To": fmt_price(e.new_price),
                   "Shift": f"{e.shift_pct:+.3f}%",
                   "Dir": "+ LONG" if e.direction == "UP" else "- SHORT"}
                  for e in reversed(movers)]
            mdf = pd.DataFrame(mr)
            st.dataframe(mdf, hide_index=True, use_container_width=True)
            unique_syms = sorted({e.symbol for e in movers})
            sel_c, go_c = st.columns([3, 1])
            with sel_c:
                chosen_mover = st.selectbox("Go to detail", [""] + unique_syms,
                                            key="mover_select")
            with go_c:
                if chosen_mover and st.button("(S)", key="mover_go"):
                    go_detail(chosen_mover); st.rerun()
            st.download_button("DL CSV", data=make_csv(mdf),
                               file_name="movers.csv", mime="text/csv")

    with tab_rank:
        top_movers = tracker.get_top_movers(20)
        if top_movers:
            for i, (sym, cnt) in enumerate(top_movers):
                rc = st.columns([3, 1, 1])
                rc[0].markdown(f"**{i + 1}. {sym}** - {cnt} events")
                if rc[1].button("(S)", key=f"rank_{sym}"):
                    go_detail(sym); st.rerun()
                if rc[2].button("*", key=f"fav_{sym}"):
                    st.session_state.favorites.add(sym); st.rerun()
            fig = go.Figure(go.Bar(
                x=[x[0] for x in top_movers],
                y=[x[1] for x in top_movers],
                marker_color="#00d2ff"))
            fig.update_layout(template="plotly_dark", height=250,
                              title="Top movers")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Accumulate data by running multiple scans")

st.caption("MEXC Scanner v4.2")

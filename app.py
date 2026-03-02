"""
═══════════════════════════════════════════════════════════
  MEXC Density Scanner v4.0
═══════════════════════════════════════════════════════════
"""
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

st.set_page_config(page_title="MEXC Scanner", page_icon="🔍",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
.block-container{padding-top:.3rem}
.stMetric>div{background:#0d1117;padding:.5rem;border-radius:8px;border:1px solid #1e2d3d}
div[data-testid="stMetricValue"]{font-size:1.2rem}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════
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
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    return df

# ─── Формат цен ───
def fmt_price(price):
    """Научная нотация для мелких: 5.50·10⁻⁵"""
    if price <= 0: return "0"
    if price >= 0.01:
        if price >= 1000: return f"{price:,.0f}"
        if price >= 1: return f"{price:.2f}"
        return f"{price:.4f}"
    exp = int(math.floor(math.log10(abs(price))))
    m = price / (10 ** exp)
    sup = str(exp).replace("-","⁻").replace("0","⁰").replace("1","¹") \
          .replace("2","²").replace("3","³").replace("4","⁴") \
          .replace("5","⁵").replace("6","⁶").replace("7","⁷") \
          .replace("8","⁸").replace("9","⁹")
    return f"{m:.2f}·10{sup}"

def fmt_price_full(price):
    if price <= 0: return "0"
    d = max(2, -int(math.floor(math.log10(abs(price))))+2) if price > 0 else 8
    return f"{price:.{d}f}"

def fmt_usd(v):
    """$1.2K, $250, $15.3K"""
    if v <= 0: return "—"
    if v >= 1000: return f"${v/1000:.1f}K"
    return f"${v:,.0f}"

def plotly_tickfmt(price):
    if price <= 0: return ".8f"
    d = max(2, -int(math.floor(math.log10(abs(price))))+2)
    return f".{d}f"

def mexc_link(s): return f"https://www.mexc.com/exchange/{s.replace('USDT','_USDT')}"
def make_csv(df): return df.to_csv(index=False).encode("utf-8-sig")
def kline_stats(df, n=None):
    if df is None or df.empty: return {"volume":0.0,"trades":0}
    sub = df.tail(n) if n else df
    return {"volume": float(sub["quote_volume"].sum()) if "quote_volume" in sub else 0.0,
            "trades": int(sub["trades"].sum()) if "trades" in sub else 0}

# ─── Робот-анализ ───
def analyze_robots(trades_raw):
    """Кластеризация торговых интервалов для выявления роботов"""
    if not trades_raw or not isinstance(trades_raw, list) or len(trades_raw) < 5:
        return None
    times = sorted([sf(t.get("time",0)) for t in trades_raw if sf(t.get("time",0))>0], reverse=True)
    if len(times) < 5: return None
    deltas = [abs(times[i]-times[i+1])/1000 for i in range(len(times)-1)]
    deltas = [d for d in deltas if 0 <= d < 600]
    if not deltas: return None
    # Объёмы по сделкам
    volumes = []
    for t in trades_raw:
        p, q = sf(t.get("price",0)), sf(t.get("qty",0))
        if p > 0 and q > 0: volumes.append(p*q)
    avg_d = sum(deltas)/len(deltas)
    min_d, max_d = min(deltas), max(deltas)
    # Мода интервалов (округлённая до 1с)
    rounded = [round(d) for d in deltas]
    mode_counter = Counter(rounded)
    mode_val, mode_cnt = mode_counter.most_common(1)[0]
    # Кластеризация: группируем интервалы в бакеты по 5с
    buckets = {}
    for i, d in enumerate(deltas):
        bucket = int(d // 5) * 5
        if bucket not in buckets: buckets[bucket] = {"count":0, "vols":[]}
        buckets[bucket]["count"] += 1
        if i < len(volumes): buckets[bucket]["vols"].append(volumes[i])
    # Детекция роботов: бакет с >20% сделок и низкой дисперсией
    robots = []
    for bk, info in sorted(buckets.items()):
        pct = info["count"] / len(deltas) * 100
        if pct < 15 or info["count"] < 3: continue
        avg_vol = sum(info["vols"])/len(info["vols"]) if info["vols"] else 0
        robots.append({
            "interval": f"{bk}-{bk+5}с",
            "count": info["count"],
            "pct": round(pct, 1),
            "avg_vol": avg_vol,
        })
    is_robot = avg_d < 30 and max_d < 120
    return {
        "avg": avg_d, "min": min_d, "max": max_d,
        "mode": mode_val, "mode_count": mode_cnt, "mode_pct": round(mode_cnt/len(deltas)*100,1),
        "total": len(deltas),
        "avg_vol": sum(volumes)/len(volumes) if volumes else 0,
        "min_vol": min(volumes) if volumes else 0,
        "max_vol": max(volumes) if volumes else 0,
        "is_robot": is_robot,
        "robots": robots,
    }

# ═══════════════════════════════════════════════════
# Session State
# ═══════════════════════════════════════════════════
defaults = {
    "tracker": DensityTracker(), "scan_results": [], "scan_df": pd.DataFrame(),
    "last_scan": 0.0, "total_pairs": 0, "client": MexcClientSync(),
    "detail_symbol": "", "target_page": 0, "favorites": set(),
    "blacklist": set(), "scanning": False, "cancel_scan": False,
}
for k, v in defaults.items():
    if k not in st.session_state: st.session_state[k] = v

# ═══════════════════════════════════════════════════
# ГРАФИКИ
# ═══════════════════════════════════════════════════

def build_candlestick(df, symbol, interval, cur_price=None):
    if df is None or df.empty or len(df)<2: return None
    try:
        med = float(df["close"].median())
        tfmt = plotly_tickfmt(med)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.03, row_heights=[0.75, 0.25],
                            specs=[[{"secondary_y":True}],[{"secondary_y":False}]])
        fig.add_trace(go.Candlestick(
            x=df["time"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color="#00FF7F", increasing_fillcolor="#00FF7F",
            decreasing_line_color="#FF3366", decreasing_fillcolor="#FF3366",
            name="Price"), row=1, col=1, secondary_y=False)
        colors = ["#00FF7F" if c>=o else "#FF3366" for c,o in zip(df["close"],df["open"])]
        fig.add_trace(go.Bar(x=df["time"], y=df["volume"],
                             marker_color=colors, opacity=0.7, name="Vol"), row=2, col=1)
        ref = float(cur_price) if cur_price and cur_price>0 else float(df["close"].iloc[-1])
        if ref > 0:
            hi, lo = float(df["high"].max()), float(df["low"].min())
            fig.add_trace(go.Scatter(
                x=[df["time"].iloc[0], df["time"].iloc[-1]],
                y=[(hi-ref)/ref*100, (lo-ref)/ref*100],
                mode="markers", marker=dict(size=0, opacity=0),
                showlegend=False, hoverinfo="skip"),
                row=1, col=1, secondary_y=True)
            fig.update_yaxes(title_text="%", ticksuffix="%", showgrid=False,
                             zeroline=True, zerolinecolor="rgba(0,210,255,0.5)",
                             row=1, col=1, secondary_y=True)
        if cur_price and cur_price>0:
            fig.add_hline(y=float(cur_price), line_dash="dot",
                          line_color="#00BFFF", line_width=1.5,
                          annotation_text=f"  {fmt_price(float(cur_price))}",
                          annotation_font_color="#00BFFF", row=1, col=1)
        fig.update_yaxes(tickformat=tfmt, exponentformat="none",
                         row=1, col=1, secondary_y=False)
        fig.update_layout(title=f"{symbol} {interval}", template="plotly_dark",
                          height=420, xaxis_rangeslider_visible=False,
                          showlegend=False, margin=dict(l=60,r=60,t=30,b=15),
                          plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.caption(f"Ошибка: {e}"); return None


def build_orderbook_chart(bids, asks, cur_price, depth=50):
    try:
        b, a = bids[:depth], asks[:depth]
        if not b and not a: return None
        levels = []
        for p,q in b: levels.append(("BID", float(p), float(p*q)))
        for p,q in a: levels.append(("ASK", float(p), float(p*q)))
        levels.sort(key=lambda x: x[1])
        prices = [x[1] for x in levels]
        vols = [x[2] for x in levels]
        # Цвета из v2.5 — видимые
        bar_colors = ["rgba(0,200,83,0.75)" if x[0]=="BID"
                      else "rgba(255,23,68,0.75)" for x in levels]
        vol_thr = sorted(vols, reverse=True)[min(9, len(vols)-1)] if vols else 0
        texts = [f"${v:,.0f}" if v>=vol_thr else "" for v in vols]
        step = max(1, len(prices)//20)
        big = {prices[i] for i,v in enumerate(vols) if v>=vol_thr}
        tv = [p for i,p in enumerate(prices) if i%step==0 or p in big]
        tt = [fmt_price(p) for p in tv]
        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h",
            marker_color=bar_colors,
            text=texts, textposition="auto",
            textfont=dict(size=10, color="white"),
            hovertext=[f"{'BID' if x[0]=='BID' else 'ASK'} {fmt_price(x[1])}: ${x[2]:,.0f}" for x in levels],
            hoverinfo="text"))
        if cur_price and float(cur_price)>0:
            mx = max(vols) if vols else 1
            fig.add_trace(go.Scatter(
                x=[0, mx*1.2], y=[float(cur_price)]*2,
                mode="lines+text",
                text=["", f" {fmt_price(float(cur_price))}"],
                textposition="middle right",
                textfont=dict(color="#00BFFF", size=12),
                line=dict(color="#00BFFF", width=2.5, dash="dot"),
                showlegend=False))
        fig.update_layout(title="📖 Стакан", template="plotly_dark",
                          height=max(450, depth*10), xaxis_title="$",
                          yaxis=dict(title="Цена", tickmode="array",
                                     tickvals=tv, ticktext=tt, exponentformat="none"),
                          margin=dict(l=90,r=20,t=35,b=25), plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.error(f"Стакан: {e}"); return None


def build_heatmap(bids, asks, cur_price, depth=30):
    try:
        levels = []
        for p,q in bids[:depth]: levels.append(("BID", float(p), float(p*q)))
        for p,q in asks[:depth]: levels.append(("ASK", float(p), float(p*q)))
        if not levels: return None
        levels.sort(key=lambda x: x[1])
        mx = max(v for _,_,v in levels) or 1.0
        prices, vols, colors, hovers = [], [], [], []
        for side, price, vol in levels:
            i = min(vol/mx, 1.0)
            prices.append(price); vols.append(vol)
            if side=="BID":
                colors.append(f"rgba(0,{int(120+135*i)},{int(50+33*i)},0.8)")
            else:
                colors.append(f"rgba({int(150+105*i)},{int(23*(1-i))},{int(40*(1-i))},0.8)")
            hovers.append(f"{side} {fmt_price(price)}: ${vol:,.0f}")
        vol_thr = sorted(vols, reverse=True)[min(7,len(vols)-1)] if vols else 0
        texts = [f"${v:,.0f}" if v>=vol_thr else "" for v in vols]
        step = max(1, len(prices)//15)
        big = {prices[i] for i,v in enumerate(vols) if v>=vol_thr}
        tv = [p for i,p in enumerate(prices) if i%step==0 or p in big]
        tt = [fmt_price(p) for p in tv]
        fig = go.Figure(go.Bar(
            y=prices, x=vols, orientation="h", marker_color=colors,
            text=texts, textposition="auto", textfont=dict(size=9, color="white"),
            hovertext=hovers, hoverinfo="text", showlegend=False))
        if cur_price and float(cur_price)>0:
            mx_x = max(vols) if vols else 1
            fig.add_trace(go.Scatter(
                x=[0, mx_x*1.2], y=[float(cur_price)]*2,
                mode="lines+text",
                text=["", f" {fmt_price(float(cur_price))}"],
                textposition="middle right",
                textfont=dict(color="#00BFFF", size=12),
                line=dict(color="#00BFFF", width=2.5, dash="dot"),
                showlegend=False))
        fig.update_layout(title="🔥 Хитмап", template="plotly_dark", height=450,
                          xaxis_title="$",
                          yaxis=dict(title="Цена", tickmode="array",
                                     tickvals=tv, ticktext=tt, exponentformat="none"),
                          margin=dict(l=90,r=20,t=35,b=25), plot_bgcolor="#0a0e14")
        return fig
    except Exception as e:
        st.error(f"Хитмап: {e}"); return None


# ═══════════════════════════════════════════════════
# СКАНИРОВАНИЕ с отменой
# ═══════════════════════════════════════════════════
def run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n):
    import config as cfg
    cfg.MIN_DAILY_VOLUME_USDT = min_vol; cfg.MAX_DAILY_VOLUME_USDT = max_vol
    cfg.MIN_SPREAD_PCT = min_spread; cfg.WALL_MULTIPLIER = wall_mult
    cfg.MIN_WALL_SIZE_USDT = min_wall_usd
    client = st.session_state.client
    st.session_state.scanning = True
    st.session_state.cancel_scan = False
    progress = st.progress(0, "Загрузка пар...")
    try: info = client.get_exchange_info()
    except Exception as e: st.error(f"API: {e}"); st.session_state.scanning = False; return
    if not info or "symbols" not in info:
        st.error(f"❌ {client.last_error or 'Нет'}"); progress.empty()
        st.session_state.scanning = False; return
    blacklist = st.session_state.blacklist
    all_sym = []
    for s in info["symbols"]:
        try:
            if s.get("quoteAsset")!="USDT": continue
            sym = s["symbol"]
            if sym in blacklist: continue
            st_ = s.get("status","")
            if (str(st_) in ("1","ENABLED","True","true") or st_ is True or st_==1) \
               and s.get("isSpotTradingAllowed", True):
                all_sym.append(sym)
        except: continue
    if not all_sym:
        for s in info["symbols"]:
            try:
                sym = s.get("symbol","")
                if s.get("quoteAsset")=="USDT" and sym not in blacklist:
                    all_sym.append(sym)
            except: continue
    if not all_sym: st.error("0 пар"); progress.empty(); st.session_state.scanning = False; return
    progress.progress(5)
    try: tickers = client.get_all_tickers_24h()
    except: st.error("Тикеры"); progress.empty(); st.session_state.scanning = False; return
    if not tickers: st.error(client.last_error); progress.empty(); st.session_state.scanning = False; return
    tm = {t["symbol"]:t for t in tickers if "symbol" in t}
    cands = [(sym,tm[sym]) for sym in all_sym
             if sym in tm and min_vol<=sf(tm[sym].get("quoteVolume",0))<=max_vol]
    cands.sort(key=lambda x: sf(x[1].get("quoteVolume",0)), reverse=True)
    if not cands: st.warning("0 в диапазоне"); progress.empty(); st.session_state.scanning = False; return
    progress.progress(15, f"{len(cands)} пар")
    results, total = [], len(cands)
    for i, (sym, tk) in enumerate(cands):
        if st.session_state.cancel_scan:
            st.warning(f"Скан отменён на {i}/{total}")
            break
        try:
            book = client.get_order_book(sym, cfg.ORDER_BOOK_DEPTH)
            if book:
                r = analyze_order_book(sym, book, tk)
                if r and r.spread_pct >= min_spread:
                    r.trade_count_24h = extract_tc(tk)
                    results.append(r)
        except: pass
        if (i+1)%8==0 or i==total-1:
            progress.progress(15+int((i+1)/total*80), f"{i+1}/{total} → {len(results)}")
    results.sort(key=lambda r: r.score, reverse=True)
    top = results[:top_n]
    progress.progress(96)
    for r in top[:10]:
        if r.trade_count_24h==0:
            try:
                tc = extract_tc(client.get_ticker_24h(r.symbol))
                if tc>0: r.trade_count_24h = tc
            except: pass
    st.session_state.tracker.update(top)
    st.session_state.scan_results = top
    st.session_state.last_scan = time.time()
    st.session_state.total_pairs = total
    st.session_state.scanning = False
    st.session_state.cancel_scan = False
    progress.progress(100); time.sleep(0.15); progress.empty()


# ═══════════════════════════════════════════════════
# Сайдбар
# ═══════════════════════════════════════════════════
PAGES = ["📊 Сканер","🔍 Детали","📈 Мониторинг"]

def go_detail(sym):
    st.session_state.detail_symbol = sym
    st.session_state.target_page = 1

with st.sidebar:
    st.markdown("## ⚙️ Параметры")
    min_vol = st.number_input("Мин объём 24ч ($)", value=100, min_value=0, step=100,
                              help="Минимальный суточный объём торгов в USDT")
    max_vol = st.number_input("Макс объём 24ч ($)", value=500_000, min_value=100, step=10000,
                              help="Максимальный суточный объём торгов в USDT")
    min_spread = st.slider("Мин спред %", 0.0, 20.0, 0.5, 0.1)
    wall_mult = st.slider("Множитель x", 2, 50, 5)
    min_wall_usd = st.number_input("Мин стенка $", value=50, min_value=1, step=10)
    top_n = st.slider("Топ N", 5, 100, 30)
    st.markdown("---")
    auto_on = st.checkbox("🔄 Авто-скан", value=True)
    auto_sec = st.select_slider("Интервал", [15,20,30,45,60,90], value=30)
    if auto_on:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=auto_sec*1000, key="ar")
        except ImportError: st.caption("pip install streamlit-autorefresh")
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        scan_btn = st.button("🚀 Скан", use_container_width=True, type="primary")
    with col_s2:
        if st.button("⛔ Стоп", use_container_width=True):
            st.session_state.cancel_scan = True
    st.markdown("---")
    # Чёрный список
    st.markdown("### 🚫 Чёрный список")
    bl_input = st.text_input("Добавить (через запятую)", placeholder="XYZUSDT,ABCUSDT",
                             key="bl_inp")
    if bl_input:
        for s in bl_input.upper().replace(" ","").split(","):
            if s and s.endswith("USDT"):
                st.session_state.blacklist.add(s)
        st.rerun()
    bl = st.session_state.blacklist
    if bl:
        st.caption(f"Заблокировано: {', '.join(sorted(bl))}")
        if st.button("🗑 Очистить ЧС", key="clr_bl"):
            st.session_state.blacklist = set(); st.rerun()
    st.markdown("---")
    # Избранное
    fav = st.session_state.favorites
    if fav: st.caption(f"⭐ Избранное: {len(fav)}")
    # Импорт/Экспорт избранного
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        uploaded = st.file_uploader("📥 CSV", type=["csv","txt"], key="fav_imp",
                                    label_visibility="collapsed")
        if uploaded:
            content = uploaded.getvalue().decode("utf-8")
            new = {l.strip().upper() for l in content.replace(",","\n").split("\n")
                   if l.strip().upper().endswith("USDT") and len(l.strip())>4}
            if new:
                st.session_state.favorites.update(new)
                st.success(f"+{len(new)}"); st.rerun()
    with col_f2:
        if fav:
            st.download_button("📤", data="\n".join(sorted(fav)).encode(),
                               file_name="favorites.csv", mime="text/csv",
                               use_container_width=True)
    st.markdown("---")
    stats = st.session_state.tracker.get_stats()
    st.caption(f"Сканов: {stats['total_scans']} | ⚡ {stats['total_mover_events']}")
    if st.button("🔧 API", use_container_width=True):
        ok, msg = st.session_state.client.ping()
        st.success(f"✅ {msg}") if ok else st.error(f"❌ {msg}")

# ═══════════════════════════════════════════════════
if scan_btn or (auto_on and not st.session_state.scanning
                and time.time()-st.session_state.last_scan > max(auto_sec-5, 10)):
    run_scan(min_vol, max_vol, min_spread, wall_mult, min_wall_usd, top_n)

_idx = max(0, min(st.session_state.target_page, len(PAGES)-1))
page = st.radio("nav", PAGES, horizontal=True, index=_idx, label_visibility="collapsed")
for i,p in enumerate(PAGES):
    if page==p: st.session_state.target_page = i
st.markdown("---")


# ═══════════════════════════════════════════════════
# СТРАНИЦА 1 — СКАНЕР
# ═══════════════════════════════════════════════════
if page == PAGES[0]:
    results = st.session_state.scan_results
    if not results:
        st.info("Ожидание первого скана...")
    else:
        tracker = st.session_state.tracker
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Найдено", len(results))
        c2.metric("Проверено", st.session_state.total_pairs)
        c3.metric("Лучший", f"⭐{results[0].score}")
        c4.metric("⚡ Переставки", sum(1 for r in results if r.has_movers))

        # ─── Таймфрейм для сделок ───
        tf_col1, tf_col2, tf_col3 = st.columns([2,2,4])
        with tf_col1:
            trades_tf = st.selectbox("Сделки за", ["5м","15м","1ч","4ч","24ч"], index=2, key="ttf")
        with tf_col2:
            sort_by = st.selectbox("Сортировка",
                                   ["Скор ↓","Объём стенки ↓","Расстояние ↑","Время жизни ↓","Спред ↓"],
                                   key="sort")

        # ─── Построение таблицы ───
        rows = []
        for r in results:
            if not r.all_walls: continue
            bw = r.biggest_wall
            # Время жизни из трекера
            tw_list = tracker.get_tracked_walls(r.symbol)
            tw_big = None
            if tw_list:
                for tw in tw_list:
                    if bw and abs(tw.price - bw.price) < bw.price * 0.001:
                        tw_big = tw; break
                if not tw_big: tw_big = tw_list[0]

            lifetime = tw_big.lifetime_str if tw_big else "—"
            lifetime_sec = tw_big.lifetime_sec if tw_big else 0

            bid_top = max(r.bid_walls, key=lambda w: w.size_usdt) if r.bid_walls else None
            ask_top = max(r.ask_walls, key=lambda w: w.size_usdt) if r.ask_walls else None

            rows.append({
                "Скор": r.score,
                "Пара": r.symbol,
                "Цена": fmt_price(r.mid_price),
                "Спред%": round(r.spread_pct, 2),
                "Объём 24ч": fmt_usd(r.volume_24h_usdt),
                "Сделок": r.trade_count_24h if r.trade_count_24h > 0 else "—",
                "BID стенка": f"{fmt_usd(bid_top.size_usdt)} ({bid_top.multiplier}x)" if bid_top else "—",
                "BID расст%": f"{bid_top.distance_pct}%" if bid_top else "—",
                "ASK стенка": f"{fmt_usd(ask_top.size_usdt)} ({ask_top.multiplier}x)" if ask_top else "—",
                "ASK расст%": f"{ask_top.distance_pct}%" if ask_top else "—",
                "Время": lifetime,
                "⚡": "⚡" if r.has_movers else "",
                # скрытые для сортировки
                "_wall_usd": bw.size_usdt if bw else 0,
                "_dist": bw.distance_pct if bw else 99,
                "_lt": lifetime_sec,
                "_spread": r.spread_pct,
            })

        if not rows:
            st.warning("Нет стенок")
        else:
            # Сортировка
            if "Объём" in sort_by: rows.sort(key=lambda r: r["_wall_usd"], reverse=True)
            elif "Расстояние" in sort_by: rows.sort(key=lambda r: r["_dist"])
            elif "Время" in sort_by: rows.sort(key=lambda r: r["_lt"], reverse=True)
            elif "Спред" in sort_by: rows.sort(key=lambda r: r["_spread"], reverse=True)
            # else: default score sort

            display_cols = ["Скор","Пара","Цена","Спред%","Объём 24ч","Сделок",
                           "BID стенка","BID расст%","ASK стенка","ASK расст%","Время","⚡"]
            df_display = pd.DataFrame(rows)[display_cols]
            st.dataframe(df_display, hide_index=True, use_container_width=True,
                         height=min(len(df_display)*35+40, 700))

            # Кнопки перехода
            st.markdown("##### Выбери пару →")
            syms = [r["Пара"] for r in rows]
            nc = min(12, len(syms))
            cols = st.columns(nc)
            for i, sym in enumerate(syms[:nc]):
                with cols[i]:
                    lbl = "⭐" + sym if sym in st.session_state.favorites else sym
                    if st.button(lbl, key=f"g_{sym}", use_container_width=True):
                        go_detail(sym); st.rerun()
            if len(syms) > 12:
                c_sel, c_go, c_fav = st.columns([3,1,1])
                with c_sel:
                    ch = st.selectbox("Все пары", [""] + syms, key="sp")
                with c_go:
                    if ch and st.button("🔍", key="go_sel"):
                        go_detail(ch); st.rerun()
                with c_fav:
                    if ch and st.button("⭐", key="fav_sel"):
                        st.session_state.favorites.add(ch); st.rerun()

            st.download_button("📥 CSV", data=make_csv(df_display),
                               file_name=f"scan_{datetime.now().strftime('%H%M')}.csv",
                               mime="text/csv")


# ═══════════════════════════════════════════════════
# СТРАНИЦА 2 — ДЕТАЛЬНЫЙ АНАЛИЗ
# ═══════════════════════════════════════════════════
elif page == PAGES[1]:
    results = st.session_state.scan_results
    sym_list = [r.symbol for r in results] if results else []
    # Навигация
    hdr = st.columns([1, 3, 2, 1, 1])
    with hdr[0]:
        if st.button("← Назад"): st.session_state.target_page = 0; st.rerun()
    with hdr[1]:
        idx = 0
        ds = st.session_state.detail_symbol
        if ds and ds in sym_list: idx = sym_list.index(ds)+1
        target = st.selectbox("Пара", [""]+sym_list, index=idx, key="dsel", label_visibility="collapsed")
    with hdr[2]:
        manual = st.text_input("", placeholder="XYZUSDT", label_visibility="collapsed")
    symbol = manual.strip().upper() if manual.strip() else target
    with hdr[3]:
        if symbol:
            is_fav = symbol in st.session_state.favorites
            if st.button("⭐" if is_fav else "☆", key="fav_d"):
                if is_fav: st.session_state.favorites.discard(symbol)
                else: st.session_state.favorites.add(symbol)
                st.rerun()
    with hdr[4]:
        if symbol:
            is_bl = symbol in st.session_state.blacklist
            if st.button("🚫" if is_bl else "🔇", key="bl_d",
                         help="Чёрный список"):
                if is_bl: st.session_state.blacklist.discard(symbol)
                else: st.session_state.blacklist.add(symbol)
                st.rerun()

    if not symbol: st.info("Выбери пару"); st.stop()
    st.session_state.detail_symbol = symbol
    client = st.session_state.client
    tracker = st.session_state.tracker

    with st.spinner(f"{symbol}..."):
        try:
            book_raw = client.get_order_book(symbol, 500)
            ticker_raw = client.get_ticker_24h(symbol)
            trades_raw = client.get_recent_trades(symbol, 1000)
            kl_1m = client.get_klines(symbol, "1m", 100)
            kl_5m = client.get_klines(symbol, "5m", 100)
            kl_1h = client.get_klines(symbol, "60m", 100)
            kl_4h = client.get_klines(symbol, "4h", 100)
            kl_1d = client.get_klines(symbol, "1d", 100)
        except Exception as e: st.error(str(e)); st.stop()

    if not book_raw or not book_raw.get("bids") or not book_raw.get("asks"):
        st.error(f"Нет данных {symbol}"); st.stop()
    bids = parse_book(book_raw["bids"])
    asks = parse_book(book_raw["asks"])
    if not bids or not asks: st.error("Пусто"); st.stop()
    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb+ba)/2; spread = (ba-bb)/bb*100
    bdepth = sum(float(p)*float(q) for p,q in bids)
    adepth = sum(float(p)*float(q) for p,q in asks)
    td = ticker_raw
    if isinstance(td, list): td = td[0] if td else {}
    if not isinstance(td, dict): td = {}
    tc24 = extract_tc(td); vol24 = sf(td.get("quoteVolume",0))
    df_1m,df_5m,df_1h,df_4h,df_1d = [parse_klines(x) for x in [kl_1m,kl_5m,kl_1h,kl_4h,kl_1d]]

    st.markdown(f"### {symbol}  ·  {fmt_price(mid)}  ·  [MEXC ↗]({mexc_link(symbol)})")
    m1,m2,m3,m4,m5,m6 = st.columns(6)
    m1.metric("Спред", f"{spread:.2f}%")
    m2.metric("Bid $", f"${bdepth:,.0f}")
    m3.metric("Ask $", f"${adepth:,.0f}")
    m4.metric("Сделок 24ч", f"{tc24:,}" if tc24 else "—")
    m5.metric("Объём 24ч", f"${vol24:,.0f}")
    s4h = kline_stats(df_1h, 4)
    m6.metric("Объём 4ч", f"${s4h['volume']:,.0f}")

    # ─── Плотности с временем жизни ───
    st.markdown("#### 🧱 Плотности (стенки)")
    tw_list = tracker.get_tracked_walls(symbol)
    if tw_list:
        tw_rows = []
        for tw in tw_list:
            tw_rows.append({
                "Сторона": "🟢 BID" if tw.side=="BID" else "🔴 ASK",
                "Цена": fmt_price(tw.price),
                "Объём": fmt_usd(tw.size_usdt),
                "Множ.": f"{tw.multiplier}x",
                "Расст.": f"{tw.distance_pct}%",
                "Время жизни": tw.lifetime_str,
                "Сканов": tw.seen_count,
            })
        st.dataframe(pd.DataFrame(tw_rows), hide_index=True, use_container_width=True)
    else:
        st.caption("Плотности появятся после нескольких сканов")

    # ─── Переставки по этой паре ───
    sym_movers = tracker.get_symbol_movers(symbol)
    if sym_movers:
        st.markdown("#### ⚡ Переставки (история)")
        mv_rows = []
        for e in reversed(sym_movers[-20:]):
            direction = "🟢 LONG" if e.direction=="UP" else "🔴 SHORT"
            mv_rows.append({
                "Время": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                "Сторона": e.side,
                "Объём": fmt_usd(e.size_usdt),
                "Было": fmt_price(e.old_price),
                "Стало": fmt_price(e.new_price),
                "Сдвиг%": f"{e.shift_pct:+.3f}%",
                "Направление": direction,
            })
        st.dataframe(pd.DataFrame(mv_rows), hide_index=True, use_container_width=True)

    # ─── Робот-анализ ───
    st.markdown("#### 🤖 Анализ торгов")
    robot = analyze_robots(trades_raw)
    if robot:
        ri = robot
        emoji = "🤖" if ri["is_robot"] else "👤"
        st.markdown(f"{emoji} **Интервалы:** ср.={ri['avg']:.1f}с  мин={ri['min']:.1f}с  макс={ri['max']:.1f}с")
        st.markdown(f"**Мода:** {ri['mode']}с ({ri['mode_count']} раз, {ri['mode_pct']}% сделок)")
        st.markdown(f"**Объём:** ср.={fmt_usd(ri['avg_vol'])}  мин={fmt_usd(ri['min_vol'])}  макс={fmt_usd(ri['max_vol'])}")

        if ri["robots"]:
            st.markdown(f"**Обнаружено роботов: {len(ri['robots'])}**")
            for j, bot in enumerate(ri["robots"]):
                st.markdown(f"  `Робот #{j+1}`: интервал **{bot['interval']}**, "
                            f"{bot['count']} сделок ({bot['pct']}%), "
                            f"ср.объём {fmt_usd(bot['avg_vol'])}")
        elif ri["is_robot"]:
            st.markdown("🤖 **Один робот** — стабильные интервалы и объёмы")
    else:
        st.caption("Мало сделок для анализа")

    # ─── Объёмы ───
    st.markdown("#### 📊 Объёмы")
    s5,s15,s60 = kline_stats(df_5m,1), kline_stats(df_5m,3), kline_stats(df_5m,12)
    vc = st.columns(5)
    vc[0].metric("5м", f"${s5['volume']:,.0f}", f"{s5['trades']} сд.")
    vc[1].metric("15м", f"${s15['volume']:,.0f}", f"{s15['trades']} сд.")
    vc[2].metric("1ч", f"${s60['volume']:,.0f}", f"{s60['trades']} сд.")
    vc[3].metric("4ч", f"${s4h['volume']:,.0f}", f"{s4h['trades']} сд.")
    vc[4].metric("24ч", f"${vol24:,.0f}", f"{tc24:,} сд.")

    # ─── 5 графиков ───
    st.markdown("#### 📈 Графики")
    tabs = st.tabs(["1м","5м","1ч","4ч","1д"])
    for tab, df_k, lbl in zip(tabs, [df_1m,df_5m,df_1h,df_4h,df_1d],
                               ["1m","5m","1h","4h","1d"]):
        with tab:
            f = build_candlestick(df_k, symbol, lbl, mid)
            if f: st.plotly_chart(f, use_container_width=True)
            else: st.warning(f"Нет {lbl}")

    # ─── Стакан + Хитмап ───
    st.markdown("#### 📖 Стакан / Хитмап")
    dv = st.select_slider("Глубина", [20,30,50,100], value=50, key="obd")
    col_ob, col_hm = st.columns(2)
    with col_ob:
        fg = build_orderbook_chart(bids, asks, mid, dv)
        if fg: st.plotly_chart(fg, use_container_width=True)
    with col_hm:
        fh = build_heatmap(bids, asks, mid, 30)
        if fh: st.plotly_chart(fh, use_container_width=True)

    # ─── Сделки ───
    trades_df = pd.DataFrame()
    if trades_raw and isinstance(trades_raw, list):
        st.markdown("#### 📋 Последние сделки")
        trs = []
        for t in trades_raw[:50]:
            try:
                p,q,ts = sf(t.get("price",0)), sf(t.get("qty",0)), sf(t.get("time",0))
                trs.append({"Время": pd.to_datetime(ts,unit="ms").strftime("%H:%M:%S") if ts>0 else "—",
                            "Цена": fmt_price(p), "Кол-во": q, "$": round(p*q,2),
                            "": "🟢" if not t.get("isBuyerMaker") else "🔴"})
            except: continue
        if trs:
            trades_df = pd.DataFrame(trs)
            st.dataframe(trades_df, hide_index=True, use_container_width=True, height=300)

    # Экспорт
    st.markdown("---")
    export = {}
    ob_df = pd.DataFrame([{"Сторона":s,"Цена":float(p),"Кол-во":float(q),"$":round(float(p*q),4)}
                           for s,data in [("BID",bids),("ASK",asks)] for p,q in data])
    export["orderbook"] = ob_df
    if not trades_df.empty: export["trades"] = trades_df
    for lbl, kdf in [("1m",df_1m),("5m",df_5m),("1h",df_1h),("4h",df_4h),("1d",df_1d)]:
        if kdf is not None and not kdf.empty: export[f"klines_{lbl}"] = kdf
    def sym_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
            for n,d in export.items(): zf.writestr(f"{symbol}_{n}.csv", d.to_csv(index=False))
        buf.seek(0); return buf.getvalue()
    st.download_button(f"📦 {symbol} ZIP", data=sym_zip(),
                       file_name=f"{symbol}_{datetime.now().strftime('%H%M')}.zip",
                       mime="application/zip", use_container_width=True)


# ═══════════════════════════════════════════════════
# СТРАНИЦА 3 — МОНИТОРИНГ ПЕРЕСТАВОК
# ═══════════════════════════════════════════════════
elif page == PAGES[2]:
    tracker = st.session_state.tracker
    st.markdown("### 📈 Мониторинг переставок")
    st.caption("Переставляш = плотность двигающаяся по стакану. Признак робота/ММ.")

    t_log, t_rank = st.tabs(["📋 Лог","🏆 Рейтинг"])

    with t_log:
        movers = tracker.get_active_movers(7200)
        if not movers:
            st.info("Нет переставок. Подожди несколько сканов.")
        else:
            st.success(f"⚡ {len(movers)} переставок за 2ч")
            mr = []
            for e in reversed(movers):
                direction = "🟢 LONG" if e.direction=="UP" else "🔴 SHORT"
                mr.append({
                    "Время": datetime.fromtimestamp(e.timestamp).strftime("%H:%M:%S"),
                    "Пара": e.symbol,
                    "Сторона": e.side,
                    "Объём": fmt_usd(e.size_usdt),
                    "Было": fmt_price(e.old_price),
                    "Стало": fmt_price(e.new_price),
                    "Сдвиг%": f"{e.shift_pct:+.3f}%",
                    "Направление": direction,
                })
            mdf = pd.DataFrame(mr)
            st.dataframe(mdf, hide_index=True, use_container_width=True)
            us = sorted({e.symbol for e in movers})
            cp,cg = st.columns([3,1])
            with cp: cm = st.selectbox("→ Детали", [""]+us, key="mp")
            with cg:
                if cm and st.button("🔍", key="mg"):
                    go_detail(cm); st.rerun()
            st.download_button("📥", data=make_csv(mdf),
                               file_name=f"movers_{datetime.now().strftime('%H%M')}.csv",
                               mime="text/csv")

    with t_rank:
        tm = tracker.get_top_movers(20)
        if tm:
            st.markdown("**Пары с наибольшим числом переставок:**")
            for i, (sym, cnt) in enumerate(tm):
                cols = st.columns([3,1,1])
                cols[0].markdown(f"**{i+1}. {sym}** — {cnt} переставок")
                if cols[1].button("🔍", key=f"tr_{sym}"):
                    go_detail(sym); st.rerun()
                if cols[2].button("⭐", key=f"tf_{sym}"):
                    st.session_state.favorites.add(sym); st.rerun()
            fig = go.Figure(go.Bar(
                x=[x[0] for x in tm], y=[x[1] for x in tm],
                marker_color="#00BFFF"))
            fig.update_layout(template="plotly_dark", height=250, title="Топ переставляшей")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Накопи данные — запусти несколько сканов")

st.caption("MEXC Scanner v4.0")

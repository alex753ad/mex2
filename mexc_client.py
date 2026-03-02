"""
Клиент MEXC API — v2.4
Синхронный (Streamlit) + асинхронный (VPS)
Диагностика, retry, fallback domains, кэширование exchangeInfo
"""
import asyncio
import time
import requests
from typing import Optional

import config

# Несколько зеркал MEXC API (пробуем по порядку)
MEXC_DOMAINS = [
    "https://api.mexc.com",
    "https://www.mexc.com",
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
}


# ═══════════════════════════════════════════════════
# Синхронный клиент (для Streamlit)
# ═══════════════════════════════════════════════════

class MexcClientSync:
    """Синхронный HTTP-клиент с диагностикой"""

    def __init__(self):
        self.base_url = config.MEXC_BASE_URL
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count = 0
        self._window_start = time.time()
        self.last_error = ""
        self._exchange_info_cache = None
        self._exchange_info_time = 0

    def _rate_limit(self):
        now = time.time()
        if now - self._window_start < 1.0:
            self._req_count += 1
            if self._req_count > 12:
                time.sleep(1.0 - (now - self._window_start) + 0.1)
                self._window_start = time.time()
                self._req_count = 0
        else:
            self._window_start = now
            self._req_count = 1

    def _get(self, endpoint: str, params: dict = None,
             timeout: int = 20, retries: int = 2):
        """GET запрос с retry и диагностикой"""
        self._rate_limit()
        last_err = ""
        for attempt in range(retries + 1):
            try:
                url = f"{self.base_url}{endpoint}"
                r = self.session.get(url, params=params, timeout=timeout)
                if r.status_code == 200:
                    self.last_error = ""
                    return r.json()
                elif r.status_code == 429:
                    last_err = f"429 Rate Limit (попытка {attempt+1})"
                    time.sleep(3 + attempt * 2)
                    continue
                elif r.status_code == 403:
                    last_err = f"403 Forbidden — IP заблокирован MEXC"
                    break
                elif r.status_code == 503:
                    last_err = f"503 Service Unavailable"
                    time.sleep(2)
                    continue
                else:
                    last_err = (f"HTTP {r.status_code}: "
                                f"{r.text[:200]}")
                    break
            except requests.exceptions.ConnectTimeout:
                last_err = f"ConnectTimeout ({timeout}с)"
                time.sleep(1)
            except requests.exceptions.ReadTimeout:
                last_err = f"ReadTimeout ({timeout}с)"
                time.sleep(1)
            except requests.exceptions.ConnectionError as e:
                last_err = f"ConnectionError: {str(e)[:100]}"
                time.sleep(1)
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:100]}"
                break

        self.last_error = last_err
        return None

    def _get_with_fallback(self, endpoint: str, params: dict = None,
                           timeout: int = 20):
        """Пробует основной URL, потом fallback домены"""
        # Сначала основной
        result = self._get(endpoint, params, timeout)
        if result is not None:
            return result

        # Пробуем альтернативные домены
        original = self.base_url
        for domain in MEXC_DOMAINS:
            if domain == original:
                continue
            self.base_url = domain
            result = self._get(endpoint, params, timeout, retries=1)
            if result is not None:
                # Запоминаем работающий домен
                return result
            self.base_url = original
        self.base_url = original
        return None

    def get_exchange_info(self):
        """С кэшированием — exchangeInfo меняется редко"""
        now = time.time()
        # Кэш на 5 минут
        if (self._exchange_info_cache
                and now - self._exchange_info_time < 300):
            return self._exchange_info_cache
        result = self._get_with_fallback(
            "/api/v3/exchangeInfo", timeout=30)
        if result:
            self._exchange_info_cache = result
            self._exchange_info_time = now
        return result

    def get_all_tickers_24h(self):
        return self._get_with_fallback("/api/v3/ticker/24hr",
                                       timeout=25)

    def get_order_book(self, symbol: str, limit: int = 100):
        return self._get("/api/v3/depth",
                         {"symbol": symbol, "limit": limit})

    def get_recent_trades(self, symbol: str, limit: int = 100):
        return self._get("/api/v3/trades",
                         {"symbol": symbol, "limit": limit})

    def get_klines(self, symbol: str, interval: str = "60m",
                   limit: int = 100):
        return self._get("/api/v3/klines", {
            "symbol": symbol, "interval": interval, "limit": limit,
        })

    def get_agg_trades(self, symbol: str, limit: int = 1000):
        return self._get("/api/v3/aggTrades", {
            "symbol": symbol, "limit": limit,
        })

    def get_ticker_24h(self, symbol: str):
        return self._get("/api/v3/ticker/24hr",
                         {"symbol": symbol})

    def ping(self) -> tuple[bool, str]:
        """Диагностика соединения"""
        try:
            r = self.session.get(
                f"{self.base_url}/api/v3/ping", timeout=10)
            if r.status_code == 200:
                return True, f"OK ({self.base_url})"
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def server_time(self) -> tuple[bool, str]:
        """Время сервера MEXC"""
        try:
            r = self.session.get(
                f"{self.base_url}/api/v3/time", timeout=10)
            if r.status_code == 200:
                data = r.json()
                return True, str(data.get("serverTime", "?"))
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, str(e)


# ═══════════════════════════════════════════════════
# Асинхронный клиент (для ws_monitor.py)
# ═══════════════════════════════════════════════════

try:
    import aiohttp

    class MexcClientAsync:
        """Асинхронный HTTP-клиент для MEXC"""

        def __init__(self):
            self.base_url = config.MEXC_BASE_URL
            self._session: Optional[aiohttp.ClientSession] = None
            self._req_count = 0
            self._window_start = time.time()

        async def _get_session(self) -> aiohttp.ClientSession:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15),
                    headers=HEADERS,
                )
            return self._session

        async def close(self):
            if self._session and not self._session.closed:
                await self._session.close()

        async def _request(self, endpoint: str,
                           params: dict = None):
            session = await self._get_session()
            now = time.time()
            if now - self._window_start < 1.0:
                self._req_count += 1
                if self._req_count > 12:
                    await asyncio.sleep(
                        1.0 - (now - self._window_start) + 0.1)
                    self._window_start = time.time()
                    self._req_count = 0
            else:
                self._window_start = now
                self._req_count = 1

            try:
                async with session.get(
                    f"{self.base_url}{endpoint}", params=params
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        await asyncio.sleep(5)
                        return await self._request(endpoint, params)
                    return None
            except Exception:
                return None

        async def get_exchange_info(self):
            return await self._request("/api/v3/exchangeInfo")

        async def get_all_tickers_24h(self):
            return await self._request("/api/v3/ticker/24hr")

        async def get_order_book(self, symbol: str,
                                 limit: int = 100):
            return await self._request(
                "/api/v3/depth",
                {"symbol": symbol, "limit": limit})

        async def get_recent_trades(self, symbol: str,
                                    limit: int = 100):
            return await self._request(
                "/api/v3/trades",
                {"symbol": symbol, "limit": limit})

except ImportError:
    # aiohttp не установлен — только sync-клиент
    pass

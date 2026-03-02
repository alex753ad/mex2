# MEXC Density Scanner v4.0

## Запуск локально (Windows)

```bash
# 1. Открой командную строку (Win+R → cmd) и перейди в папку:
cd C:\Users\User\Desktop\mexc

# 2. Установи зависимости (один раз):
pip install -r requirements.txt

# 3. Запусти:
streamlit run app.py
```

**Если `streamlit` не найден:**
```bash
python -m streamlit run app.py
```

**Если `python` не найден:**
```bash
py -m pip install -r requirements.txt
py -m streamlit run app.py
```

**Если Python 3.14 (новый, может быть несовместим):**
```bash
# Установи Python 3.11 или 3.12 с python.org
# Потом:
py -3.12 -m pip install -r requirements.txt
py -3.12 -m streamlit run app.py
```

## Структура
- `app.py` — Streamlit-дашборд (главный файл)
- `mexc_client.py` — HTTP-клиент MEXC API
- `analyzer.py` — Анализ стакана, поиск стенок
- `history.py` — Трекер переставок
- `config.py` — Настройки
- `ws_monitor.py` — WebSocket-монитор (для VPS)

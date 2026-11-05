# BTC 5 分鐘跨平台配對交易 Demo

這是獨立資料夾中的初版系統：配對 Polymarket 與 Binance Wallet Predict 同一個 BTC 5 分鐘區間，讀取兩邊 UP/DOWN 完整可見 order book，計算 walk-forward spread Z-score，並用 shadow execution 模擬延遲後的雙腿進出場。

`live` 是即時行情 shadow mode：持續記錄原始 L2 深度、行情 API RTT、來源時間戳年齡與模擬訂單事件；不會建立或送出任何真實訂單。

## 執行

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe run.py demo
```

`demo` 用合成行情與合成歷史值，走一次既有訊號/紙上成交流程；不連網、不污染 live 資料庫。即時 shadow 掃描先複製範例環境檔：

```powershell
Copy-Item .env.example .env
```

編輯 `.env`，填入 Binance Wallet Predict 市場資料 API 所需的 `BINANCE_API_KEY`、`BINANCE_API_SECRET`，再執行：

```powershell
.venv\Scripts\python.exe run.py live
```

Discord 損益推送是選用功能：在 Discord 目標伺服器的「伺服器設定 → 整合 → Webhooks」建立 incoming webhook，選擇要接收訊息的頻道，再複製 webhook URL 到 `.env` 的 `PAIR_DISCORD_WEBHOOK_URL`。程式啟動後會立即推送摘要，再依 `PAIR_DISCORD_PNL_INTERVAL_SECONDS` 定期推送；預設每 900 秒（15 分鐘），最短可設 60 秒。摘要包含本次啟動後新增的已實現損益、資料庫累計損益、完成筆數、勝率、未平倉和未解決筆數。Webhook 欄位留白就不推送。Webhook URL 等同密碼，請勿分享或提交 `.env`；`.gitignore` 已排除 `.env`。

`python run.py live --duration-seconds 120` 會跑兩分鐘。預設資料庫為 `data/pair_trading.sqlite3`。`python run.py history-status` 查看校準進度；`python run.py report` 查看 shadow 配對成交率、平倉損益及行情延遲摘要。

不預載舊回測/舊日期資料。啟動後先進入資料蒐集模式：持續存報價與 order book，但五個時間桶都各自累積至少 50 個已結束市場以前，不產生進場訊號。每個已結束市場的每個 bucket 至少收集 5 個有效 spread 才記錄該桶 median；連續完整收集約需 50 個 5 分鐘市場（約 4 小時 10 分鐘）才可能解除 warm-up。

`python run.py clear-history` 只清除 bucket calibration rows 和舊的 `data/market_history.csv`，保留 quote ticks、完整深度、shadow logs 及紙上交易紀錄。手動匯入校準資料可用 `import-history --history-csv FILE`，CSV 欄位需為 `market_key,market_end_time,bucket,median_spread`，可加 `sample_count`；程式仍會排除目前市場及未來市場歷史，任一 bucket 不足 50 筆就維持資料蒐集模式。

## 策略與模擬成交

- Spread = Polymarket UP mid − Binance UP mid；以五個 60 秒 event-time bucket 分組。
- 每個已結束市場、每個 bucket 只貢獻一筆市場 median；rolling window 最近 200 個已結束市場。所有 bucket 至少各有 50 個過去市場，才允許產生訊號。
- `Z < -2.5`：買 Polymarket UP + Binance DOWN；`Z > +2.5`：買 Polymarket DOWN + Binance UP。每個 matched market 最多進場一次。
- 每腿訂單至少 5 shares。訊號觸發後，兩腿各自加入模擬 network/API/jitter 延遲；僅能用「模擬訂單抵達後收到」的第一份新鮮 WebSocket book 做 IOC 撮合，逐檔消耗可見 ask 深度。部分成交的 IOC 餘額視為取消；兩腿數量不平衡的殘餘曝險會另排延遲回補。
- 配對成交部分從較晚一腿成交後持有 20 秒，再對兩腿分別模擬延遲平倉，按可見 bid 深度逐檔成交。部分退出會以新訂單延遲後重試。進出場都計 taker fee，預設每份 `0.07 * p * (1-p)`。
- Gamma 與 Binance market metadata 仍由 REST 更新；Polymarket CLOB 與 Binance Predict 的 L2 order book 改由 WebSocket 推送。SQLite 保存每個 outcome 的完整 L2 snapshot 和價位，shadow 事件引用 snapshot ID，並記錄 VWAP、fee、partial fill/cancel、source age 及模擬訂單 latency 分量。

Binance 目前的二元市場 metadata 會提供一個 `marketId` 與 UP/DOWN 兩個 outcome token；orderbook WebSocket payload 只帶 `marketId`，不帶 `tokenId`。程式以該 stream 的 UP 簿作為基準，依二元合約互補關係推導 DOWN 簿：DOWN bids = `1 - UP asks`、DOWN asks = `1 - UP bids`，保留每檔數量。DOWN 因此是從同一份 Binance stream 推導，並非第二份獨立行情簿。

## 延遲量測與限制

Order book 透過 WebSocket 接收：Polymarket 先用完整簿初始化，再將價格檔增量套用到本地簿；Binance 推送完整簿，並依來源時間戳忽略較舊更新。行情快照由更新事件觸發，短暫合併密集事件；市場 metadata 仍每 10 秒用 REST 更新。資料庫保存來源時間戳可用時的 source age，以及本機 WebSocket 收到時間；沒有來源時間戳時以本機收到時間計算 freshness。WebSocket feed 不提供單次 order-book request RTT；report 會把保留下來的舊 REST RTT 樣本數與平均值單獨標出，新 WebSocket 樣本的 RTT 欄位留空。

由於完全不送訂單，真實「送單網路延遲」與「交易 API 接收/ACK 延遲」無從量測。程式分開保存可調的 network/API 延遲假設，另加 0 到設定上限的隨機 jitter。預設各 venue network=100ms、API=150ms、jitter=0–100ms；這些只是情境參數，不是平台實測值，可用 `.env` 改值比較敏感度。

Shadow 比「訊號當下立即按 mid/top-of-book 成交」更嚴格，但仍不等同實盤：WebSocket 斷線期間不會有可用行情，重連後需等新簿初始化；可見深度仍可能在訂單抵達前撤掉；目前假設 taker IOC 立即吃可見深度、不排隊，也沒有真實 ACK/拒單、rate limit、實際費率級距或結算自動化。市場結束時仍未平倉的交易標記 `UNRESOLVED_AT_END`，不猜結算損益。跨平台相同時間區間也不代表結算規則相同，shadow PnL 不是實盤獲利證明。

## 參考來源

- [chester-tsai/polymarket_trade_project，`feat/strategy-integration`（commit `152db7ee`）](https://github.com/chester-tsai/polymarket_trade_project/tree/feat/strategy-integration)：BTC 5m 市場探索、UP/DOWN token 對應與 order-book 讀取模式。
- [Polymarket CLOB WebSocket market channel](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)：asset 訂閱、完整簿初始化與價格檔更新格式。
- [Binance Prediction Market Orderbook WebSocket Push API](https://developers.binance.com/en/docs/products/w3w-prediction/websocket-api/orderbook)：簽名 topic 格式、完整簿 payload 與更新時間戳。

此 Demo 僅重做市場 metadata 與 order-book 資料路徑；不含現貨 feed、錢包、真實下單、結算自動化或來源 repo 的其他策略/模型。

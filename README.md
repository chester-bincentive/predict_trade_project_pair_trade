# BTC 5 分鐘跨平台配對交易 Demo

這是獨立資料夾中的初版系統：配對 Polymarket 與 Binance Wallet Predict 同一個 BTC 5 分鐘區間，讀取兩邊 UP/DOWN 完整可見 order book，計算 walk-forward spread Z-score，並用 shadow execution 模擬延遲後的雙腿進出場。

`live` 是即時行情 paper/shadow mode：order book 只在記憶體中供策略與成交模擬使用；資料庫保存訊號決策、紙上訂單嘗試與成交、費用與損益，以及市場訂閱和程式啟停事件。為了跨重啟維持策略校準，另保留每個 bucket 最近 200 筆 spread 中位數與 100 筆 RV10 中位數；不保存逐筆行情或完整深度。不會建立或送出任何真實訂單。
`demo` 與公開的 `PaperEngine` 也使用相同的延遲到達成交邏輯。

## 執行

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe run.py demo
```

此工作區已建立 `.venv`。若要在 PowerShell 直接輸入 `python`，只對目前視窗啟用虛擬環境：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
. .\.venv\Scripts\Activate.ps1
python --version
```

也可略過啟用步驟，直接執行 `.\.venv\Scripts\python.exe run.py demo`。

`demo` 用合成行情與合成歷史值，走一次既有訊號/紙上成交流程；不連網、不污染 live 資料庫。即時 shadow 掃描先複製範例環境檔：

```powershell
Copy-Item .env.example .env
```

編輯 `.env`，填入 Binance Wallet Predict 市場資料 API 所需的 `BINANCE_API_KEY`、`BINANCE_API_SECRET`，再執行：

```powershell
.venv\Scripts\python.exe run.py live
```

Discord 損益推送是選用功能：在 Discord 目標伺服器的「伺服器設定 → 整合 → Webhooks」建立 incoming webhook，選擇要接收訊息的頻道，再複製 webhook URL 到 `.env` 的 `PAIR_DISCORD_WEBHOOK_URL`。程式啟動後會立即推送摘要，再依 `PAIR_DISCORD_PNL_INTERVAL_SECONDS` 定期推送；預設每 900 秒（15 分鐘），最短可設 60 秒。摘要包含本次啟動後新增的已實現損益、資料庫累計損益、完成筆數、勝率、未平倉和未解決筆數。Webhook 欄位留白就不推送。Webhook URL 等同密碼，請勿分享或提交 `.env`；`.gitignore` 已排除 `.env`。

`python run.py live --duration-seconds 120` 會跑兩分鐘。預設資料庫為 `data/pair_trading.sqlite3`。`python run.py history-status` 查看校準進度；`python run.py report` 查看 paper/shadow 配對成交率與損益。

不預載舊回測/舊日期資料。啟動後行情與 order book 只在記憶體中運作；資料庫不逐筆保存它們。策略校準只保存每個已結束市場、每個 bucket 的 spread 與 RV10 中位數，並限制在 rolling window 內。訊號需要各 bucket 至少 50 個 spread 歷史樣本，進場 bucket 至少 100 個 OU 與 RV10 歷史樣本；連續完整蒐集約 100 個 5 分鐘市場（約 8 小時 20 分鐘）後，才可能解除 warm-up。

`python run.py clear-history` 只清除 bucket calibration rows 和舊的 `data/market_history.csv`；舊版本留下的 quote ticks 與完整深度資料會保留，但新版 live 不再新增這些資料。手動匯入 spread 校準資料可用 `import-history --history-csv FILE`，CSV 欄位需為 `market_key,market_end_time,bucket,median_spread`，可加 `sample_count`；RV10 q90 的 100 個市場樣本仍需由 live 行情蒐集。

## 策略與模擬成交

- Spread = Polymarket UP mid − Binance UP mid；以五個 60 秒 event-time bucket 分組。
- OU score 使用每個已結束市場、每個 bucket 一筆 spread median，rolling window 最近 100 個市場。Bucket 0 使用一般 z-score；其他 bucket 以這 100 筆估計 AR(1) 平穩均值和標準差，無法估計時退回 z-score。進場 bucket 至少要有 100 筆 OU 歷史。RV10 q90 取同 bucket 最近 100 個已結束市場的 market-median RV10，必須累積滿 100 個才計算。資料庫每個 bucket 各保留最近 100 筆 spread 與 RV10 校準摘要。
- 僅在 `|OU score| > 3` 且即時 `RV10 > 歷史同 bucket q90` 時進場；OU score 的正負決定方向：負值買 Polymarket UP + Binance DOWN，正值買 Polymarket DOWN + Binance UP。RV10 是最近 10 秒價差相鄰變動平方和開根號，跨 bucket 重設。每個 matched market 最多進場一次。
- 每腿訂單至少 5 shares。訊號觸發後，兩腿各自固定延遲 330ms 到達；首次 IOC 使用到達當下記憶體中最新、已收到且未過期的 WebSocket book，不等下一筆報價，逐檔消耗可見 ask 深度。部分成交的 IOC 餘額視為取消；兩腿數量不平衡的殘餘曝險會另排延遲回補，後續重試等新的報價。
- OU score 沿進場方向回到 ±1.5 時觸發出場，最多持有 60 秒；入場時至少需剩 20 秒市場時間。出場訊號觸發後，兩腿分別模擬 330ms 延遲平倉，按到達當下記憶體報價的可見 bid 深度逐檔成交。部分退出會等新的報價再以新訂單延遲重試。進出場都計 taker fee，預設每份 `0.07 * p * (1-p)`。
- Gamma 與 Binance market metadata 仍由 REST 更新；Polymarket CLOB 與 Binance Predict 的 L2 order book 改由 WebSocket 推送。Order book 留在記憶體中，不逐次寫入 SQLite。訊號成立時保存一筆交易決策；紙上進出場的送單、成交或未成交、每腿數量與 VWAP、費用和損益寫入交易及事件表。程式另記錄 WebSocket 市場訂閱及 run 起訖事件。

Binance 目前的二元市場 metadata 會提供一個 `marketId` 與 UP/DOWN 兩個 outcome token；orderbook WebSocket payload 只帶 `marketId`，不帶 `tokenId`。程式以該 stream 的 UP 簿作為基準，依二元合約互補關係推導 DOWN 簿：DOWN bids = `1 - UP asks`、DOWN asks = `1 - UP bids`，保留每檔數量。DOWN 因此是從同一份 Binance stream 推導，並非第二份獨立行情簿。

## 延遲量測與限制

Order book 透過 WebSocket 接收：Polymarket 先用完整簿初始化，再將價格檔增量套用到本地簿；Binance 推送完整簿，並依來源時間戳忽略較舊更新。行情快照由更新事件觸發，短暫合併密集事件；市場 metadata 仍每 10 秒用 REST 更新。source age 在記憶體中用於 freshness 判斷，並附在實際紙上訂單事件；不把整份 order book 或每次行情快照存進資料庫。WebSocket feed 不提供單次 order-book request RTT。

由於完全不送訂單，真實「送單網路延遲」與「交易 API 接收/ACK 延遲」無從量測。預設每腿從模擬送單到抵達固定 330ms、jitter=0；這是情境假設，不是平台實測值。可用 `.env` 調整延遲參數。

Shadow 比「訊號當下立即按 mid/top-of-book 成交」更嚴格，但仍不等同實盤：WebSocket 斷線期間不會有可用行情，重連後需等新簿初始化；可見深度仍可能在訂單抵達前撤掉；目前假設 taker IOC 立即吃可見深度、不排隊，也沒有真實 ACK/拒單、rate limit、實際費率級距或結算自動化。市場結束時仍未平倉的交易標記 `UNRESOLVED_AT_END`，不猜結算損益。跨平台相同時間區間也不代表結算規則相同，shadow PnL 不是實盤獲利證明。

## 參考來源

- [chester-tsai/polymarket_trade_project，`feat/strategy-integration`（commit `152db7ee`）](https://github.com/chester-tsai/polymarket_trade_project/tree/feat/strategy-integration)：BTC 5m 市場探索、UP/DOWN token 對應與 order-book 讀取模式。
- [Polymarket CLOB WebSocket market channel](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)：asset 訂閱、完整簿初始化與價格檔更新格式。
- [Binance Prediction Market Orderbook WebSocket Push API](https://developers.binance.com/en/docs/products/w3w-prediction/websocket-api/orderbook)：簽名 topic 格式、完整簿 payload 與更新時間戳。

此 Demo 僅重做市場 metadata 與 order-book 資料路徑；不含現貨 feed、錢包、真實下單、結算自動化或來源 repo 的其他策略/模型。

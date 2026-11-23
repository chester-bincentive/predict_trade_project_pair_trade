# BTC 5 分鐘跨平台訊號 Polymarket 單腿 Shadow Trade Demo

這是獨立資料夾中的初版系統：讀取 Polymarket 與 Binance Wallet Predict 同一個 BTC 5 分鐘區間的行情，計算 walk-forward spread 與 OU score，再以 shadow execution 模擬只在 Polymarket 建立單腿部位。Binance 行情仍用於訊號計算，不送出或模擬 Binance 訂單。

未設定模式時，`live` 指令以即時行情執行 paper/shadow mode：order book 只在記憶體中供策略與成交模擬使用；資料庫保存訊號決策、紙上訂單嘗試與成交、費用與損益，以及市場訂閱和程式啟停事件。只有明確指定 `--execution-mode live` 或設定 `PAIR_TRADING_MODE=live` 才會送出真實訂單。`.env.example` 是 live 範本；複製成 `.env` 並填妥有效憑證後，會啟用真實下單。為了跨重啟維持策略校準，另保留每個 bucket 最近 200 筆 spread 中位數與 100 筆 RV10 中位數；不保存逐筆行情或完整深度。
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

`demo` 用合成行情與合成歷史值，走一次既有訊號/紙上成交流程；不連網、不污染 live 資料庫。複製範例環境檔：

```powershell
Copy-Item .env.example .env
```

`.env.example` 預設為 `PAIR_TRADING_MODE=live`。若要執行 paper/shadow，先將它改為 `PAIR_TRADING_MODE=paper`。Live 模式還需要填妥下方列出的 Polymarket 憑證；不要把 `.env` 提交到 repo。

編輯 `.env`，填入 Binance Wallet Predict 市場資料 API 所需的 `BINANCE_API_KEY`、`BINANCE_API_SECRET`，再依「實際下單模式」一節設定 Polymarket 憑證。明確以 live 模式啟動：

```powershell
.venv\Scripts\python.exe run.py live --execution-mode live
```

Discord 通知是選用功能：在 Discord 目標伺服器的「伺服器設定 → 整合 → Webhooks」建立 incoming webhook，選擇接收頻道，並把 webhook URL 設為 `.env` 的 `PAIR_DISCORD_WEBHOOK_URL`。損益摘要依 `PAIR_DISCORD_PNL_INTERVAL_SECONDS` 定期推送；預設每 900 秒（15 分鐘），最短 60 秒。其他有效的 Live 交易風控情況會另行立即告警，並 mention `PAIR_DISCORD_MENTION_USER_ID` 指定的使用者；下單結果不明仍會為避免重複送單而暫停該市場到期，但不會另行提醒。市場／Token 驗證失敗、行情／交易主迴圈錯誤、低於最小賣出量、賣出餘額／授權拒單及退出行情暫時不可用也只會記錄並按既有流程處理，不會另行提醒。告警只允許 mention 指定使用者，不會解析 `@everyone` 或角色。Webhook 欄位留白時不會推送任何 Discord 通知。Webhook URL 等同密碼，請勿分享或提交 `.env`；`.gitignore` 已排除 `.env`。

`python run.py live --duration-seconds 120` 會跑兩分鐘。預設資料庫為 `data/pair_trading.sqlite3`。`python run.py history-status` 查看校準進度；`python run.py report` 查看 paper/shadow 單腿成交率與損益。

不預載舊回測/舊日期資料。啟動後行情與 order book 只在記憶體中運作；資料庫不逐筆保存它們。策略校準只保存每個已結束市場、每個 bucket 的 spread 與 RV10 中位數，並限制在 rolling window 內。訊號需要各 bucket 至少 50 個 spread 歷史樣本，進場 bucket 至少 100 個 OU 與 RV10 歷史樣本；連續完整蒐集約 100 個 5 分鐘市場（約 8 小時 20 分鐘）後，才可能解除 warm-up。

`python run.py clear-history` 只清除 bucket calibration rows 和舊的 `data/market_history.csv`；舊版本留下的 quote ticks 與完整深度資料會保留，但新版 live 不再新增這些資料。手動匯入 spread 校準資料可用 `import-history --history-csv FILE`，CSV 欄位需為 `market_key,market_end_time,bucket,median_spread`，可加 `sample_count`；RV10 q90 的 100 個市場樣本仍需由 live 行情蒐集。

## 實際下單模式

未設定模式時，程式仍以 paper/shadow 執行；`.env.example` 已設為 `PAIR_TRADING_MODE=live`，因此複製後會選擇真實下單模式。Live 使用 `polymarket-client==0.18.0`，私鑰由 `CREDSTORE_REGION` 與 `CREDSTORE_SECRET_NAME` 指定的 credstore 讀取，或直接填 `POLYMARKET_PRIVATE_KEY`。另外必須提供 `POLYMARKET_RELAYER_API_KEY` 與 `POLYMARKET_RELAYER_API_KEY_ADDRESS`。EOA 可不填 wallet address；proxy/smart-wallet 要填 `POLYMARKET_WALLET_ADDRESS`。範例中的憑證欄位保持空白，需自行填入有效值；沒有有效憑證時不能送單。要跑 paper/shadow，請把 `PAIR_TRADING_MODE` 改為 `paper`。

Live 會先驗證 Gamma condition ID 與登入後 CLOB 的 UP/DOWN token 是否一致，並在進入訊號區前預熱 SDK 訂單 metadata、驗證與連線；CLOB book-read 與 order-post 的 HTTP/2 連線都會定期暖機。買入使用 FAK market order，`max_price` 設為最新記憶體 best ask 加 `PAIR_LIVE_BUY_MAX_PRICE_TICKS` 個市場 tick（預設 15）；USDC 預算按「目標 shares × max_price」向上取整到美分，手續費另計。市場 tick grid 在預簽池限制內時，程式會在市場就緒階段為兩個 token 預簽各個可能的價格上限；若池未就緒或 tick grid 過大，則使用同一 `max_price` 保護路徑即時簽名。兩種路徑都不需要在訊號後讀取 CLOB `/book`。明確 FAK 未撮合時，程式重讀最新記憶體簿；若 best ask 未超過首次訊號的 `max_price`，沿用原上限與 USDC 預算最多重試一次，超過上限就記錄跳過、不追價。逾時、取消或回覆不完整則標為未知，不會自動重送。

賣出使用 exact-share FAK，限價設為 $0.01，讓可成交的剩餘持倉盡量立即賣出；FAK 明確未成交或部分成交時，立即用最新記憶體簿重送剩餘股數，不等冷卻時間或下一筆 WebSocket 更新。若目前沒有可用 bid、報價過期或市場驗證失敗，則等可用簿更新後再送。委託前會檢查簿的更新年齡（`PAIR_LIVE_MAX_BOOK_AGE_SECONDS`，預設 2 秒）與市場最後 60 秒限制。每次 intent 會先寫入資料庫，再呼叫 CLOB；簽名、SDK POST、HTTP 回應和回報耗時也會寫入交易事件。

到期仍持有的部位改記為 `SETTLEMENT_PENDING`，釋放策略槽位繼續下一個市場。獨立 WebSocket 監聽 Polymarket `market_resolved` 勝方，將未平倉 shares 按 $1 或 $0 派彩納入淨損益；重啟時會替資料庫中仍待結算的市場恢復監聽，最長監聽到到期後 `PAIR_RESOLUTION_GRACE_SECONDS`（預設 900 秒）。此流程只更新本機交易帳本，不會替錢包執行鏈上 redeem。若 API 訂單結果不明，仍會停止新交易並要求對帳，以避免重複下單。

Live 帳本預設獨立存於 `data/live_pair_trading.sqlite3`，啟動時只從 paper DB 複製已完成市場的 spread/RV10 校準摘要，不複製 paper 交易。若要下單，請先確認帳戶、token、資金及環境變數，再使用 `python run.py live --execution-mode live`；不要用 `demo` 模式下單。程式不會在本次修改中啟動或送出任何訂單。

## 策略與模擬成交

- Spread = Polymarket UP mid − Binance UP mid；以五個 60 秒 event-time bucket 分組。
- OU score 使用每個已結束市場、每個 bucket 一筆 spread median，rolling window 最近 100 個市場。Bucket 0 使用一般 z-score；其他 bucket 以這 100 筆估計 AR(1) 平穩均值和標準差，無法估計時退回 z-score。進場 bucket 至少要有 100 筆 OU 歷史。RV10 q90 取同 bucket 最近 100 個已結束市場的 market-median RV10，必須累積滿 100 個才計算。資料庫每個 bucket 各保留最近 100 筆 spread 與 RV10 校準摘要。
- 僅在 `|OU score| > 3` 且即時 `RV10 > 歷史同 bucket q90` 時進場；OU score 為負時只買 Polymarket UP，為正時只買 Polymarket DOWN。Binance 行情仍參與 spread、OU 與 RV10 計算，但不建立 Binance 部位。RV10 是最近 10 秒價差相鄰變動平方和開根號，跨 bucket 重設。每個 matched market 最多進場一次。
- 每筆 Polymarket 訂單至少 5 shares。訊號觸發後固定延遲 330ms 到達；首次 IOC 使用到達當下記憶體中最新、已收到且未過期的 Polymarket WebSocket book，不等下一筆報價，逐檔消耗可見 ask 深度。部分成交的 IOC 餘額視為取消，實際成交數量成為持倉。
- 市場進入最後 60 秒時不開新倉。OU score 沿進場方向回到 ±1.5 時觸發出場，最多持有 60 秒；出場訊號觸發後，模擬 330ms 延遲平倉，按到達當下記憶體報價的可見 bid 深度逐檔成交；部分退出會等新的報價再以新訂單延遲重試。進出場都計 taker fee，預設每份 `0.07 * p * (1-p)`。
- Gamma 與 Binance market metadata 仍由 REST 更新；Polymarket CLOB 與 Binance Predict 的 L2 order book 改由 WebSocket 推送。Order book 留在記憶體中，不逐次寫入 SQLite。訊號成立時保存一筆交易決策；Polymarket 紙上進出場的送單、成交或未成交、數量與 VWAP、費用和損益寫入交易及事件表。程式另記錄 WebSocket 市場訂閱及 run 起訖事件。

Binance 目前的二元市場 metadata 會提供一個 `marketId` 與 UP/DOWN 兩個 outcome token；orderbook WebSocket payload 只帶 `marketId`，不帶 `tokenId`。程式以該 stream 的 UP 簿作為基準，依二元合約互補關係推導 DOWN 簿：DOWN bids = `1 - UP asks`、DOWN asks = `1 - UP bids`，保留每檔數量。DOWN 因此是從同一份 Binance stream 推導，並非第二份獨立行情簿。

## 延遲量測與限制

Order book 透過 WebSocket 接收：Polymarket 先用完整簿初始化，再將價格檔增量套用到本地簿；Binance 推送完整簿，並依來源時間戳忽略較舊更新。行情快照由更新事件觸發，短暫合併密集事件；市場 metadata 仍每 10 秒用 REST 更新。source age 在記憶體中用於 freshness 判斷，並附在實際紙上訂單事件；不把整份 order book 或每次行情快照存進資料庫。WebSocket feed 不提供單次 order-book request RTT。

由於完全不送訂單，真實「送單網路延遲」與「交易 API 接收/ACK 延遲」無從量測。預設每腿從模擬送單到抵達固定 330ms、jitter=0；這是情境假設，不是平台實測值。可用 `.env` 調整延遲參數。

Shadow 比「訊號當下立即按 mid/top-of-book 成交」更嚴格，但仍不等同實盤：WebSocket 斷線期間不會有可用行情，重連後需等新簿初始化；可見深度仍可能在訂單抵達前撤掉；模擬會立即吃可見深度、不排隊，並不包含真實 ACK/拒單、rate limit 或帳戶實際費率級距。Live 雖採用真實 FAK 回報與官方市場結算結果，仍可能因市場解析事件延遲、錢包餘額/授權、鏈上 redeem 與資料源中斷而需要人工核對；本機帳本結算不代表鏈上贖回已完成。跨平台相同時間區間也不代表結算規則相同，shadow PnL 不是實盤獲利證明。

## 參考來源

- [chester-tsai/polymarket_trade_project，`feat/strategy-integration`（commit `152db7ee`）](https://github.com/chester-tsai/polymarket_trade_project/tree/feat/strategy-integration)：BTC 5m 市場探索、UP/DOWN token 對應與 order-book 讀取模式。
- [Polymarket CLOB WebSocket market channel](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)：asset 訂閱、完整簿初始化與價格檔更新格式。
- [Binance Prediction Market Orderbook WebSocket Push API](https://developers.binance.com/en/docs/products/w3w-prediction/websocket-api/orderbook)：簽名 topic 格式、完整簿 payload 與更新時間戳。

此 Demo 僅重做市場 metadata 與 order-book 資料路徑；不含現貨 feed、錢包、真實下單、結算自動化或來源 repo 的其他策略/模型。

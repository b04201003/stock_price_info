import os
from dotenv import load_dotenv
import websocket
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import pytz
import time
import logging
import signal
import sys
from time import sleep
from random import uniform


# 在全域變數區塊加入
MAX_RETRIES = 3
RETRY_DELAY = 60  # 基本重試延遲時間（秒）
last_connection_attempt = 0
connection_attempts = 0
# Load environment variables
load_dotenv()

# 設定日誌
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 在環境變數驗證部分加入更詳細的日誌
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

# 詳細記錄環境變數狀態
logger.info("檢查環境變數設定...")
logger.info(f"FINNHUB_API_KEY 是否存在: {bool(FINNHUB_API_KEY)}")
logger.info(f"SLACK_BOT_TOKEN 是否存在: {bool(SLACK_BOT_TOKEN)}")
logger.info(f"SLACK_CHANNEL 是否存在: {bool(SLACK_CHANNEL)}")

if not all([FINNHUB_API_KEY, SLACK_BOT_TOKEN, SLACK_CHANNEL]):
    logger.error("環境變數缺失:")
    if not FINNHUB_API_KEY:
        logger.error("- FINNHUB_API_KEY 未設定")
    if not SLACK_BOT_TOKEN:
        logger.error("- SLACK_BOT_TOKEN 未設定")
    if not SLACK_CHANNEL:
        logger.error("- SLACK_CHANNEL 未設定")
    sys.exit(1)

# 初始化 Slack 客戶端
slack_client = WebClient(token=SLACK_BOT_TOKEN)

# 全域變數控制退出和數據收集
running = True
ws_instance = None
batch_data = []  # 儲存每三分鐘的報價數據
last_sent_time = time.time()  # 上次傳送時間


# 美股開盤時間檢查
def is_market_open():
    et_tz = pytz.timezone("US/Eastern")
    now = datetime.now(et_tz)
    weekday = now.weekday()  # 0=星期一, 6=星期日
    if weekday >= 5:  # 週六或週日
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close


# 傳送到 Slack
def send_to_slack(message):
    try:
        slack_client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
        logger.info(f"訊息已傳送到 Slack: {message}")
    except SlackApiError as e:
        logger.error(f"傳送到 Slack 失敗: {e.response['error']}")


# 計算 OHLC 和交易量並傳送
def process_batch_and_send():
    global batch_data, last_sent_time
    current_time = time.time()
    time_range_start = datetime.fromtimestamp(last_sent_time).strftime("%H:%M:%S")
    time_range_end = datetime.fromtimestamp(current_time).strftime("%H:%M:%S")

    if not batch_data:
        # 若三分鐘內無交易
        msg = (
            f"QQQ 3分鐘彙總:\n"
            f"起始時間: {time_range_start} ET\n"
            f"結束時間: {time_range_end} ET\n"
            f"該時段無交易"
        )
        send_to_slack(msg)
    else:
        # 有交易時計算 OHLC 和交易量
        prices = [item["p"] for item in batch_data]
        volumes = [item.get("v", 0) for item in batch_data]  # 若無成交量則為 0
        timestamps = [item["t"] / 1000 for item in batch_data]  # 轉為秒

        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        total_volume = sum(volumes)

        first_time = datetime.fromtimestamp(timestamps[0]).strftime("%H:%M:%S")
        last_time = datetime.fromtimestamp(timestamps[-1]).strftime("%H:%M:%S")

        msg = (
            f"QQQ 3分鐘彙總:\n"
            f"起始時間: {first_time} ET\n"
            f"結束時間: {last_time} ET\n"
            f"開: {open_price:.2f}, 高: {high_price:.2f}, 低: {low_price:.2f}, 收: {close_price:.2f}\n"
            f"總交易量: {total_volume}"
        )
        send_to_slack(msg)

    # 重置批次數據
    batch_data = []
    last_sent_time = current_time


# WebSocket 回調函數
def on_message(ws, message):
    global batch_data, last_sent_time
    data = json.loads(message)
    if "data" in data:
        for item in data["data"]:
            batch_data.append(item)  # 收集每筆報價

        # 每三分鐘（180秒）處理一次
        current_time = time.time()
        if current_time - last_sent_time >= 180:
            process_batch_and_send()


# 修改 on_error 處理函數
def on_error(ws, error):
    global connection_attempts
    if "429" in str(error):
        logger.error(f"達到 API 請求限制，等待重試...")
        connection_attempts += 1
    else:
        logger.error(f"WebSocket 錯誤: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket 關閉: {close_status_code} - {close_msg}")


# 修改 on_open 處理函數
def on_open(ws):
    global connection_attempts
    connection_attempts = 0  # 重置重試計數
    logger.info("WebSocket 連線成功，訂閱 QQQ...")
    ws.send('{"type":"subscribe","symbol":"QQQ"}')


# 處理退出信號
def signal_handler(sig, frame):
    global running, ws_instance
    logger.info("收到退出信號，正在關閉...")
    running = False
    if ws_instance:
        ws_instance.close()  # 平滑關閉 WebSocket
    if batch_data or (time.time() - last_sent_time >= 180):  # 若有數據或超過三分鐘
        process_batch_and_send()
    sys.exit(0)


# WebSocket 運行函數
def run_websocket():
    global ws_instance, last_connection_attempt, connection_attempts

    while running:
        try:
            current_time = time.time()
            time_since_last_attempt = current_time - last_connection_attempt

            # 檢查重試頻率
            if time_since_last_attempt < RETRY_DELAY:
                sleep_time = RETRY_DELAY - time_since_last_attempt
                logger.info(f"等待 {sleep_time:.0f} 秒後重試...")
                time.sleep(sleep_time)

            # 指數退避重試
            if connection_attempts > 0:
                backoff = min(
                    300, RETRY_DELAY * (2 ** (connection_attempts - 1))
                )  # 最多等待 5 分鐘
                jitter = uniform(0, 0.1 * backoff)  # 加入隨機延遲
                logger.info(
                    f"第 {connection_attempts} 次重試，等待 {backoff + jitter:.1f} 秒..."
                )
                time.sleep(backoff + jitter)

            last_connection_attempt = time.time()
            connection_attempts += 1

            ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
            logger.info(f"嘗試建立 WebSocket 連線 (嘗試次數: {connection_attempts})")

            ws_instance = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws_instance.run_forever()

            # 如果連線成功，重置嘗試次數
            connection_attempts = 0

        except Exception as e:
            if not running:
                break

            logger.error(f"WebSocket 異常: {e}")

            if connection_attempts >= MAX_RETRIES:
                logger.error(f"重試次數超過上限 ({MAX_RETRIES})，等待較長時間後重試...")
                time.sleep(300)  # 等待 5 分鐘
                connection_attempts = 0  # 重置重試次數
            else:
                logger.info(f"5 秒後重試... (嘗試次數: {connection_attempts})")
                time.sleep(5)


# 主程式
if __name__ == "__main__":
    # 註冊信號處理器
    signal.signal(signal.SIGINT, signal_handler)  # 捕捉 Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 捕捉終止信號

    logger.info("程式啟動...")
    while running:
        if is_market_open():
            logger.info("市場開盤，啟動 WebSocket...")
            run_websocket()  # 在開盤時間運行 WebSocket
        else:
            logger.info("市場關閉，等待中...")
            # 在市場關閉時仍檢查是否需傳送「無交易」訊息
            current_time = time.time()
            if current_time - last_sent_time >= 180:
                process_batch_and_send()
            time.sleep(60)  # 非開盤時間每分鐘檢查一次

    logger.info("程式已關閉")

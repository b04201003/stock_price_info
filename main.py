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


def on_error(ws, error):
    logger.error(f"WebSocket 錯誤: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket 關閉: {close_status_code} - {close_msg}")


def on_open(ws):
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
    global ws_instance
    while running:
        try:
            ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
            ws_instance = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws_instance.run_forever()
        except Exception as e:
            if running:  # 只有在程式未主動退出時才重連
                logger.error(f"WebSocket 異常: {e}，5 秒後重連...")
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

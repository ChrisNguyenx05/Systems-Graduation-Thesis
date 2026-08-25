import asyncio
import json
import requests
import websockets
import os
from decimal import Decimal
from datetime import datetime

# Lấy thư mục hiện tại của file test_api.py để lưu file cùng cấp
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
KLINES_FILE_PATH = os.path.join(CURRENT_DIR, "rest_klines_sample.json")
DEPTH_FILE_PATH = os.path.join(CURRENT_DIR, "rest_depth_sample.json")
TRADES_FILE_PATH = os.path.join(CURRENT_DIR, "websocket_trades_sample.json")
WS_DEPTH_FILE_PATH = os.path.join(CURRENT_DIR, "websocket_depth_sample.json")
LIQUIDATIONS_FILE_PATH = os.path.join(CURRENT_DIR, "websocket_liquidations_sample.json")

# ==============================================================================
# PHẦN 1: TEST REST API (HTTP) & LƯU LẠI FILE LOCAL
# ==============================================================================
def test_rest_api():
    print("=== PHẦN 1: KIỂM TRA & LƯU DỮ LIỆU BINANCE REST API (HTTP) ===")
    base_url = "https://api.binance.com"
    
    # 1. Test Ping kết nối
    ping_url = f"{base_url}/api/v3/ping"
    print(f"1. Đang gửi request Ping tới: {ping_url}...")
    try:
        response = requests.get(ping_url, timeout=5)
        if response.status_code == 200:
            print("   -> Ping thành công! Kết nối mạng tới Binance tốt.")
        else:
            print(f"   -> Thất bại. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"   -> Lỗi kết nối: {e}")
        return False

    # 2. Test lấy dữ liệu nến (Klines) lịch sử của BTCUSDT (3 nến 1 phút gần nhất)
    klines_url = f"{base_url}/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 3
    }
    print(f"\n2. Đang lấy 3 nến 1 phút gần nhất từ: {klines_url}...")
    try:
        response = requests.get(klines_url, params=params, timeout=5)
        if response.status_code == 200:
            klines = response.json()
            print("   -> Lấy nến thành công!")
            
            # Lưu dữ liệu nến thô dạng JSON vào thư mục local
            with open(KLINES_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(klines, f, indent=4)
            print(f"   [LƯU FILE]: Đã lưu dữ liệu nến thô vào: {KLINES_FILE_PATH}")
            
            # Hiển thị cấu trúc mẫu
            for idx, kline in enumerate(klines):
                open_time = datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"   * Nến {idx+1} ({open_time}): Mở={kline[1]} | Cao={kline[2]} | Thấp={kline[3]} | Đóng={kline[4]} | Vol={kline[5]} BTC")
        else:
            print(f"   -> Thất bại. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"   -> Lỗi khi lấy nến: {e}")
        return False
    
    # 3. Test lấy dữ liệu Sổ lệnh (Order Book Depth) của BTCUSDT (Top 5 levels)
    depth_url = f"{base_url}/api/v3/depth"
    depth_params = {
        "symbol": "BTCUSDT",
        "limit": 5
    }
    print(f"\n3. Đang lấy thông tin Sổ lệnh (Depth) từ: {depth_url}...")
    try:
        response = requests.get(depth_url, params=depth_params, timeout=5)
        if response.status_code == 200:
            depth = response.json()
            print("   -> Lấy sổ lệnh thành công!")
            
            # Lưu dữ liệu sổ lệnh dạng JSON vào thư mục local
            with open(DEPTH_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(depth, f, indent=4)
            print(f"   [LƯU FILE]: Đã lưu dữ liệu sổ lệnh vào: {DEPTH_FILE_PATH}")
            
            # Hiển thị mức giá mua/bán tốt nhất
            print(f"   * Giá Bid tốt nhất: {depth['bids'][0][0]} (SL: {depth['bids'][0][1]})")
            print(f"   * Giá Ask tốt nhất: {depth['asks'][0][0]} (SL: {depth['asks'][0][1]})")
        else:
            print(f"   -> Thất bại. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"   -> Lỗi khi lấy sổ lệnh: {e}")
        return False
    
    print("================================================\n")
    return True

# ==============================================================================
# PHẦN 2: TEST WEBSOCKET & LƯU LẠI FILE LOCAL (TRADES, DEPTH, LIQUIDATIONS)
# ==============================================================================
async def test_websocket_trades():
    print("=== PHẦN 2.1: KIỂM TRA & LƯU DỮ LIỆU GIAO DỊCH REALTIME (aggTrade) ===")
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    print(f"Đang kết nối WebSocket tới: {url}...")
    
    trades_sample_list = []
    try:
        async with websockets.connect(url) as websocket:
            print("Kết nối thành công! Đang đợi 20 giao dịch realtime để ghi file...\n")
            
            for i in range(1, 21):
                raw_message = await websocket.recv()
                data = json.loads(raw_message)
                
                # Thêm vào danh sách để lưu file
                trades_sample_list.append(data)
                
                # Trích xuất để in console
                event_time_ms = data.get("E")
                event_time = datetime.fromtimestamp(event_time_ms / 1000).strftime('%H:%M:%S.%f')[:-3]
                trade_id = data.get("a")
                price = Decimal(data.get("p"))
                quantity = Decimal(data.get("q"))
                is_buyer_maker = data.get("m")
                side = "SELL" if is_buyer_maker else "BUY"
                
                print(f"[{i}/20] [{event_time}] ID: {trade_id} | {side} | Giá: {price} | SL: {quantity}")
                
            # Lưu 20 giao dịch thô dạng JSON vào thư mục local
            with open(TRADES_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(trades_sample_list, f, indent=4)
            
            print(f"\n[LƯU FILE]: Đã lưu 20 giao dịch thô vào: {TRADES_FILE_PATH}")
            
    except Exception as e:
        print(f"Lỗi WebSocket Trades: {e}")
    print("=========================================================\n")


async def test_websocket_depth():
    print("=== PHẦN 2.2: KIỂM TRA & LƯU DỮ LIỆU SỔ LỆNH REALTIME (depth5@100ms) ===")
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"
    print(f"Đang kết nối WebSocket tới: {url}...")
    
    depth_sample_list = []
    try:
        async with websockets.connect(url) as websocket:
            print("Kết nối thành công! Đang đợi 5 cập nhật sổ lệnh...\n")
            
            for i in range(1, 6):
                raw_message = await websocket.recv()
                data = json.loads(raw_message)
                
                # Thêm vào danh sách để lưu file
                depth_sample_list.append(data)
                
                # Trích xuất giá bid/ask tốt nhất
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                best_bid = bids[0][0] if bids else "N/A"
                best_ask = asks[0][0] if asks else "N/A"
                
                print(f"[{i}/5] Cập nhật Sổ lệnh - Bid tốt nhất: {best_bid} | Ask tốt nhất: {best_ask}")
                
            # Lưu 5 cập nhật sổ lệnh dạng JSON vào thư mục local
            with open(WS_DEPTH_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(depth_sample_list, f, indent=4)
            
            print(f"\n[LƯU FILE]: Đã lưu 5 cập nhật sổ lệnh vào: {WS_DEPTH_FILE_PATH}")
            
    except Exception as e:
        print(f"Lỗi WebSocket Depth: {e}")
    print("=========================================================\n")


async def test_websocket_futures_liquidations():
    print("=== PHẦN 2.3: KIỂM TRA LỆNH THANH LÝ REALTIME TRÊN FUTURES (forceOrder) ===")
    # Dùng Binance Futures WebSocket Stream công khai
    url = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
    print(f"Đang kết nối WebSocket tới: {url}...")
    
    liquidation_list = []
    try:
        # Sử dụng wait_for với timeout để tránh treo script khi không có lệnh thanh lý nào phát sinh
        async with websockets.connect(url) as websocket:
            print("Kết nối thành công! Đang đợi lệnh thanh lý realtime (Lắng nghe tối đa 8 giây)...\n")
            
            while len(liquidation_list) < 3:
                try:
                    # Chờ tin nhắn từ WebSocket với timeout 8 giây
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=8.0)
                    data = json.loads(raw_message)
                    liquidation_list.append(data)
                    
                    order_data = data.get("o", {})
                    symbol = order_data.get("s")
                    side = order_data.get("S")
                    price = order_data.get("p")
                    quantity = order_data.get("q")
                    print(f"[THANH LÝ] {symbol} | Phe {side} bị cháy | Giá: {price} | SL: {quantity}")
                except asyncio.TimeoutError:
                    print("   -> Không có lệnh thanh lý (Liquidation) nào phát sinh trong 8 giây qua (thị trường ổn định).")
                    break
            
            if liquidation_list:
                # Lưu các lệnh thanh lý thô dạng JSON vào thư mục local
                with open(LIQUIDATIONS_FILE_PATH, "w", encoding="utf-8") as f:
                    json.dump(liquidation_list, f, indent=4)
                print(f"\n[LƯU FILE]: Đã lưu các lệnh thanh lý mẫu vào: {LIQUIDATIONS_FILE_PATH}")
                
    except Exception as e:
        print(f"Lỗi WebSocket Futures Liquidations: {e}")
    print("=========================================================")

# ==============================================================================
# HÀM CHẠY CHÍNH
# ==============================================================================
async def main():
    # 1. Chạy test REST API (đồng bộ)
    rest_ok = test_rest_api()
    
    # 2. Nếu REST API kết nối tốt, tiếp tục test các luồng WebSocket (bất đồng bộ)
    if rest_ok:
        await test_websocket_trades()
        await test_websocket_depth()
        await test_websocket_futures_liquidations()

if __name__ == "__main__":
    asyncio.run(main())

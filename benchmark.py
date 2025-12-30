import threading
import time
import json
import random
import http.client
from urllib.parse import urlparse

# --- AYARLAR ---
# Sunucun 3002 portunda çalıştığı için burayı güncelledik
TARGET_URL = "http://127.0.0.1:3002/ingest"
NUM_THREADS = 50       # Aynı anda saldıracak işçi sayısı
DURATION = 10          # Test süresi (saniye)

# Global sayaçlar (Thread-safe olması için kilit kullanacağız)
success_count = 0
fail_count = 0
is_running = True
lock = threading.Lock()

def send_request():
    global success_count, fail_count
    
    # URL'i parçalarına ayır (hostname, port, path)
    url_parts = urlparse(TARGET_URL)
    headers = {"Content-Type": "application/json"}
    
    # Bağlantıyı döngü dışında aç (Keep-Alive)
    conn = http.client.HTTPConnection(url_parts.hostname, url_parts.port)

    while is_running:
        try:
            # Rastgele veri oluştur
            payload = json.dumps([{
                "level": random.choice(["info", "error", "debug"]),
                "message": "Stress test log entry - Rust vs Python",
                "user_id": random.randint(1, 10000),
                "extra_data": "x" * 50 # Biraz yük olsun
            }])
            
            # http.client, 'requests' kütüphanesinden daha hızlıdır (benchmark için ideal)
            conn.request("POST", url_parts.path, payload, headers)
            response = conn.getresponse()
            response.read() # Cevabı oku ve buffer'ı temizle
            
            # 200-299 arası başarılı sayılır
            if 200 <= response.status < 300:
                with lock:
                    success_count += 1
            else:
                with lock:
                    fail_count += 1
        except Exception as e:
            # Hata durumunda bağlantıyı yenile
            try:
                conn.close()
            except:
                pass
            conn = http.client.HTTPConnection(url_parts.hostname, url_parts.port)
            with lock:
                fail_count += 1

def main():
    global is_running
    print(f"\n🚀 STRESS TESTİ BAŞLIYOR: {TARGET_URL}")
    print(f"🧵 Thread Sayısı : {NUM_THREADS}")
    print(f"⏱️  Süre          : {DURATION} saniye")
    print("-" * 50)
    print("Saldırı başladı... Lütfen bekleyin...")

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=send_request)
        t.daemon = True
        t.start()
        threads.append(t)

    # Belirlenen süre kadar bekle
    time.sleep(DURATION)
    is_running = False
    time.sleep(1) # Threadlerin durması için kısa bir mola

    # Sonuçları hesapla
    rps = success_count / DURATION

    print("\n" + "=" * 50)
    print("📊 SONUÇLAR (GÖVDE GÖSTERİSİ)")
    print("=" * 50)
    print(f"✅ Toplam Başarılı İstek : {success_count:,}")
    print(f"❌ Başarısız İstek        : {fail_count}")
    print(f"⚡ RPS (İstek/Saniye)     : {rps:,.2f}")
    print("=" * 50)
    print("Rust ve AVX2'nin gücü adına! 💪\n")

if __name__ == "__main__":
    main()
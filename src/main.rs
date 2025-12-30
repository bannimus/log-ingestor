use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool};
use tracing::{debug, info};

// --- 1. Veri Modeli ---
// Gelen JSON verisini karşılayacak yapı.
#[derive(Debug, Deserialize, Serialize)]
struct LogEntry {
    level: String,
    message: String,
    // Gelen JSON'da tanımlamadığımız diğer tüm alanları 'extra' içine atar.
    // Böylece veri kaybı olmaz.
    #[serde(flatten)]
    extra: serde_json::Value,
}

// --- 2. Uygulama Durumu (State) ---
// Axum handler'ları arasında veri paylaşmak için kullanılır.
// Kanalın gönderici ucunu (Sender) burada tutuyoruz.
#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<LogEntry>,
}

#[tokio::main]
async fn main() {
    // Loglamayı başlat (Konsola bilgi basmak için)
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // --- 3. MPSC Kanalı Kurulumu ---
    // tx: Transmitter (Gönderici), rx: Receiver (Alıcı)
    // 10.000 kapasiteli bir kanal açıyoruz.
    let (tx, mut rx) = mpsc::channel::<LogEntry>(10000);

    // --- 4. Veritabanı Kurulumu (SQLite) ---
    // WAL Modu (Write-Ahead Logging) performansı artırır.
    let db_options = SqliteConnectOptions::new()
        .filename("logs.db")
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);

    let pool = SqlitePool::connect_with(db_options)
        .await
        .expect("Veritabanına bağlanılamadı");

    // Tabloyu oluştur (Yoksa)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            details TEXT
        )",
    )
    .execute(&pool)
    .await
    .expect("Tablo oluşturulamadı");

    // --- 5. Arka Plan Veritabanı Yazıcısı (Consumer) ---
    // Bu görev (task) ana sunucudan bağımsız, ayrı bir thread gibi çalışır.
    let writer_task = tokio::spawn(async move {
        // Kanal açık olduğu sürece gelen verileri al
        while let Some(log) = rx.recv().await {
            debug!("💾 DB'ye yazılıyor: {}", log.message);
            
            // Timestamp'i extra alanından çek (ingest_handler eklemişti)
            let timestamp = log.extra.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");

            // Geri kalan veriyi JSON string'e çevir (details sütunu için)
            let details = serde_json::to_string(&log.extra).unwrap_or_default();

            // SQL Insert
            let _ = sqlx::query("INSERT INTO logs (level, message, timestamp, details) VALUES (?, ?, ?, ?)")
                .bind(&log.level)
                .bind(&log.message)
                .bind(timestamp)
                .bind(details)
                .execute(&pool)
                .await;
        }
        // Veritabanı bağlantı havuzu (pool) otomatik kapanır.
    });

    // --- 6. Sunucu Ayarları ---
    let state = AppState { tx };

    let app = Router::new()
        .route("/ingest", post(ingest_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3002").await.unwrap();
    info!("🚀 Log Ingestion Sunucusu 3002 portunda çalışıyor...");
    
    // Graceful Shutdown ile sunucuyu başlat
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    // Sunucu durduğunda, arka plandaki yazıcının işini bitirmesini bekle
    let _ = writer_task.await;
    info!("✅ Tüm loglar diske yazıldı ve sunucu güvenle kapandı.");
}

// CTRL+C sinyalini dinleyen yardımcı fonksiyon
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("🛑 Kapatma sinyali alındı (CTRL+C). İstekler durduruluyor...");
}

// --- 6. Request Handler (Producer) ---
// HTTP isteğini karşılar, filtreler ve kanala atar.
// Dosya yazma işlemini beklemez, hemen cevap döner.
async fn ingest_handler(
    State(state): State<AppState>,
    Json(payload): Json<Vec<LogEntry>>, // Batch (dizi) olarak log kabul eder
) -> StatusCode {
    
    debug!("📥 İstek alındı: {} adet log", payload.len());
    for mut log in payload {
        // Sadece "error" seviyesindeki logları filtrele
        if log.level == "error" {
            // Eğer 'timestamp' alanı yoksa, şu anki UTC zamanını ekle
            if let serde_json::Value::Object(ref mut map) = log.extra {
                if !map.contains_key("timestamp") {
                    let now = chrono::Utc::now().to_rfc3339();
                    map.insert("timestamp".to_string(), serde_json::Value::String(now));
                }
            }
            debug!("✅ Hata logu tespit edildi, kanala gönderiliyor...");
            // Kanala gönder.
            // await kullanıyoruz ama bu işlem sadece belleğe yazdığı için nanosaniyeler sürer.
            // Eğer kanal doluysa (10.000 log birikmişse) burada bekler (Backpressure).
            let _ = state.tx.send(log).await;
        } else {
            debug!("ℹ️ Log seviyesi '{}', filtrelendi.", log.level);
        }
    }

    // İstemciye "Kabul Edildi" (202 Accepted) dönüyoruz.
    StatusCode::ACCEPTED
}

import time
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- 🛠️ MEKANISME RETRY UNTUK KONEKSI KAFKA BROKER ---
consumer = None
while True:
    try:
        print("⏳ Mencoba menghubungkan ke Kafka Broker...")
        consumer = KafkaConsumer(
            'weather_topic',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='latest',  # Membaca data terbaru yang masuk
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ Berhasil terhubung ke Kafka!")
        break  # Keluar dari loop jika berhasil terkoneksi

    except NoBrokersAvailable:
        print("❌ Kafka Broker belum siap. Mencoba lagi dalam 5 detik...")
        time.sleep(5)
# -----------------------------------------------------

print("📥 Consumer siap menerima data dari Kafka...")

# --- 🔄 LOOP STREAMING DATA UTAMA ---
try:
    for message in consumer:
        data = message.value
        
        # --- PROSES DATA SAINS SEDERHANA ---
        # Menambahkan status berdasarkan nilai AQI (Air Quality Index)
        if data['aqi'] <= 50:
            data['status'] = 'Baik'
        elif data['aqi'] <= 100:
            data['status'] = 'Sedang'
        else:
            data['status'] = 'Tidak Sehat'
            
        print(f"[{data['timestamp']}] {data['city']}: {data['temperature']}°C, AQI: {data['aqi']} ({data['status']})")
except KeyboardInterrupt:
    print("\n🛑 Consumer dihentikan oleh pengguna.")
    
finally:
    if consumer:
        consumer.close()
        print("🔌 Koneksi Consumer ke Kafka berhasil ditutup.")
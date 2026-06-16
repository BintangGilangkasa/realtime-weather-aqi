import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC_NAME = 'weather_topic'

# --- 🛠️ MEKANISME RETRY UNTUK KONEKSI KAFKA BROKER ---
producer = None
while True:
    try:
        print("⏳ Mencoba menghubungkan ke Kafka Broker...")
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Berhasil terhubung ke Kafka!")
        break  # Keluar dari loop inisialisasi jika berhasil terkoneksi
    
    except NoBrokersAvailable:
        print("❌ Kafka Broker belum siap. Mencoba lagi dalam 5 detik...")
        time.sleep(5)
# -----------------------------------------------------

def get_live_data():
    cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 'Yogyakarta']
    selected_city = random.choice(cities)
    
    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'city': selected_city,
        'temperature': round(random.uniform(24.0, 34.0), 1),
        'aqi': random.randint(30, 160)  # Kualitas udara
    }

print("🚀 Producer mulai mengirim data... Tekan Ctrl+C untuk berhenti.")

# --- 🔄 LOOP STREAMING DATA UTAMA ---
try:
    while True:
        data = get_live_data()
        
        # Kirim data ke Kafka
        producer.send(TOPIC_NAME, value=data)
        print(f"Sent to Kafka: {data}")
        
        # Jeda waktu pengiriman tiap 3 detik
        time.sleep(3)
except KeyboardInterrupt:
    print("\n🛑 Producer dihentikan oleh pengguna.")
finally:
    if producer:
        producer.close()
        print("🔌 Koneksi Producer ke Kafka berhasil ditutup.")
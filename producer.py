import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'weather_topic'

def get_live_data():
    # TIPS UAS: Jika API key belum aktif, gunakan data simulasi/tiruan dulu agar logika jalan
    cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang']
    selected_city = random.choice(cities)
    
    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'city': selected_city,
        'temperature': round(random.uniform(24.0, 34.0), 1),
        'aqi': random.randint(30, 160) # Kualitas udara
    }

print("🚀 Producer mulai mengirim data... Tekan Ctrl+C untuk berhenti.")

try:
    while True:
        data = get_live_data()
        
        # Kirim data ke Kafka
        producer.send(TOPIC_NAME, value=data)
        print(send_msg := f"Sent to Kafka: {data}")
        
        # Jeda waktu pengiriman (misal tiap 3 detik)
        time.sleep(3)
except KeyboardInterrupt:
    print("\n🛑 Producer dihentikan.")
finally:
    producer.close()
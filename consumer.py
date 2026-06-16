import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'weather_topic',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='latest', # Membaca data terbaru yang masuk
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📥 Consumer siap menerima data dari Kafka...")

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
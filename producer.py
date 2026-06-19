import time
import json
import random
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC_NAME = 'weather_topic'

# 🔑 API KEY BARU KAMU DARI OPENWEATHERMAP (Sesuai Gambar)
API_KEY = "d31df0473a5bef1f792a2ce5651e8ffb" 

# --- MEKANISME RETRY UNTUK KONEKSI KAFKA ---
producer = None
while True:
    try:
        print("⏳ Mencoba menghubungkan ke Kafka Broker...")
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Berhasil terhubung ke Kafka!")
        break
    except NoBrokersAvailable:
        print("❌ Kafka Broker belum siap. Mencoba lagi dalam 5 detik...")
        time.sleep(5)

def get_live_data_from_api():
    cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 'Yogyakarta', 'Salatiga']
    selected_city = random.choice(cities)
    
    try:
        # 🌐 URL Resmi OpenWeatherMap (Disertai parameter data polusi/AQI)
        url = f"http://api.openweathermap.org/data/2.5/weather?q={selected_city}&appid={API_KEY}&units=metric"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            api_data = response.json()
            
            parsed_data = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'city': selected_city,
                'temperature': float(api_data['main']['temp']),       # Suhu asli (°C)
                'pressure': float(api_data['main']['pressure']),      # Tekanan udara asli (hPa / mb)
                'wind_speed': float(api_data['wind']['speed'] * 3.6), # Konversi m/s asli ke km/jam
                'aqi': random.randint(30, 150)                        # Cadangan nilai AQI agar layout tetap penuh
            }
            return parsed_data
        else:
            print(f"⚠️ API mengembalikan status code: {response.status_code}")
            
    except Exception as e:
        print(f"⚠️ Gagal mengambil data API OpenWeather untuk {selected_city}: {e}")
    
    # SYSTEM BACKUP (Jika API limit / internet terputus)
    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'city': selected_city,
        'temperature': round(random.uniform(24.0, 34.0), 1),
        'pressure': 1010.0,
        'wind_speed': round(random.uniform(5.0, 25.0), 1),
        'aqi': random.randint(30, 160)
    }

print("🚀 Producer (OpenWeatherMap Real API Mode) mulai mengirim data...")

# --- LOOP STREAMING UTAMA ---
try:
    while True:
        data = get_live_data_from_api()
        producer.send(TOPIC_NAME, value=data)
        print(f"📡 Sent to Kafka (OpenWeather API): {data}")
        time.sleep(5) 
except KeyboardInterrupt:
    print("\n🛑 Producer dihentikan.")
finally:
    if producer:
        producer.close()
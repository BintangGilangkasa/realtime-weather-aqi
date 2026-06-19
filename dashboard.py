import time
import streamlit as st
import pandas as pd
import plotly.express as px
import json
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
from kafka.errors import NoBrokersAvailable

st.set_page_config(page_title="Real-Time Data Streaming", layout="wide")
st.title("📊 Real-Time Weather & AQI Dashboard")

# 🔄 Pemicu refresh otomatis setiap 2 detik (2000 milidetik)
# Ini menggantikan 'for message in consumer' agar tidak mengunci generator Python
st_autorefresh(interval=2000, key="datarefresh")

# 1. Inisialisasi Kafka Consumer (Gunakan cache agar tidak membuat koneksi baru terus)
@st.cache_resource
def get_kafka_consumer():
    try:
        custome_obj = KafkaConsumer(
            'weather_topic',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # Ditambahkan timeout agar .poll() tidak nge-hang jika data kosong
            consumer_timeout_ms=1000 
    )
        return custome_obj
    except NoBrokersAvailable:
        st.error("❌ Kafka Broker belum siap. Pastikan producer.py sudah dijalankan dan Kafka Broker aktif.")
        time.sleep(5)

consumer = get_kafka_consumer()

# 2. Inisialisasi Storage di Memori Streamlit
if 'data_history' not in st.session_state:
    st.session_state.data_history = []

# --- 📊 SIDEBAR UNTUK FILTER KOTA ---
st.sidebar.header("⚙️ Pengaturan Filter")

list_kota = ['Semua Kota', 'Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Yogyakarta', 'Semarang', 'Salatiga']
kota_terpilih = st.sidebar.selectbox("Pilih Kota yang Ingin Dipantau:", list_kota)

if st.sidebar.button("🗑️ Bersihkan Riwayat Data"):
    st.session_state.data_history = []
    st.meta.clear()
    st.rerun()
# -------------------------------------

# --- 📥 AMBIL DATA DARI KAFKA MENGGUNAKAN POLL ---
# --- 📥 AMBIL DATA DARI KAFKA MENGGUNAKAN POLL ---
raw_messages = consumer.poll(timeout_ms=500)

for topic_partition, messages in raw_messages.items():
    for message in messages:
        new_data = message.value
        
        # 💡 PERBAIKAN: Hitung status AQI secara mandiri di dashboard karena producer tidak mengirimkannya
        if 'status' not in new_data:
            aqi = new_data.get('aqi', 0)
            if aqi <= 50:
                new_data['status'] = 'Baik'
            elif aqi <= 100:
                new_data['status'] = 'Sedang'
            else:
                new_data['status'] = 'Tidak Sehat'
                
        st.session_state.data_history.append(new_data)

# Batasi kapasitas memori agar tidak lag
if len(st.session_state.data_history) > 100:
    st.session_state.data_history = st.session_state.data_history[-100:]
# --------------------------------------------------

# 3. PROSES DAN FILTER DATA UNTUK VISUALISASI
if st.session_state.data_history:
    df_all = pd.DataFrame(st.session_state.data_history)
    
    # Terapkan filter kota terlebih dahulu
    if kota_terpilih == 'Semua Kota':
        df_filtered = df_all
    else:
        df_filtered = df_all[df_all['city'] == kota_terpilih]
    
    # 💡 PERBAIKAN LOGIKA: Ambil data_terbaru langsung dari baris terakhir df_filtered yang sudah tersaring
    if not df_filtered.empty:
        data_terbaru = df_filtered.iloc[-1].to_dict()
    else:
        data_terbaru = None

    # 4. RENDER RE-DESIGN VISUALISASI
    
    # A. Tampilkan Metrik jika datanya tersedia
    if data_terbaru:
        col1, col2, col3 = st.columns(3)
        col1.metric(
            label=f"Suhu Terbaru ({data_terbaru.get('city', 'N/A')})", 
            value=f"{data_terbaru.get('temperature', 0)} °C"
        )
        col2.metric(
            label="Air Quality Index (AQI)", 
            value=data_terbaru.get('aqi', 0)
        )
        col3.metric(
            label="Status Kualitas Udara", 
            value=data_terbaru.get('status', 'N/A')  # Sekarang status tidak akan N/A lagi!
        )
    else:
        st.warning(f"⚠️ Belum ada data streaming yang masuk untuk kota **{kota_terpilih}**.")
            
    # B. Tampilkan Grafik Line Chart
    if not df_filtered.empty:
        fig = px.line(
            df_filtered, 
            x='timestamp', 
            y='temperature', 
            color='city', 
            title=f"Tren Suhu Real-Time ({kota_terpilih})",
            markers=True
        )
        # Mempercantik tampilan layout chart plotly
        fig.update_layout(xaxis_title="Waktu", yaxis_title="Suhu (°C)")
        st.plotly_chart(fig, use_container_width=True)
            
        # C. Tampilkan Tabel Log
        st.write("📋 **Log Data Berjalan:**")
        st.dataframe(df_filtered.sort_values(by='timestamp', ascending=False), use_container_width=True)
else:
    st.info("⏳ Menunggu data masuk dari Kafka Producer... Pastikan producer.py sudah dijalankan.")
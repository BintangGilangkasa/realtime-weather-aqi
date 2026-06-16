# 📊 Real-Time Weather & AQI Data Streaming System using Apache Kafka

Project ini merupakan sistem pengolahan data streaming secara **real-time** untuk memantau suhu dan kualitas udara (**Air Quality Index / AQI**) di beberapa kota besar di Indonesia. Sistem dibangun menggunakan **Apache Kafka** sebagai event broker, komponen pengolahan data berbasis **Python**, dan visualisasi interaktif menggunakan **Streamlit**.

Proyek ini dibuat untuk memenuhi tugas **UAS Analisis Data Stream**.

---

## 🏗️ Arsitektur Sistem

Sistem berjalan secara terisolasi dan otomatis di dalam ekosistem Docker dengan alur sebagai berikut:

1. **Producer (`producer.py`)**

   * Mensimulasikan pengambilan data cuaca dan AQI secara berkala.
   * Mengirimkan data ke Kafka Broker melalui topic `weather_topic`.

2. **Kafka Broker (`kafka`)**

   * Mengelola antrean pesan (message queue) dari data streaming yang diterima.

3. **Consumer (`consumer.py`)**

   * Membaca data streaming dari Kafka secara kontinu.
   * Melakukan proses pengolahan dan analisis data sederhana.

4. **Dashboard (`dashboard.py`)**

   * Aplikasi Streamlit yang menampilkan data secara real-time.
   * Menyediakan filter kota serta visualisasi tren suhu dan kualitas udara.

---

## 🔄 Alur Data Streaming

```text
Producer
    ↓
Kafka Topic (weather_topic)
    ↓
Consumer
    ↓
Streamlit Dashboard
```

---

## 🛠️ Prasyarat (Prerequisites)

Pastikan perangkat Anda telah terinstal:

* Docker Desktop
* Browser modern (Google Chrome, Microsoft Edge, Mozilla Firefox, dll.)

> **Catatan:** Anda tidak perlu menginstal Python, Java, atau library lainnya secara manual karena seluruh environment sudah dikemas di dalam Docker.

---

## 🚀 Cara Menjalankan Project

### 1. Buka Terminal

Masuk ke direktori project:

```bash
cd path/to/mini-project-streaming
```

### 2. Jalankan Docker Compose

Bangun image dan jalankan seluruh layanan:

```bash
docker-compose up --build
```

Tunggu hingga proses selesai dan seluruh service berjalan dengan normal.

Service yang akan aktif:

* Kafka Broker
* Producer
* Consumer
* Streamlit Dashboard

---

### 3. Akses Dashboard

Buka browser dan kunjungi:

```text
http://localhost:8501
```

Dashboard akan menampilkan:

* 🌡️ Suhu terkini tiap kota
* 🌫️ Nilai AQI (Air Quality Index)
* 📈 Grafik tren data secara real-time
* 🔍 Filter kota pada sidebar

Kota yang tersedia antara lain:

* Jakarta
* Bandung
* Surabaya
* Semarang
* Yogyakarta
* dan kota lainnya

---

## 🛑 Menghentikan Sistem

Untuk menghentikan seluruh service:

1. Tekan:

```bash
Ctrl + C
```

## 📂 Struktur Project

```text
mini-project-streaming/
│
├── docker-compose.yml       # Konfigurasi orkestrasi container
├── Dockerfile               # Blueprint environment Python
├── requirements.txt         # Daftar dependency Python
├── producer.py              # Pengirim data streaming ke Kafka
├── consumer.py              # Pemroses data streaming
├── dashboard.py             # Dashboard visualisasi Streamlit
└── README.md                # Dokumentasi project
```

---

## 🧰 Teknologi yang Digunakan

| Teknologi      | Fungsi                |
| -------------- | --------------------- |
| Apache Kafka   | Message Broker        |
| Python         | Data Processing       |
| Streamlit      | Dashboard Visualisasi |
| Docker         | Containerization      |
| Docker Compose | Orchestration Service |

---

## 📊 Fitur Utama

✅ Simulasi data cuaca dan kualitas udara secara real-time

✅ Pengiriman data streaming menggunakan Apache Kafka

✅ Pemrosesan data secara kontinu menggunakan Consumer

✅ Dashboard interaktif berbasis Streamlit

✅ Filter data berdasarkan kota

✅ Visualisasi tren suhu dan AQI secara langsung

---

## 👨‍💻 Pengembang

Proyek ini dibuat untuk memenuhi tugas mata kuliah **Analisis Data Stream** pada Program Studi **Sains Data**.

**Universitas Islam Negeri Salatiga**

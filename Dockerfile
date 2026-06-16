# Gunakan base image Python yang ringan
FROM python:3.11-slim

# Set folder kerja di dalam kontainer Docker
WORKDIR /app

# Salin file requirements terlebih dahulu agar instalasi bisa di-cache oleh Docker
COPY requirements.txt .

# Install semua dependensi library Python
RUN pip install --no-cache-dir -r requirements.txt

# Salin seluruh kode project ke dalam kontainer
COPY . .

# Expose port Streamlit (default 8501)
EXPOSE 8501
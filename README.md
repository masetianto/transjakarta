
# Transjakarta Data Pipeline - Take Home Test

## Deskripsi
Pipeline ETL harian untuk mengolah data transaksi Transjakarta, dijadwalkan di Apache Airflow setiap jam 07:00.
Proyek ini bersifat demonstrasi dan sudah berisi contoh data dummy serta contoh output CSV.

## Struktur Folder
- `dags/` -> berisi DAG Airflow (`dag_datapelanggan.py`)
- `data/input/` -> file CSV sumber (5 file dummy)
- `data/intermediate/` -> file sementara yang dibuat selama ETL
- `data/output/` -> output agregasi (3 file CSV contoh)
- `docker-compose.yml` -> menjalankan PostgreSQL + Airflow (demo)

## Menjalankan (singkat)
1. Pastikan Docker & Docker Compose terpasang.
2. Salin seluruh folder proyek ke mesin yang memiliki Docker.
3. Jalankan:
   ```bash
   docker-compose up -d
   ```
4. Buka Airflow UI di `http://localhost:8080` (username/password: admin/admin).
5. Aktifkan DAG `dag_datapelanggan` dan trigger / biarkan dijadwalkan setiap 07:00.

> Catatan: image Airflow resmi memerlukan konfigurasi lebih detil untuk produksi. docker-compose.yml disederhanakan untuk kebutuhan take-home demo.


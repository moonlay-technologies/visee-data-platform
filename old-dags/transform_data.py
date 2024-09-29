from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
import csv

postgres_visee="postgresql+psycopg2://postgres:Standar123.@viseedb-dev.cbvmxai42qe0.ap-southeast-1.rds.amazonaws.com:5432/main"

# Buat koneksi ke database
engine = create_engine(postgres_visee, echo=True)  # Ganti dengan konfigurasi koneksi yang sesuai

# Definisi Base untuk declarative base class
Base = declarative_base()

# Definisi model untuk tabel activity
class Activity(Base):
    __tablename__ = 'viseetor_dwell'

    activity_date = Column(DateTime, primary_key=True)
    gender = Column(String)
    activity = Column(String)

# Buat session
Session = sessionmaker(bind=engine)
session = Session()

# Ambil data dari database
query = session.query(Activity).order_by(Activity.activity_date)
results = query.all()

# Inisialisasi variabel untuk menyimpan hasil pengolahan data
processed_data = {}

# Fungsi untuk memproses data
def process_data(entry):
    activity_time = entry.activity_date
    gender = entry.gender
    activity = entry.activity

    if gender not in processed_data:
        processed_data[gender] = []

    if activity == "in":
        processed_data[gender].append({"in": activity_time, "out": None})
    elif activity == "out":
        for record in reversed(processed_data[gender]):
            if record["out"] is None:
                record["out"] = activity_time
                break

# Memproses data
for entry in results:
    process_data(entry)

# Nama file CSV untuk menyimpan hasil
output_file = 'activity_output.csv'

# Menulis hasil ke dalam file CSV
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['gender', 'activity_time_in', 'activity_time_out'])
    for gender, records in processed_data.items():
        for record in records:
            writer.writerow([gender, record['in'], record['out']])

print(f"Data telah diekspor ke dalam file: {output_file}")

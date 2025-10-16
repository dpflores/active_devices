# insert_device_summary_sqlalchemy_gz.py
import os
import re
import gzip
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv


Base = declarative_base()

# --- CONFIGURACIÓN BASE DE DATOS ---
load_dotenv()  # 👈 Carga automáticamente el archivo .env

SQL_URI = os.getenv("SQL_URI", "mysql+pymysql://user:password@localhost/dbname")

class DeviceStatusSummary(Base):
    __tablename__ = "device_status_summary"

    date = Column(Date, primary_key=True)
    online_count = Column(Integer, nullable=False)
    offline_count = Column(Integer, nullable=False)
    filename = Column(String(255), nullable=False)


def extract_date_from_filename(filename: str):
    """Extrae la fecha (YYYY.MM.DD) del nombre del archivo."""
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", filename)
    if match:
        year, month, day = match.groups()
        return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()
    return None


def count_devices_from_file(file_path: str, is_gz: bool = False):
    """Cuenta los dispositivos online/offline de un archivo (normal o .gz)."""
    online_count = 0
    offline_count = 0

    open_func = gzip.open if is_gz else open

    with open_func(file_path, "rt", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if "Online:" in line and "Offline:" in line:
                parts = line.split(",")
                try:
                    online = int(parts[1].split(":")[1].strip())
                    offline = int(parts[2].split(":")[1].strip())
                    online_count += online
                    offline_count += offline
                except (IndexError, ValueError):
                    continue

    return online_count, offline_count


def process_directory(folder_path: str, db_url: str):
    """Procesa los archivos .csv y .csv.gz de una carpeta."""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            # Solo procesar archivos de Chile
            if "Chile" not in filename:
                continue

            # Archivos normales
            if filename.endswith(".csv"):
                is_gz = False
            # Archivos comprimidos con gzip
            elif filename.endswith(".csv.gz"):
                is_gz = True
            else:
                continue

            date_only = extract_date_from_filename(filename)
            if not date_only:
                print(f"⚠️  No se encontró fecha en: {filename}")
                continue

            online, offline = count_devices_from_file(file_path, is_gz=is_gz)

            existing = session.query(DeviceStatusSummary).filter_by(date=date_only).first()
            if existing:
                existing.online_count = online
                existing.offline_count = offline
                existing.filename = filename
                print(f"🔁 Actualizado: {filename} ({date_only})")
            else:
                record = DeviceStatusSummary(
                    date=date_only,
                    online_count=online,
                    offline_count=offline,
                    filename=filename
                )
                session.add(record)
                print(f"✅ Insertado: {filename} ({date_only})")

        session.commit()
        print("\n📦 Inserción completada correctamente.")

    except Exception as e:
        session.rollback()
        print(f"❌ Error: {e}")

    finally:
        session.close()


if __name__ == "__main__":
    folder_path = "/home/dpflores/Del/DYNAMO EDGE/backup_acs_report"  # Carpeta actual
    db_url = SQL_URI
    process_directory(folder_path, db_url)

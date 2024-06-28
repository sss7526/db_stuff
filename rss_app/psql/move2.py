
import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Database setup
DATABASE_URL = 'postgresql+psycopg2://user:password@localhost/dbname'

# Session Manager
class SessionManager:
    def __init__(self, database_url=DATABASE_URL):
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.Base = declarative_base()
        self.setup_database()

    def setup_database(self):
        class Country(self.Base):
            __tablename__ = 'countries'
            id = Column(Integer, primary_key=True)
            country = Column(String, unique=True, nullable=False)
            provinces = relationship('Province', cascade='all, delete, delete-orphan')

        class Province(self.Base):
            __tablename__ = 'provinces'
            id = Column(Integer, primary_key=True)
            province = Column(String, nullable=False)
            country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
            locations = relationship('Location', cascade='all, delete, delete-orphan')
            UniqueConstraint('province', 'country_id', name='uix_province_country')

        class Location(self.Base):
            __tablename__ = 'locations'
            id = Column(Integer, primary_key=True)
            location = Column(String, nullable=False)
            mgrs = Column(String, nullable=False)
            province_id = Column(Integer, ForeignKey('provinces.id'), nullable=False)
            UniqueConstraint('location', 'province_id', name='uix_location_province')

        self.Country = Country
        self.Province = Province
        self.Location = Location
        self.Base.metadata.create_all(self.engine)

    def create_session(self):
        return self.Session()

    def drop_indexes(self):
        with self.engine.connect() as connection:
            connection.execute("DROP INDEX IF EXISTS country_pkey, uix_province_country, uix_location_province")

    def create_indexes(self):
        with self.engine.connect() as connection:
            connection.execute(
                """
                CREATE UNIQUE INDEX country_pkey ON countries (id);
                CREATE UNIQUE INDEX uix_province_country ON provinces (province, country_id);
                CREATE UNIQUE INDEX uix_location_province ON locations (location, province_id);
                """
            )

# Insert data function
def insert_data(batch, session_manager):
    country_cache, province_cache = {}, {}
    countries_to_insert, provinces_to_insert, locations_to_insert = [], [], []

    session = session_manager.create_session()

    for row in batch.itertuples(index=False):
        country_name, province_name, location_name, mgrs = row.Country, row.Province, row.Location, row.MGRS
        
        # Handle Country
        if country_name not in country_cache:
            country = session.query(session_manager.Country).filter_by(country=country_name).first()
            if not country:
                country = session_manager.Country(country=country_name)
                session.add(country)
                session.commit()  # Commit to get the ID
            country_cache[country_name] = country

        # Handle Province
        province_key = (province_name, country_cache[country_name].id)
        if province_key not in province_cache:
            province = session.query(session_manager.Province).filter_by(province=province_name, country_id=country_cache[country_name].id).first()
            if not province:
                province = session_manager.Province(province=province_name, country_id=country_cache[country_name].id)
                session.add(province)
                session.commit()  # Commit to get the ID
            province_cache[province_key] = province

        # Handle Location
        location = session_manager.Location(location=location_name, mgrs=mgrs, province_id=province_cache[province_key].id)
        locations_to_insert.append(location)

    session.bulk_save_objects(locations_to_insert)
    session.commit()
    session.close()

def process_csv_file(csv_file, session_manager, chunksize):
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        try:
            insert_data(chunk, session_manager)
        except IntegrityError as e:
            print(f"IntegrityError: {e}")
        except Exception as e:
            print(f"Error: {e}")

def process_csv_files(directory, session_manager):
    chunksize = 10000  # Adjust the chunk size according to your available memory
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

    # Drop indexes before bulk inserts
    session_manager.drop_indexes()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_csv_file, csv_file, session_manager, chunksize) for csv_file in csv_files]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            future.result()  # this will raise any exceptions caught during the processing

    # Recreate indexes after bulk inserts
    session_manager.create_indexes()

def main():
    csv_directory = 'path_to_your_directory'  # Update this to the directory containing your CSV files
    session_manager = SessionManager()  # Instantiate the session manager

    process_csv_files(csv_directory, session_manager)

if __name__ == "__main__":
    main()

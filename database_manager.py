import sqlite3
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from config import Config

class DatabaseManager:
    def __init__(self, config: Config):
        self.config = config
        self.engines = {}
        self.sessions = {}
        self.metadata = {}

        self.setup_databases()

    def setup_databases(self):
        if self.config.use_remote_db:
            self.setup_remote_database('database1', self.config.remote_db_config.database1)
            self.setup_remote_database('database2', self.config.remote_db_config.database2)
            self.setup_remote_database('database3', self.config.remote_db_config.database3)
        else:
            self.setup_local_database('database1', self.config.local_db_paths.database1, in_memory=True)
            self.setup_local_database('database2', self.config.local_db_paths.database2, in_memory=False)
            self.setup_local_database('database3', self.config.local_db_paths.database3, in_memory=False)

    def setup_remote_database(self, db_key, db_url):
        engine = create_engine(db_url)
        session_factory = sessionmaker(bind=engine)
        self.engines[db_key] = engine
        self.metadata[db_key] = MetaData(bind=engine)
        self.metadata[db_key].reflect()
        self.sessions[db_key] = scoped_session(session_factory)

    def setup_local_database(self, db_key, db_path, in_memory):
        if in_memory:
            conn = self.copy_db_to_memory(db_path)
            engine = self.create_memory_engine(conn)
        else:
            engine = create_engine(f'sqlite:///{db_path}')
            
        session_factory = sessionmaker(bind=engine)
        self.engines[db_key] = engine
        self.metadata[db_key] = MetaData(bind=engine)
        self.metadata[db_key].reflect()
        self.sessions[db_key] = scoped_session(session_factory)

    def copy_db_to_memory(self, source_db_path):
        source_conn = sqlite3.connect(source_db_path)
        memory_conn = sqlite3.connect(':memory:')
        source_conn.backup(memory_conn)
        source_conn.close()
        return memory_conn

    def create_memory_engine(self, memory_conn):
        db_url = 'sqlite://'
        engine = create_engine(db_url, creator=lambda: memory_conn)
        return engine

    def get_session(self, db_key):
        return self.sessions[db_key]

    def cleanup(self):
        for key in self.sessions:
            self.sessions[key].remove()  # Remove the session
        for key in self.engines:
            self.engines[key].dispose()  # Dispose the engine

# Base class for SQLAlchemy models
Base = declarative_base()

# Placeholder for actual models
class ExampleTable1(Base):
    __tablename__ = 'example_table1'

class ExampleTable2(Base):
    __tablename__ = 'example_table2'

class ExampleTable3(Base):
    __tablename__ = 'example_table3'
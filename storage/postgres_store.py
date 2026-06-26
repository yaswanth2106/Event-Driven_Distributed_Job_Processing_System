from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from contextlib import contextmanager
import os

try:
    import config
    DATABASE_URL = config.DATABASE_URL
except ImportError:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/platform_db")

Base = declarative_base()

class WorkerDB(Base):
    __tablename__ = 'workers'
    worker_id = Column(String, primary_key=True)
    capacity = Column(Integer)
    status = Column(String)
    last_heartbeat = Column(DateTime)

class TaskDB(Base):
    __tablename__ = 'tasks'
    task_id = Column(String, primary_key=True)
    job_type = Column(String)
    priority = Column(String)
    status = Column(String)
    retries = Column(Integer)
    worker_id = Column(String, ForeignKey('workers.worker_id'), nullable=True)

class PostgresStore:
    def __init__(self, db_url=DATABASE_URL):
        self.db_url = db_url
        self.online = False
        self.engine = create_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        import threading
        threading.Thread(target=self.init_db, daemon=True).start()

    def init_db(self):
        try:
            Base.metadata.create_all(bind=self.engine)
            self.online = True
            print("[DATABASE] PostgreSQL database connected")
        except Exception as e:
            self.online = False
            print(f"[DATABASE WARNING] PostgreSQL database offline {e}")
            #print("[DATABASE WARNING] System will proceed, but database persistence operations will fail.")

    @contextmanager
    def session_scope(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self):
        return self.SessionLocal()

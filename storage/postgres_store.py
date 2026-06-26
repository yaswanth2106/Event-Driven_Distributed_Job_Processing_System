from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from contextlib import contextmanager
import os

try:
    import config
    DATABASE_URL = config.DATABASE_URL
except ImportError:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/platform_db")

class Base(DeclarativeBase):
    pass

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
        self._setup_engine(self.db_url)
        import threading
        threading.Thread(target=self.init_db, daemon=True).start()

    def _setup_engine(self, db_url):
        if db_url.startswith("sqlite"):
            self.engine = create_engine(db_url)
        else:
            self.engine = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True
            )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def init_db(self):
        try:
            Base.metadata.create_all(bind=self.engine)
            self.online = True
            db_type = "SQLite" if self.db_url.startswith("sqlite") else "PostgreSQL"
            print(f"[DATABASE] {db_type} database connected")
        except Exception as e:
            if not self.db_url.startswith("sqlite"):
                print(f"[DATABASE WARNING] PostgreSQL database offline {e}. Falling back to SQLite...")
                self.db_url = "sqlite:///platform_db.db"
                try:
                    self._setup_engine(self.db_url)
                    Base.metadata.create_all(bind=self.engine)
                    self.online = True
                    print("[DATABASE] SQLite fallback database connected and initialized")
                except Exception as fallback_e:
                    self.online = False
                    print(f"[DATABASE ERROR] SQLite fallback also failed: {fallback_e}")
            else:
                self.online = False
                print(f"[DATABASE ERROR] SQLite database initialization failed: {e}")

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

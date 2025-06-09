import logging
import os
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, scoped_session, sessionmaker

# Base class for all SQLAlchemy models
Base: Any = declarative_base()


class DatabaseConfig(BaseModel):
    db_type: str = "postgres"
    database: str
    host: str
    port: str = "5432"
    username: str
    password: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    echo: bool = False
    development_mode: bool = False

    def get_connection_string(self) -> str:
        if self.db_type.lower() == "postgres":
            if not all([self.host, self.database, self.username, self.password]):
                raise ValueError("Missing required Postgres configuration parameters")
            return (
                f"postgresql://{self.username}:{self.password}@"
                f"{self.host}:{self.port}/{self.database}"
            )
        elif self.db_type.lower() == "sqlite":
            return f"sqlite:///{self.database}"
        raise ValueError(f"Unsupported database type: {self.db_type}")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __repr__(self) -> str:
        """String representation with masked password for security."""
        return (
            f"DatabaseConfig("
            f"db_type='{self.db_type}', "
            f"host='{self.host}', "
            f"port='{self.port}', "
            f"database='{self.database}', "
            f"username='{self.username}', "
            f"password='***')"
        )


class DatabaseManager:
    """
    Database connection manager that uses a Pydantic DatabaseConfig.
    """

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = self._create_engine()
        self.session_factory = sessionmaker(bind=self.engine)
        self.scoped_session = scoped_session(self.session_factory)

    def _create_engine(self):
        connection_string = self.config.get_connection_string()
        if self.config.db_type.lower() == "sqlite":
            connect_args = {"check_same_thread": False}
            return create_engine(
                connection_string, echo=self.config.echo, connect_args=connect_args
            )
        return create_engine(
            connection_string,
            echo=self.config.echo,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
        )

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)

    def drop_tables(self) -> None:
        if self.config.development_mode:
            Base.metadata.drop_all(self.engine)
        else:
            raise RuntimeError("Cannot drop tables: not in development mode")

    def get_session(self) -> Session:
        return self.scoped_session()

    def close_session(self, session: Optional[Session] = None) -> None:
        if session:
            session.close()
        else:
            self.scoped_session.remove()

    def close(self) -> None:
        self.scoped_session.remove()
        self.engine.dispose()


def get_development_config() -> DatabaseConfig:
    """
    Get SQLite configuration for development.
    """
    return DatabaseConfig(
        db_type="sqlite",
        database=os.environ.get("DEV_DB_PATH", ":memory:"),
        echo=os.environ.get("DB_ECHO", "False").lower() == "true",
        development_mode=True,
    )


def get_production_config() -> DatabaseConfig:
    """
    Get Postgres configuration for production from environment variables.
    """
    return DatabaseConfig(
        db_type="postgres",
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        database=os.environ.get("DB_NAME", "integration_db"),
        username=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", ""),
        pool_size=int(os.environ.get("DB_POOL_SIZE", "5")),
        max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "10")),
        pool_timeout=int(os.environ.get("DB_POOL_TIMEOUT", "30")),
        echo=os.environ.get("DB_ECHO", "False").lower() == "true",
        development_mode=False,
    )


def import_all_models():
    """Import all models to ensure they're registered with SQLAlchemy metadata."""
    # Import order matters - base models first, then dependent models
    # Configure mappers to resolve all relationships
    from sqlalchemy.orm import configure_mappers

    from src.db.db_entity_models import Entity  # noqa
    from src.db.db_error_models import ProcessingError  # noqa
    from src.db.db_state_transition_models import StateTransition  # noqa
    from src.db.db_tenant_models import Tenant  # noqa
    from src.db.db_credential_models import ExternalCredential  # noqa

    configure_mappers()


def init_db(db_manager: DatabaseManager) -> None:
    """
    Initialize the database, creating all tables.

    Args:
        db_manager: DatabaseManager instance to use for table creation
    """
    logging.warning("Initializing DB")
    # First ensure all models are imported and registered
    import_all_models()

    # Then create tables
    db_manager.create_tables()


def close_db() -> None:
    """
    Close the database connections and dispose of the engine.
    """
    global db_manager
    if db_manager:
        db_manager.close()
        db_manager = None

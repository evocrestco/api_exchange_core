import logging
import os
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, scoped_session, sessionmaker

from src.config import get_config

# Base class for all SQLAlchemy models
Base: Any = declarative_base()


class DatabaseConfig(BaseModel):
    db_type: str = "sqlite"
    database: str = ":memory:"
    host: Optional[str] = None
    port: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
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


def create_database_manager() -> DatabaseManager:
    """
    Create a DatabaseManager based on the centralized configuration.
    """
    app_config = get_config()

    # Parse the connection string from centralized config
    conn_str = app_config.database.connection_string

    if conn_str.startswith("postgresql://") or conn_str.startswith("postgres://"):
        # Production-like Postgres configuration
        config = DatabaseConfig(
            db_type="postgres",
            database=conn_str,
            pool_size=app_config.database.pool_size,
            max_overflow=app_config.database.max_overflow,
            pool_timeout=app_config.database.pool_timeout,
            echo=app_config.database.echo,
            development_mode=app_config.environment == "development",
        )
    else:
        # SQLite configuration (for development/testing)
        db_path = (
            conn_str.replace("sqlite:///", "") if conn_str.startswith("sqlite:///") else ":memory:"
        )
        config = DatabaseConfig(
            db_type="sqlite",
            database=db_path,
            echo=app_config.database.echo,
            development_mode=app_config.environment == "development",
        )

    return DatabaseManager(config)


# Global database manager instance
db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get or create the global DatabaseManager instance.
    """
    global db_manager
    if db_manager is None:
        db_manager = create_database_manager()
    return db_manager


def get_db_session() -> Session:
    """
    Get a SQLAlchemy session from the global manager.
    """
    return get_db_manager().get_session()


def import_all_models():
    """Import all models to ensure they're registered with SQLAlchemy metadata."""
    # Import order matters - base models first, then dependent models
    # Configure mappers to resolve all relationships
    from sqlalchemy.orm import configure_mappers

    from src.db.db_entity_models import Entity  # noqa
    from src.db.db_error_models import ProcessingError  # noqa
    from src.db.db_state_transition_models import StateTransition  # noqa
    from src.db.db_tenant_models import Tenant  # noqa

    configure_mappers()


def init_db() -> None:
    """
    Initialize the database, creating all tables.
    """
    logging.warning("Initializing DB")
    # First ensure all models are imported and registered
    import_all_models()

    # Then create tables
    get_db_manager().create_tables()


def close_db() -> None:
    """
    Close the database connections and dispose of the engine.
    """
    global db_manager
    if db_manager:
        db_manager.close()
        db_manager = None

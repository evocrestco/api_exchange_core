import os
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, scoped_session, sessionmaker

from ..exceptions import ErrorCode, ServiceError, ValidationError
from ..utils import get_logger

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
                raise ValidationError(
                    "Missing required Postgres configuration parameters",
                    error_code=ErrorCode.MISSING_REQUIRED,
                    field="database_config",
                    value={"host": self.host, "database": self.database, "username": self.username},
                )
            return (
                f"postgresql://{self.username}:{self.password}@"
                f"{self.host}:{self.port}/{self.database}"
            )
        elif self.db_type.lower() == "sqlite":
            return f"sqlite:///{self.database}"
        raise ValidationError(
            f"Unsupported database type: {self.db_type}",
            error_code=ErrorCode.INVALID_FORMAT,
            field="db_type",
            value=self.db_type,
        )

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
            raise ServiceError(
                "Cannot drop tables: not in development mode",
                error_code=ErrorCode.CONFIGURATION_ERROR,
                operation="drop_tables",
                development_mode=self.config.development_mode,
            )

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

    from .db_api_token_models import APIToken, APITokenUsageLog, TokenCoordination  # noqa
    from .db_credential_models import ExternalCredential  # noqa
    from .db_entity_models import Entity  # noqa
    from .db_pipeline_state_models import PipelineStateHistory  # noqa
    from .db_tenant_models import Tenant  # noqa

    configure_mappers()


def init_db(db_manager: DatabaseManager) -> None:
    """
    Initialize the database, creating all tables.

    Args:
        db_manager: DatabaseManager instance to use for table creation
    """
    get_logger().warning("Initializing DB")
    # First ensure all models are imported and registered
    import_all_models()

    # Then create tables
    db_manager.create_tables()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get the global database manager instance.

    Returns:
        DatabaseManager: The current database manager

    Raises:
        ServiceError: If no database manager has been initialized
    """
    global _db_manager
    if _db_manager is None:
        raise ServiceError(
            "Database manager not initialized. Call initialize_db() first.",
            error_code=ErrorCode.CONFIGURATION_ERROR,
            operation="get_db_manager",
        )
    return _db_manager


def set_db_manager(manager: DatabaseManager) -> None:
    """
    Set the global database manager instance.

    This is primarily used for testing to inject a test database manager.

    Args:
        manager: DatabaseManager instance to set as global
    """
    global _db_manager
    _db_manager = manager


def initialize_db(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """
    Initialize the global database manager with the given config.

    Args:
        config: Optional DatabaseConfig. If None, uses production config from environment.

    Returns:
        DatabaseManager: The initialized database manager
    """
    global _db_manager

    if config is None:
        config = get_production_config()

    _db_manager = DatabaseManager(config)

    # Initialize tables
    import_all_models()
    _db_manager.create_tables()

    return _db_manager


def close_db() -> None:
    """
    Close the database connections and dispose of the engine.
    """
    global _db_manager
    if _db_manager:
        _db_manager.close()
        _db_manager = None

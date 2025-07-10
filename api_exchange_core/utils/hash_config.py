"""
Hash configuration for data comparison and duplicate detection.

This module provides a configuration class for hash calculation,
allowing for flexible and reusable hash strategies.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class HashConfig(BaseModel):
    """
    Configuration for data hash calculation.

    This class encapsulates all parameters related to how data hashes
    are calculated, providing a reusable configuration object.
    """

    key_fields: Optional[List[str]] = Field(
        default=None,
        description="Optional list of fields to include in hash calculation. "
        "If empty, all fields except those in ignore_fields are used.",
    )

    ignore_fields: Optional[List[str]] = Field(
        default=None, description="Fields to exclude from hash calculation."
    )

    sort_keys: bool = Field(
        default=True, description="Whether to sort dictionary keys for deterministic ordering."
    )

    @classmethod
    def default(cls) -> "HashConfig":
        """
        Create a default hash configuration.

        Returns:
            Default HashConfig instance with standard settings
        """
        return cls(
            key_fields=None,
            ignore_fields=[
                "created_at",
                "updated_at",
                "metadata",
                "version",
                "data_hash",
                "last_processed_at",
                "processing_history",
            ],
            sort_keys=True,
        )

    @classmethod
    def for_type(cls, data_type: str) -> "HashConfig":
        """
        Create a hash configuration optimized for a specific data type.

        Args:
            data_type: Type of data

        Returns:
            HashConfig instance configured for the data type
        """
        # Default config for all types - applications can override this method
        # to provide data-type-specific hash configurations
        return cls.default()

"""
Hash utilities for entity comparison and duplicate detection.

This module provides functions for generating deterministic hashes of entity data
to enable duplicate detection and comparison.
"""

import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple

from ..exceptions import ErrorCode, ValidationError
from .hash_config import HashConfig
from .json_utils import dumps
from .logger import get_logger

logger = get_logger()


def _apply_config(
    config: Optional[HashConfig],
    key_fields: Optional[List[str]],
    ignore_fields: Optional[List[str]],
    sort_keys: Optional[bool] = None,
) -> Tuple[Optional[List[str]], List[str], bool]:
    """
    Apply configuration parameters with proper defaults.

    Args:
        config: HashConfig object if provided
        key_fields: List of fields to include
        ignore_fields: List of fields to exclude
        sort_keys: Whether to sort keys

    Returns:
        Tuple of (key_fields, ignore_fields, sort_keys) with all values resolved
    """
    # Use config object if provided, otherwise use individual parameters
    if config is not None:
        key_fields = config.key_fields
        ignore_fields = config.ignore_fields
        if sort_keys is not None:
            sort_keys = config.sort_keys

    # Default fields to ignore
    if ignore_fields is None:
        ignore_fields = [
            "created_at",
            "updated_at",
            "metadata",
            "version",
            "data_hash",
            "last_processed_at",
            "processing_history",
        ]

    # Default sort_keys value
    if sort_keys is None:
        sort_keys = True

    return key_fields, ignore_fields, sort_keys


def _get_nested_value(data: Dict[str, Any], field: str) -> Any:
    """
    Extract a value from a nested dictionary using dot notation.

    Args:
        data: Dictionary to extract from
        field: Field name with dot notation (e.g., "customer.email")

    Returns:
        Extracted value or None if not found
    """
    if "." not in field:
        return data.get(field)

    # Handle nested fields
    parts = field.split(".")
    value = data

    # Traverse the nested structure
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None

    return value


def calculate_entity_hash(
    data: Dict[str, Any],
    key_fields: Optional[List[str]] = None,
    ignore_fields: Optional[List[str]] = None,
    sort_keys: bool = True,
    config: Optional[HashConfig] = None,
) -> str:
    """
    Calculate a deterministic hash of entity data for duplicate detection.

    Args:
        data: Dict containing the entity's data
        key_fields: Optional list of fields to include in hash calculation.
                   If empty, all fields except those in ignore_fields are used.
        ignore_fields: Fields to exclude from hash calculation
        sort_keys: Whether to sort dictionary keys for deterministic ordering
        config: Optional hash configuration object that overrides other parameters if provided

    Returns:
        String containing SHA-256 hash

    Raises:
        TypeError: If data is None
    """
    # Check for None data
    if data is None:
        raise ValidationError(
            "Cannot calculate hash for None data",
            error_code=ErrorCode.TYPE_MISMATCH,
            field="data",
            value=data
        )

    # Apply configuration
    key_fields, ignore_fields, sort_keys = _apply_config(
        config, key_fields, ignore_fields, sort_keys
    )

    # Create a filtered copy to hash
    filtered_data = {}

    # If key_fields is provided, only include those fields
    if key_fields:
        for field in key_fields:
            if "." in field:
                value = _get_nested_value(data, field)
                if value is not None:
                    filtered_data[field] = value
            elif field in data:
                filtered_data[field] = data[field]
    else:
        # Otherwise include all fields except ignored ones
        for key, value in data.items():
            if key not in ignore_fields:
                filtered_data[key] = value

    try:
        # Use the same JSON serialization as the system to ensure consistency
        serialized = dumps(filtered_data, sort_keys=sort_keys)

        # Calculate hash
        hash_value = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

        return hash_value
    except Exception as e:
        logger.error(f"Error calculating entity hash: {str(e)}")
        # Return a fallback hash of the str representation as a last resort
        return hashlib.sha256(str(filtered_data).encode("utf-8")).hexdigest()


def extract_key_fields(data: Dict[str, Any], key_fields: List[str]) -> Dict[str, Any]:
    """
    Extract only the specified fields from data, supporting nested fields.

    Args:
        data: Source data dictionary
        key_fields: List of field names to extract, supports dot notation

    Returns:
        Dictionary containing only the specified fields
    """
    if not key_fields:
        return data

    result = {}
    for field in key_fields:
        value = _get_nested_value(data, field)
        if value is not None:
            result[field] = value

    return result


def compare_entities(
    existing_data: Dict[str, Any],
    new_data: Dict[str, Any],
    key_fields: Optional[List[str]] = None,
    ignore_fields: Optional[List[str]] = None,
    config: Optional[HashConfig] = None,
) -> Dict[str, Any]:
    """
    Compare two entity data dictionaries and identify differences.

    Args:
        existing_data: Existing entity data
        new_data: New entity data to compare
        key_fields: Optional list of fields to compare. If None, all fields are compared.
        ignore_fields: Fields to exclude from comparison
        config: Optional hash configuration object that overrides other parameters if provided

    Returns:
        Dictionary mapping changed field names to (old_value, new_value) tuples
    """
    # Apply configuration
    key_fields, ignore_fields, _ = _apply_config(config, key_fields, ignore_fields)

    # Determine fields to compare
    fields_to_compare: Set[str] = set()

    if key_fields:
        fields_to_compare.update(key_fields)
    else:
        fields_to_compare.update(key for key in existing_data.keys() if key not in ignore_fields)
        fields_to_compare.update(key for key in new_data.keys() if key not in ignore_fields)

    # Compare fields
    changes = {}
    for field in fields_to_compare:
        if field in ignore_fields:
            continue

        # Get values, handling nested fields
        old_value = _get_nested_value(existing_data, field)
        new_value = _get_nested_value(new_data, field)

        # Compare values (handles None values)
        if old_value != new_value:
            changes[field] = (old_value, new_value)

    return changes

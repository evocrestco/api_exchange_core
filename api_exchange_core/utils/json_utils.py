import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Union


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Handle Pydantic models and other objects with model_dump method
        elif hasattr(obj, "model_dump"):
            return obj.model_dump()
        # Handle objects with __dict__ attribute
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        return super().default(obj)


def dumps(obj: Any, **kwargs) -> str:
    """JSON dumps with Decimal and datetime support."""
    return json.dumps(obj, cls=EnhancedJSONEncoder, **kwargs)


def dump(obj: Any, fp, **kwargs) -> None:
    """JSON dump with Decimal and datetime support."""
    json.dump(obj, fp, cls=EnhancedJSONEncoder, **kwargs)


def loads(s: Union[str, bytes, bytearray], **kwargs) -> Any:
    """Standard JSON loads function."""
    return json.loads(s, **kwargs)


def load(fp, **kwargs) -> Any:
    """Standard JSON load function."""
    return json.load(fp, **kwargs)

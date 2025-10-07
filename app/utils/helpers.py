import base64
from typing import Any, Optional


def to_base64(input_data: Any) -> str:
    """Convert various input formats to base64"""
    if not input_data:
        return ''
    
    if isinstance(input_data, str):
        if input_data.startswith('0x'):
            input_data = input_data[2:]
        try:
            return base64.b64encode(bytes.fromhex(input_data)).decode('utf-8')
        except ValueError:
            return base64.b64encode(input_data.encode('utf-8')).decode('utf-8')
    
    if isinstance(input_data, bytes):
        return base64.b64encode(input_data).decode('utf-8')
    
    if isinstance(input_data, list):
        return base64.b64encode(bytes(input_data)).decode('utf-8')
    
    if isinstance(input_data, dict) and 'data' in input_data and isinstance(input_data['data'], list):
        return base64.b64encode(bytes(input_data['data'])).decode('utf-8')
    
    try:
        str_data = str(input_data)
        if str_data.startswith('0x'):
            str_data = str_data[2:]
        return base64.b64encode(bytes.fromhex(str_data)).decode('utf-8')
    except (ValueError, TypeError):
        return base64.b64encode(str(input_data).encode('utf-8')).decode('utf-8')

def gid_to_numeric_id(gid: Optional[str]) -> Optional[int]:
    """Convert Shopify GraphQL GID (e.g., gid://shopify/Product/123) to numeric id."""
    if not gid or not isinstance(gid, str):
        return None
    try:
        return int(gid.rsplit('/', 1)[-1])
    except Exception:
        return None

def parse_metafield_value(value: Any) -> Any:
    """Parse metafield value which may be a JSON string or already structured.
    - If JSON array-like strings are detected, return parsed list.
    - Otherwise return the value unchanged.
    """
    if not value:
        return value
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.startswith('[') and s.endswith(']'):
            try:
                import json
                return json.loads(s)
            except Exception:
                return value
        if s.startswith('"[') and s.endswith(']"'):
            try:
                import json
                return json.loads(json.loads(s))
            except Exception:
                return value
    return value
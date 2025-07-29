import base64
from typing import Any


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


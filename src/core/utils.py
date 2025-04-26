import base64
import re
from typing import Any

def is_base64(content: str) -> bool:
    """
    Check if a string is base64 encoded.
    
    Args:
        content: String to check
        
    Returns:
        bool: True if the string is base64 encoded, False otherwise
    """
    try:
        # Check if string matches base64 pattern
        if not re.match('^[A-Za-z0-9+/]*={0,2}$', content):
            return False
            
        # Try to decode
        base64.b64decode(content)
        return True
    except Exception:
        return False

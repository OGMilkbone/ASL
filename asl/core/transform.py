"""
Schema Transformer module for handling data transformations between schema versions.
"""

from typing import Any, Dict, Optional
import re
from datetime import datetime


class SchemaTransformer:
    """Handles schema transformations and data conversions."""
    
    def __init__(self):
        self._transform_cache: Dict[str, Any] = {}
        
    def transform_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data according to the schema's transformation rules."""
        transformed_data = data.copy()
        
        for field, field_schema in schema.items():
            if "transform" in field_schema:
                transform_rule = field_schema["transform"]
                if field in transformed_data:
                    transformed_data[field] = self._apply_transform(
                        transformed_data[field],
                        transform_rule
                    )
                    
        return transformed_data
        
    def _apply_transform(self, value: Any, transform_rule: str) -> Any:
        """Apply a transformation rule to a value."""
        # Check cache first
        cache_key = f"{value}:{transform_rule}"
        if cache_key in self._transform_cache:
            return self._transform_cache[cache_key]
            
        # Parse the transformation rule
        if transform_rule.startswith("split"):
            # Handle split transformation
            match = re.match(r"split\((.*?),\s*['\"](.*?)['\"]\)\[(\d+)\]", transform_rule)
            if match:
                field, delimiter, index = match.groups()
                parts = value.split(delimiter)
                if len(parts) > int(index):
                    result = parts[int(index)]
                else:
                    result = value
            else:
                result = value
                
        elif transform_rule.startswith("concat"):
            # Handle concatenation transformation
            match = re.match(r"concat\((.*?)\)", transform_rule)
            if match:
                fields = [f.strip().strip("'\"") for f in match.group(1).split(",")]
                result = "".join(str(value.get(f, "")) for f in fields)
            else:
                result = value
                
        elif transform_rule.startswith("date_format"):
            # Handle date format transformation
            match = re.match(r"date_format\((.*?),\s*['\"](.*?)['\"]\)", transform_rule)
            if match:
                field, format_str = match.groups()
                try:
                    date = datetime.fromisoformat(value)
                    result = date.strftime(format_str)
                except (ValueError, TypeError):
                    result = value
            else:
                result = value
                
        else:
            # Unknown transformation rule
            result = value
            
        # Cache the result
        self._transform_cache[cache_key] = result
        return result
        
    def clear_cache(self) -> None:
        """Clear the transformation cache."""
        self._transform_cache.clear()
        
    def validate_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """Validate data against a schema."""
        for field, field_schema in schema.items():
            if field not in data:
                if "required" in field_schema and field_schema["required"]:
                    return False
                continue
                
            field_type = field_schema.get("type", "string")
            value = data[field]
            
            if not self._validate_type(value, field_type):
                return False
                
        return True
        
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate a value against an expected type."""
        type_validators = {
            "string": lambda x: isinstance(x, str),
            "integer": lambda x: isinstance(x, int),
            "float": lambda x: isinstance(x, (int, float)),
            "boolean": lambda x: isinstance(x, bool),
            "datetime": lambda x: isinstance(x, (str, datetime)),
            "array": lambda x: isinstance(x, list),
            "object": lambda x: isinstance(x, dict)
        }
        
        validator = type_validators.get(expected_type)
        if validator:
            return validator(value)
        return True 
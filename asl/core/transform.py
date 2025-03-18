"""
Schema Transformer module for handling data transformations between schema versions.
"""

from typing import Any, Dict, Optional
import re
from datetime import datetime


class SchemaTransformer:
    """
    Handles the transformation of data between different schema versions.
    """
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        
    def transform(
        self,
        data: Dict,
        transformation: str,
        context: Optional[Dict] = None
    ) -> Any:
        """
        Transform data according to a transformation rule.
        
        Args:
            data: Data to transform
            transformation: Transformation rule as a string expression
            context: Optional context data for the transformation
            
        Returns:
            Transformed data
        """
        # Check cache first
        cache_key = f"{str(data)}:{transformation}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Prepare context for transformation
        ctx = {
            "data": data,
            "datetime": datetime,
            **(context or {})
        }
        
        # Parse and execute transformation
        try:
            # Basic field access
            if "." in transformation:
                parts = transformation.split(".")
                value = data
                for part in parts:
                    value = value[part]
                result = value
            # Function calls
            elif "(" in transformation:
                func_name = transformation.split("(")[0]
                args = self._parse_args(transformation)
                result = self._execute_function(func_name, args, ctx)
            # Simple field access
            else:
                result = data[transformation]
                
            # Cache the result
            self._cache[cache_key] = result
            return result
            
        except Exception as e:
            raise ValueError(f"Transformation failed: {str(e)}")
            
    def _parse_args(self, transformation: str) -> list:
        """Parse function arguments from a transformation string."""
        # Extract arguments between parentheses
        args_str = transformation[transformation.find("(")+1:transformation.rfind(")")]
        if not args_str:
            return []
            
        # Split arguments and evaluate each one
        args = []
        current = ""
        in_quotes = False
        
        for char in args_str:
            if char == '"' and not in_quotes:
                in_quotes = True
            elif char == '"' and in_quotes:
                in_quotes = False
            elif char == "," and not in_quotes:
                args.append(current.strip())
                current = ""
            else:
                current += char
                
        if current:
            args.append(current.strip())
            
        return args
        
    def _execute_function(self, func_name: str, args: list, context: Dict) -> Any:
        """Execute a transformation function with the given arguments."""
        # Built-in functions
        if func_name == "split":
            if len(args) != 2:
                raise ValueError("split() requires exactly 2 arguments")
            text = self.transform(context["data"], args[0], context)
            delimiter = args[1].strip('"')
            return text.split(delimiter)
            
        elif func_name == "concat":
            if len(args) < 2:
                raise ValueError("concat() requires at least 2 arguments")
            parts = [self.transform(context["data"], arg, context) for arg in args]
            return "".join(str(part) for part in parts)
            
        elif func_name == "format_date":
            if len(args) != 2:
                raise ValueError("format_date() requires exactly 2 arguments")
            date = self.transform(context["data"], args[0], context)
            format_str = args[1].strip('"')
            return datetime.strptime(date, format_str)
            
        else:
            raise ValueError(f"Unknown function: {func_name}")
            
    def clear_cache(self) -> None:
        """Clear the transformation cache."""
        self._cache.clear() 
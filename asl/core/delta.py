"""
Schema Delta module for handling schema differences and transformations.
"""

from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field
import re
import ast
import operator as op


class SchemaDelta(BaseModel):
    """
    Represents a schema delta containing changes between schema versions.
    """
    added_fields: Dict[str, str] = Field(default_factory=dict, description="Fields added in this version")
    removed_fields: Dict[str, str] = Field(default_factory=dict, description="Fields removed in this version")
    transformations: Dict[str, str] = Field(
        default_factory=dict,
        description="Transformation rules for converting between versions"
    )
    metadata: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict,
        description="Additional metadata about the schema delta"
    )

    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply this delta to transform data from old schema to new schema."""
        result = data.copy()
        
        # Store the original data before removing fields
        old_data = result.copy()
        
        # Remove fields
        for field in self.removed_fields:
            if field in result:
                del result[field]
        
        # Apply transformations
        for field, expr in self.transformations.items():
            try:
                # Create a safe environment for evaluation
                safe_env = {
                    'data': old_data,
                    'split': str.split,
                    'concat': lambda *args: ''.join(str(arg) for arg in args)
                }
                
                # Parse the transformation expression
                tree = ast.parse(expr, mode='eval')
                
                # Convert the AST to a string representation that can be safely evaluated
                if isinstance(tree.body, ast.Call):
                    # Handle function calls
                    func_name = tree.body.func.id
                    args = [ast.unparse(arg) for arg in tree.body.args]
                    
                    # Replace field references with data dictionary access
                    processed_args = []
                    for arg in args:
                        if arg in old_data:
                            processed_args.append(f'data["{arg}"]')
                        else:
                            processed_args.append(arg)
                    
                    # Evaluate the function call
                    result[field] = safe_env[func_name](*[eval(arg, {"__builtins__": {}}, safe_env) for arg in processed_args])
                else:
                    # Handle field references
                    if expr in old_data:
                        result[field] = old_data[expr]
                    else:
                        # Handle other expressions
                        processed_expr = expr
                        for key in old_data:
                            processed_expr = processed_expr.replace(key, f'data["{key}"]')
                        result[field] = eval(processed_expr, {"__builtins__": {}}, safe_env)
            except Exception as e:
                raise ValueError(f"Error evaluating transformation for field {field}: {str(e)}")
        
        return result
        
    def reverse(self) -> 'SchemaDelta':
        """Create a reverse delta for transforming data back to the old schema."""
        # Swap added and removed fields
        added = self.removed_fields
        removed = self.added_fields
        
        # Reverse transformations
        transformations = {}
        for field, expr in self.transformations.items():
            # For simple field references, just swap the fields
            if expr in self.removed_fields:
                transformations[expr] = field
            else:
                # For complex transformations, we can't automatically reverse them
                # This would require more sophisticated analysis
                pass
        
        return SchemaDelta(
            added_fields=added,
            removed_fields=removed,
            transformations=transformations,
            metadata=self.metadata
        )
        
    def is_compatible_with(self, other: 'SchemaDelta') -> bool:
        """Check if this delta is compatible with another delta."""
        # For now, we consider deltas compatible if they don't modify the same fields
        return not (set(self.added_fields.keys()) & set(other.added_fields.keys()) or
                   set(self.removed_fields.keys()) & set(other.removed_fields.keys()) or
                   set(self.transformations.keys()) & set(other.transformations.keys())) 
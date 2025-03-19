"""
Schema transformer module for transforming data between schema versions.
"""

from typing import Dict, List, Any, Optional
from .delta import SchemaDelta
from .registry import SchemaRegistry

class SchemaTransformer:
    """
    Transforms data between different schema versions using schema deltas.
    """
    
    def __init__(self, registry: Optional[SchemaRegistry] = None):
        """Initialize the transformer with an optional registry."""
        self._registry = registry
        
    def transform(self, data: Dict[str, Any], path: str) -> Any:
        """
        Get a value from a nested dictionary using a dot-separated path.
        
        Args:
            data: The dictionary to search in
            path: A dot-separated path to the value (e.g. "user.address.street")
            
        Returns:
            The value at the specified path, or None if not found
        """
        if not path:
            return None
            
        parts = path.split('.')
        current = data
        
        for part in parts:
            if not isinstance(current, dict):
                return None
            if part not in current:
                return None
            current = current[part]
            
        return current
        
    def transform_data(self, subject: str, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """
        Transform data from one schema version to another.
        
        Args:
            subject: The schema subject
            data: The data to transform
            from_version: The source schema version
            to_version: The target schema version
            
        Returns:
            The transformed data
            
        Raises:
            ValueError: If the transformation is not possible
        """
        if not self._registry:
            raise ValueError("No registry provided for schema transformation")
        return self._registry.transform_data(subject, data, from_version, to_version)
        
    def _get_delta_chain(self, subject: str, from_version: str, to_version: str) -> List[SchemaDelta]:
        """Get the chain of deltas needed to transform between versions."""
        if not self._registry:
            raise ValueError("No registry provided for schema transformation")
        return self._registry._get_delta_chain(subject, from_version, to_version)
        
    def _apply_delta(self, data: Dict[str, Any], delta: SchemaDelta) -> Dict[str, Any]:
        """Apply a schema delta to transform data."""
        return delta.apply(data)
    
    def _reverse_delta(self, data: Dict[str, Any], delta: SchemaDelta) -> Dict[str, Any]:
        """Apply a delta in reverse to transform data back."""
        return delta.reverse().apply(data)
        
    def transform_data(self, subject: str, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform data between two schema versions.
        
        Args:
            subject: The schema subject
            data: The data to transform
            from_version: The source version
            to_version: The target version
            
        Returns:
            The transformed data
        """
        if subject not in self._deltas:
            raise ValueError(f"Subject {subject} not found")
            
        if from_version == to_version:
            return data
            
        # Get delta chain
        deltas = self._get_delta_chain(subject, from_version, to_version)
        if not deltas:
            raise ValueError(f"No transformation path found from {from_version} to {to_version}")
            
        # Apply deltas
        result = data
        for delta in deltas:
            result = delta.apply(result)
        return result
        
    def _get_delta_chain(self, subject: str, from_version: str, to_version: str) -> List[SchemaDelta]:
        """Get the chain of deltas needed to transform between versions."""
        if subject not in self._deltas:
            return []
            
        versions = sorted(self._deltas[subject].keys())
        if from_version not in versions or to_version not in versions:
            return []
            
        from_idx = versions.index(from_version)
        to_idx = versions.index(to_version)
        
        if from_idx < to_idx:
            # Forward transformation
            return [
                self._deltas[subject][v]
                for v in versions[from_idx + 1:to_idx + 1]
            ]
        else:
            # Backward transformation
            return [
                self._deltas[subject][v].reverse()
                for v in reversed(versions[to_idx + 1:from_idx + 1])
            ]
            
    def _apply_delta(self, data: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a delta to transform data."""
        result = data.copy()
        
        # Remove fields
        for field in delta.get('removed', []):
            result.pop(field, None)
            
        # Add new fields with transformations
        for field, transform in delta.get('transformations', {}).items():
            try:
                result[field] = eval(transform, {'data': data})
            except Exception as e:
                raise ValueError(f"Error applying transformation for field {field}: {str(e)}")
                
        return result
        
    def _reverse_delta(self, delta: Dict[str, Any]) -> Dict[str, Any]:
        """Create a reverse delta for backward transformation."""
        return {
            'added': delta.get('removed', []),
            'removed': delta.get('added', []),
            'transformations': {}  # TODO: Add support for reverse transformations
        } 
"""
Schema Registry module for managing schema versions and deltas.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime

from .delta import SchemaDelta


class SchemaRegistry:
    """
    Manages schema versions and their deltas, providing version control and compatibility checking.
    """
    
    def __init__(self):
        self._schemas: Dict[str, Dict[str, SchemaDelta]] = {}
        self._versions: Dict[str, List[str]] = {}
        self._compatibility_matrix: Dict[str, Dict[str, bool]] = {}
        
    def register_delta(
        self,
        schema_name: str,
        version: str,
        delta: SchemaDelta,
        previous_version: Optional[str] = None
    ) -> None:
        """
        Register a new schema delta for a given schema name and version.
        
        Args:
            schema_name: Name of the schema
            version: Version identifier for this delta
            delta: SchemaDelta object containing the changes
            previous_version: Optional previous version this delta is based on
        """
        if schema_name not in self._schemas:
            self._schemas[schema_name] = {}
            self._versions[schema_name] = []
            
        if version in self._schemas[schema_name]:
            raise ValueError(f"Version {version} already exists for schema {schema_name}")
            
        self._schemas[schema_name][version] = delta
        self._versions[schema_name].append(version)
        self._versions[schema_name].sort()  # Keep versions in order
        
        # Update compatibility matrix
        self._update_compatibility_matrix(schema_name, version, previous_version)
        
    def get_delta(self, schema_name: str, version: str) -> Optional[SchemaDelta]:
        """
        Retrieve a schema delta for a given schema name and version.
        
        Args:
            schema_name: Name of the schema
            version: Version identifier
            
        Returns:
            SchemaDelta object if found, None otherwise
        """
        return self._schemas.get(schema_name, {}).get(version)
        
    def get_versions(self, schema_name: str) -> List[str]:
        """
        Get all versions for a given schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of version identifiers
        """
        return self._versions.get(schema_name, [])
        
    def transform_data(
        self,
        schema_name: str,
        data: Dict,
        from_version: str,
        to_version: str
    ) -> Dict:
        """
        Transform data between two schema versions.
        
        Args:
            schema_name: Name of the schema
            data: Data to transform
            from_version: Source version
            to_version: Target version
            
        Returns:
            Transformed data
        """
        if not self.is_compatible(schema_name, from_version, to_version):
            raise ValueError(f"Schema versions {from_version} and {to_version} are not compatible")
            
        # Get the chain of deltas to apply
        delta_chain = self._get_delta_chain(schema_name, from_version, to_version)
        
        # Apply each delta in sequence
        result = data
        for delta in delta_chain:
            result = delta.apply(result)
            
        return result
        
    def is_compatible(self, schema_name: str, version1: str, version2: str) -> bool:
        """
        Check if two schema versions are compatible.
        
        Args:
            schema_name: Name of the schema
            version1: First version
            version2: Second version
            
        Returns:
            True if versions are compatible, False otherwise
        """
        return self._compatibility_matrix.get(schema_name, {}).get(
            f"{version1}:{version2}", False
        )
        
    def _update_compatibility_matrix(
        self,
        schema_name: str,
        new_version: str,
        previous_version: Optional[str]
    ) -> None:
        """Update the compatibility matrix when a new version is registered."""
        if schema_name not in self._compatibility_matrix:
            self._compatibility_matrix[schema_name] = {}
            
        # A version is always compatible with itself
        self._compatibility_matrix[schema_name][f"{new_version}:{new_version}"] = True
        
        if previous_version:
            # New version is compatible with previous version
            self._compatibility_matrix[schema_name][f"{previous_version}:{new_version}"] = True
            
            # Previous version is compatible with new version (backward compatibility)
            self._compatibility_matrix[schema_name][f"{new_version}:{previous_version}"] = True
            
    def _get_delta_chain(
        self,
        schema_name: str,
        from_version: str,
        to_version: str
    ) -> List[SchemaDelta]:
        """Get the chain of deltas needed to transform between two versions."""
        versions = self._versions[schema_name]
        from_idx = versions.index(from_version)
        to_idx = versions.index(to_version)
        
        if from_idx < to_idx:
            # Forward transformation
            return [
                self._schemas[schema_name][v]
                for v in versions[from_idx + 1:to_idx + 1]
            ]
        else:
            # Backward transformation
            return [
                self._schemas[schema_name][v]
                for v in reversed(versions[to_idx + 1:from_idx + 1])
            ] 
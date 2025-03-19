"""
Schema Registry module for managing schema versions and deltas.
"""

from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from datetime import datetime

from .delta import SchemaDelta
from .metadata import SchemaMetadata

@dataclass
class SchemaVersion:
    version: str
    created_at: datetime
    schema: Dict[str, Any]
    delta: Optional[Dict[str, Any]] = None

class SchemaRegistry:
    """
    Manages schema versions and their deltas, providing version control and compatibility checking.
    """
    
    def __init__(self):
        self._schemas: Dict[str, Dict[str, SchemaDelta]] = {}
        self._metadata: Dict[str, Dict[str, SchemaMetadata]] = {}
        self._compatibility_matrix: Dict[str, Dict[str, bool]] = {}
        
    def register_schema(self, subject: str, version: str, delta: SchemaDelta, metadata: Optional[SchemaMetadata] = None) -> None:
        """Register a new schema version for a subject."""
        if subject not in self._schemas:
            self._schemas[subject] = {}
            self._metadata[subject] = {}
            
        if version in self._schemas[subject]:
            raise ValueError(f"Schema version {version} already exists for subject {subject}")
            
        self._schemas[subject][version] = delta
        self._metadata[subject][version] = metadata or SchemaMetadata(
            created_at=datetime.now().timestamp(),
            created_by="system",
            description=f"Schema version {version} for {subject}",
            tags=["auto-generated"]
        )
        
        # Update compatibility matrix
        self._update_compatibility_matrix(subject, version)
        
    def register_delta(
        self,
        subject: str,
        version: str,
        delta: SchemaDelta,
        metadata_or_base_version: Optional[Union[Dict[str, Any], str]] = None
    ) -> None:
        """Register a new schema delta for a subject."""
        if subject not in self._schemas:
            self._schemas[subject] = {}
            self._metadata[subject] = {}
            self._compatibility_matrix[subject] = {}
        
        if version in self._schemas[subject]:
            raise ValueError(f"Schema version {version} already exists for subject {subject}")
        
        self._schemas[subject][version] = delta
        
        # Handle metadata or base_version
        if isinstance(metadata_or_base_version, dict):
            # It's metadata
            metadata = metadata_or_base_version
            base_version = None
        elif isinstance(metadata_or_base_version, str):
            # It's a base version
            if metadata_or_base_version not in self._schemas[subject]:
                raise ValueError(f"Base version {metadata_or_base_version} does not exist for subject {subject}")
            metadata = {
                "created_at": datetime.now().timestamp(),
                "created_by": "system",
                "description": f"Schema version {version}",
                "tags": []
            }
            base_version = metadata_or_base_version
        else:
            # No metadata or base version provided
            metadata = {
                "created_at": datetime.now().timestamp(),
                "created_by": "system",
                "description": f"Schema version {version}",
                "tags": []
            }
            base_version = None
        
        self._metadata[subject][version] = SchemaMetadata(**metadata)
        
        # Update compatibility matrix
        self._update_compatibility_matrix(subject, version, base_version)
        
    def get_schema(self, subject: str, version: str) -> Tuple[Optional[SchemaDelta], Optional[Dict[str, Any]]]:
        """Get a schema delta and metadata for a subject and version."""
        if subject not in self._schemas or version not in self._schemas[subject]:
            return None, None
        delta = self._schemas[subject][version]
        metadata = self._metadata[subject][version].model_dump()
        return delta, metadata
        
    def get_latest_version(self, subject: str) -> Optional[str]:
        """Get the latest schema version for a subject."""
        if subject not in self._schemas:
            return None
        return max(self._schemas[subject].keys())
        
    def get_delta(self, subject: str, version: str) -> Optional[SchemaDelta]:
        """Get a schema delta for a subject and version."""
        return self._schemas.get(subject, {}).get(version)
        
    def get_metadata(self, subject: str, version: str) -> Optional[SchemaMetadata]:
        """Get metadata for a schema version."""
        if subject not in self._metadata or version not in self._metadata[subject]:
            return None
        return self._metadata[subject][version]
        
    def get_versions(self, subject: str) -> List[str]:
        """Get all versions for a subject."""
        if subject not in self._schemas:
            return []
        return sorted(self._schemas[subject].keys())
        
    def transform_data(self, subject: str, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform data between two schema versions."""
        if subject not in self._schemas:
            raise ValueError(f"Subject {subject} not found")
        
        if from_version == to_version:
            return data
        
        # Handle initial version case
        if from_version == "v0":
            result = data
            for version in self.get_versions(subject):
                if version == "v0":
                    continue
                delta = self._schemas[subject][version]
                result = delta.apply(result)
                # Remove old fields
                for field in delta.removed_fields:
                    if field in result:
                        del result[field]
            return result
        
        # Get delta chain
        deltas = self._get_delta_chain(subject, from_version, to_version)
        if not deltas:
            raise ValueError(f"No transformation path found from {from_version} to {to_version}")
        
        # Apply deltas
        result = data
        for delta in deltas:
            result = delta.apply(result)
            # Remove old fields
            for field in delta.removed_fields:
                if field in result:
                    del result[field]
        return result
        
    def check_compatibility(self, subject: str, from_version: str, to_version: str) -> bool:
        """Check if two schema versions are compatible."""
        if subject not in self._schemas:
            return False
            
        if from_version not in self._schemas[subject] or to_version not in self._schemas[subject]:
            return False
            
        # Get all versions between from_version and to_version
        versions = sorted(self._schemas[subject].keys())
        from_idx = versions.index(from_version)
        to_idx = versions.index(to_version)
        
        # Check if we can transform data between these versions
        try:
            # Try to get the delta chain
            deltas = self._get_delta_chain(subject, from_version, to_version)
            return len(deltas) > 0
        except ValueError:
            return False
        
    def _get_delta_chain(self, subject: str, from_version: str, to_version: str) -> List[SchemaDelta]:
        """Get the chain of deltas needed to transform between versions."""
        if subject not in self._schemas:
            return []
        
        versions = self.get_versions(subject)
        if from_version not in versions or to_version not in versions:
            return []
        
        # Get all versions between from_version and to_version
        from_idx = versions.index(from_version)
        to_idx = versions.index(to_version)
        
        if from_idx < to_idx:
            # Forward transformation
            return [self._schemas[subject][v] for v in versions[from_idx + 1:to_idx + 1]]
        else:
            # Reverse transformation
            return [self._schemas[subject][v].reverse() for v in reversed(versions[to_idx + 1:from_idx + 1])]
        
    def _find_path(self, subject: str, from_version: str, to_version: str) -> List[str]:
        """Find a path between two versions in the compatibility matrix."""
        if subject not in self._compatibility_matrix:
            return []
        
        visited = set()
        path = []
        
        def dfs(current: str) -> bool:
            if current == to_version:
                path.append(current)
                return True
            
            visited.add(current)
            for next_version in self._compatibility_matrix[subject].get(current, {}):
                if next_version not in visited and self._compatibility_matrix[subject][current][next_version]:
                    if dfs(next_version):
                        path.append(current)
                        return True
            return False
        
        if dfs(from_version):
            return list(reversed(path))
        return []
        
    def _update_compatibility_matrix(self, subject: str, version: str, base_version: Optional[str] = None) -> None:
        """Update the compatibility matrix when registering a new version."""
        if subject not in self._compatibility_matrix:
            self._compatibility_matrix[subject] = {}
        
        if version not in self._compatibility_matrix[subject]:
            self._compatibility_matrix[subject][version] = {}
        
        # Version is compatible with itself
        self._compatibility_matrix[subject][version][version] = True
        
        # Get all versions
        versions = sorted(self._schemas[subject].keys())
        version_idx = versions.index(version)
        
        # All previous versions are compatible with this version
        for prev_version in versions[:version_idx]:
            self._compatibility_matrix[subject][prev_version][version] = True
            
        # This version is compatible with all future versions
        for next_version in versions[version_idx + 1:]:
            self._compatibility_matrix[subject][version][next_version] = True 
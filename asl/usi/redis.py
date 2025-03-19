"""
Redis-based Universal Schema Index implementation.
"""

import json
from typing import Dict, List, Optional, Set, Any
import redis
from pydantic import BaseModel

from ..core.delta import SchemaDelta
from ..core.metadata import SchemaMetadata


class RedisUSI:
    """
    Redis-based Universal Schema Index implementation.
    Provides distributed storage and retrieval of schema deltas and metadata.
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "asl:"
    ):
        """
        Initialize the Redis USI.
        
        Args:
            redis_url: Redis connection URL
            key_prefix: Prefix for all Redis keys
        """
        self.redis = redis.from_url(redis_url)
        self.prefix = key_prefix
        
    def _get_key(self, *parts: str) -> str:
        """Generate a Redis key with the prefix."""
        return f"{self.prefix}{':'.join(parts)}"
        
    def register_schema(
        self,
        schema_name: str,
        version: str,
        delta: SchemaDelta,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Register a new schema version with its delta and metadata.

        Args:
            schema_name: Name of the schema
            version: Version identifier
            delta: SchemaDelta object
            metadata: Optional metadata about the schema version
        """
        # Store the delta
        delta_key = self._get_key("delta", schema_name, version)
        self.redis.set(delta_key, delta.model_dump_json())

        # Store metadata
        if metadata:
            meta_key = self._get_key("meta", schema_name, version)
            meta_obj = SchemaMetadata(**metadata)
            self.redis.set(meta_key, meta_obj.model_dump_json())

        # Add version to the set of versions
        versions_key = self._get_key("versions", schema_name)
        self.redis.sadd(versions_key, version)

        # Update compatibility matrix
        self._update_compatibility_matrix(schema_name, version)

    def get_delta(self, schema_name: str, version: str) -> Optional[SchemaDelta]:
        """
        Get a schema delta by name and version.

        Args:
            schema_name: Name of the schema
            version: Version identifier

        Returns:
            SchemaDelta object if found, None otherwise
        """
        delta_key = self._get_key("delta", schema_name, version)
        delta_json = self.redis.get(delta_key)
        if not delta_json:
            return None
        return SchemaDelta.model_validate_json(delta_json.decode())

    def get_metadata(self, schema_name: str, version: str) -> Optional[Dict[str, Any]]:
        """
        Get schema metadata by name and version.

        Args:
            schema_name: Name of the schema
            version: Version identifier

        Returns:
            Metadata dictionary if found, None otherwise
        """
        meta_key = self._get_key("meta", schema_name, version)
        meta_json = self.redis.get(meta_key)
        if not meta_json:
            return None
        return SchemaMetadata.model_validate_json(meta_json.decode()).model_dump()

    def get_versions(self, schema_name: str) -> List[str]:
        """
        Get all versions for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of version identifiers
        """
        versions_key = self._get_key("versions", schema_name)
        versions = self.redis.smembers(versions_key)
        return sorted([v.decode() for v in versions])

    def is_compatible(self, schema_name: str, version1: str, version2: str) -> bool:
        """
        Check if two schema versions are compatible.

        Args:
            schema_name: Name of the schema
            version1: First version
            version2: Second version

        Returns:
            True if compatible, False otherwise
        """
        # Get the deltas
        delta1 = self.get_delta(schema_name, version1)
        delta2 = self.get_delta(schema_name, version2)
        if not delta1 or not delta2:
            return False

        # Check compatibility
        # For now, we'll say they're compatible if we can transform data between them
        try:
            chain = self.get_delta_chain(schema_name, version1, version2)
            return bool(chain)
        except Exception:
            return False

    def get_compatibility_matrix(self, schema_name: str) -> Dict[str, Dict[str, bool]]:
        """
        Get the compatibility matrix for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            Dictionary mapping version pairs to compatibility status
        """
        matrix_key = self._get_key("matrix", schema_name)
        matrix_json = self.redis.get(matrix_key)
        if matrix_json:
            return json.loads(matrix_json.decode())
        
        # If no matrix exists, create one
        versions = self.get_versions(schema_name)
        matrix = {}
        for v1 in versions:
            matrix[v1] = {}
            for v2 in versions:
                if v1 == v2:
                    matrix[v1][v2] = True
                else:
                    matrix[v1][v2] = self.is_compatible(schema_name, v1, v2)
        
        # Store the matrix
        self.redis.set(matrix_key, json.dumps(matrix))
        return matrix

    def get_delta_chain(
        self,
        schema_name: str,
        from_version: str,
        to_version: str
    ) -> List[SchemaDelta]:
        """
        Get the chain of deltas needed to transform data from one version to another.

        Args:
            schema_name: Name of the schema
            from_version: Source version
            to_version: Target version

        Returns:
            List of SchemaDelta objects
        """
        # For now, we'll just return a direct path if both versions exist
        if from_version == to_version:
            return []

        # Get the deltas
        delta1 = self.get_delta(schema_name, from_version)
        delta2 = self.get_delta(schema_name, to_version)
        if not delta1 or not delta2:
            raise ValueError(f"One or both versions not found: {from_version}, {to_version}")

        # Return the direct path
        return [delta2]

    def _update_compatibility_matrix(self, schema_name: str, new_version: str) -> None:
        """Update the compatibility matrix when a new version is added."""
        # Get all versions
        versions = self.get_versions(schema_name)
        
        # Initialize matrix for new version
        matrix_key = self._get_key("matrix", schema_name)
        matrix = {}
        matrix_json = self.redis.get(matrix_key)
        if matrix_json:
            matrix = json.loads(matrix_json.decode())
        
        # Initialize matrix for all versions
        for version in versions:
            if version not in matrix:
                matrix[version] = {}
        
        # Version is compatible with itself
        matrix[new_version][new_version] = True
        
        # Check compatibility with all other versions
        for version in versions:
            if version != new_version:
                # Previous versions are compatible with new version
                matrix[version][new_version] = True
                # New version is not compatible with previous versions
                matrix[new_version][version] = False
        
        # Store updated matrix
        self.redis.set(matrix_key, json.dumps(matrix)) 
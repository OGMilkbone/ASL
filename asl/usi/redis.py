"""
Redis-based Universal Schema Index implementation.
"""

import json
from typing import Dict, List, Optional, Set
import redis
from pydantic import BaseModel

from ..core.delta import SchemaDelta


class SchemaMetadata(BaseModel):
    """Metadata about a schema version."""
    created_at: float
    created_by: str
    description: Optional[str] = None
    tags: List[str] = []
    is_deprecated: bool = False


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
        metadata: Optional[SchemaMetadata] = None
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
        self.redis.set(delta_key, delta.json())
        
        # Store metadata
        if metadata:
            meta_key = self._get_key("meta", schema_name, version)
            self.redis.set(meta_key, metadata.json())
            
        # Add to version set
        versions_key = self._get_key("versions", schema_name)
        self.redis.sadd(versions_key, version)
        
        # Update compatibility matrix
        self._update_compatibility_matrix(schema_name, version)
        
    def get_delta(self, schema_name: str, version: str) -> Optional[SchemaDelta]:
        """
        Retrieve a schema delta.
        
        Args:
            schema_name: Name of the schema
            version: Version identifier
            
        Returns:
            SchemaDelta object if found, None otherwise
        """
        delta_key = self._get_key("delta", schema_name, version)
        delta_json = self.redis.get(delta_key)
        if delta_json:
            return SchemaDelta.parse_raw(delta_json)
        return None
        
    def get_metadata(self, schema_name: str, version: str) -> Optional[SchemaMetadata]:
        """
        Retrieve schema metadata.
        
        Args:
            schema_name: Name of the schema
            version: Version identifier
            
        Returns:
            SchemaMetadata object if found, None otherwise
        """
        meta_key = self._get_key("meta", schema_name, version)
        meta_json = self.redis.get(meta_key)
        if meta_json:
            return SchemaMetadata.parse_raw(meta_json)
        return None
        
    def get_versions(self, schema_name: str) -> Set[str]:
        """
        Get all versions for a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            Set of version identifiers
        """
        versions_key = self._get_key("versions", schema_name)
        return {v.decode() for v in self.redis.smembers(versions_key)}
        
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
        compat_key = self._get_key("compat", schema_name, f"{version1}:{version2}")
        return bool(self.redis.get(compat_key))
        
    def _update_compatibility_matrix(self, schema_name: str, new_version: str) -> None:
        """Update the compatibility matrix when a new version is registered."""
        versions = self.get_versions(schema_name)
        
        # A version is always compatible with itself
        compat_key = self._get_key("compat", schema_name, f"{new_version}:{new_version}")
        self.redis.set(compat_key, "1")
        
        # Get the previous version (assuming versions are ordered)
        versions_list = sorted(list(versions))
        if len(versions_list) > 1:
            prev_version = versions_list[-2]  # Second to last version
            
            # New version is compatible with previous version
            compat_key = self._get_key("compat", schema_name, f"{prev_version}:{new_version}")
            self.redis.set(compat_key, "1")
            
            # Previous version is compatible with new version (backward compatibility)
            compat_key = self._get_key("compat", schema_name, f"{new_version}:{prev_version}")
            self.redis.set(compat_key, "1")
            
    def get_delta_chain(
        self,
        schema_name: str,
        from_version: str,
        to_version: str
    ) -> List[SchemaDelta]:
        """
        Get the chain of deltas needed to transform between two versions.
        
        Args:
            schema_name: Name of the schema
            from_version: Source version
            to_version: Target version
            
        Returns:
            List of SchemaDelta objects
        """
        versions = sorted(list(self.get_versions(schema_name)))
        from_idx = versions.index(from_version)
        to_idx = versions.index(to_version)
        
        if from_idx < to_idx:
            # Forward transformation
            return [
                self.get_delta(schema_name, v)
                for v in versions[from_idx + 1:to_idx + 1]
                if self.get_delta(schema_name, v)
            ]
        else:
            # Backward transformation
            return [
                self.get_delta(schema_name, v)
                for v in reversed(versions[to_idx + 1:from_idx + 1])
                if self.get_delta(schema_name, v)
            ] 
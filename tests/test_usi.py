"""
Tests for the Redis-based Universal Schema Index.
"""

import pytest
import redis
from datetime import datetime

from asl.usi.redis import RedisUSI, SchemaMetadata
from asl.core.delta import SchemaDelta


@pytest.fixture
def redis_client():
    """Create a Redis client for testing."""
    client = redis.from_url("redis://localhost:6379/1")  # Use database 1 for testing
    client.flushdb()  # Clear the database before each test
    return client


@pytest.fixture
def usi(redis_client):
    """Create a RedisUSI instance for testing."""
    return RedisUSI(redis_url="redis://localhost:6379/1")


def test_register_and_retrieve_schema(usi):
    """Test registering and retrieving a schema."""
    # Create a schema delta
    delta = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    # Create metadata
    metadata = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test_user",
        description="Test schema version",
        tags=["test", "user"]
    )
    
    # Register the schema
    usi.register_schema("user", "v1", delta, metadata)
    
    # Retrieve and verify
    retrieved_delta = usi.get_delta("user", "v1")
    assert retrieved_delta is not None
    assert retrieved_delta.added == delta.added
    assert retrieved_delta.removed == delta.removed
    assert retrieved_delta.transformations == delta.transformations
    
    retrieved_metadata = usi.get_metadata("user", "v1")
    assert retrieved_metadata is not None
    assert retrieved_metadata.created_by == metadata.created_by
    assert retrieved_metadata.description == metadata.description
    assert retrieved_metadata.tags == metadata.tags


def test_version_management(usi):
    """Test version management functionality."""
    # Create and register multiple versions
    delta1 = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    delta2 = SchemaDelta(
        added=["email"],
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )
    
    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)
    
    # Get versions
    versions = usi.get_versions("user")
    assert len(versions) == 2
    assert "v1" in versions
    assert "v2" in versions


def test_compatibility_matrix(usi):
    """Test compatibility matrix functionality."""
    # Create and register multiple versions
    delta1 = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    delta2 = SchemaDelta(
        added=["email"],
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )
    
    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)
    
    # Check compatibility
    assert usi.is_compatible("user", "v1", "v1")  # Same version
    assert usi.is_compatible("user", "v1", "v2")  # Forward compatibility
    assert usi.is_compatible("user", "v2", "v1")  # Backward compatibility


def test_delta_chain(usi):
    """Test delta chain functionality."""
    # Create and register multiple versions
    delta1 = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    delta2 = SchemaDelta(
        added=["email"],
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )
    
    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)
    
    # Get delta chain
    chain = usi.get_delta_chain("user", "v1", "v2")
    assert len(chain) == 1
    assert chain[0].added == ["email"]
    
    # Test backward chain
    chain = usi.get_delta_chain("user", "v2", "v1")
    assert len(chain) == 1
    assert chain[0].added == ["email"] 
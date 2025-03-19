"""
Tests for USI functionality.
"""

import pytest
from datetime import datetime
from asl.core.delta import SchemaDelta
from asl.core.metadata import SchemaMetadata
from asl.usi.redis import RedisUSI

@pytest.fixture
def redis_client():
    """Create a Redis client for testing."""
    import redis
    client = redis.from_url("redis://localhost:6379/0")
    yield client
    # Clear all test keys after each test
    for key in client.keys("asl:*"):
        client.delete(key)

@pytest.fixture
def usi(redis_client):
    """Create a Redis USI instance for testing."""
    return RedisUSI(redis_url="redis://localhost:6379/0")

def test_register_and_retrieve_schema(usi):
    """Test registering and retrieving a schema."""
    # Create a schema delta
    delta = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
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
    usi.register_schema("user", "v1", delta, metadata.model_dump())

    # Retrieve and verify
    retrieved_delta = usi.get_delta("user", "v1")
    assert retrieved_delta is not None
    assert retrieved_delta.added_fields == delta.added_fields
    assert retrieved_delta.removed_fields == delta.removed_fields
    assert retrieved_delta.transformations == delta.transformations

def test_version_management(usi):
    """Test version management functionality."""
    # Register multiple versions
    delta1 = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )

    delta2 = SchemaDelta(
        added_fields={"email": "string"},
        removed_fields={},
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )

    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)

    # Test version listing
    versions = usi.get_versions("user")
    assert versions == ["v1", "v2"]

    # Test version retrieval
    retrieved_delta1 = usi.get_delta("user", "v1")
    retrieved_delta2 = usi.get_delta("user", "v2")
    assert retrieved_delta1 == delta1
    assert retrieved_delta2 == delta2

def test_compatibility_matrix(usi):
    """Test compatibility matrix functionality."""
    # Register multiple versions
    delta1 = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )

    delta2 = SchemaDelta(
        added_fields={"email": "string"},
        removed_fields={},
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )

    delta3 = SchemaDelta(
        added_fields={"phone": "string"},
        removed_fields={"email": "string"},
        transformations={
            "phone": "concat('+1-', email)"
        }
    )

    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)
    usi.register_schema("user", "v3", delta3)

    # Test compatibility matrix
    matrix = usi.get_compatibility_matrix("user")
    assert matrix == {
        "v1": {"v1": True, "v2": True, "v3": True},
        "v2": {"v1": False, "v2": True, "v3": True},
        "v3": {"v1": False, "v2": False, "v3": True}
    }

def test_delta_chain(usi):
    """Test delta chain functionality."""
    # Create and register multiple versions
    delta1 = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )

    delta2 = SchemaDelta(
        added_fields={"email": "string"},
        removed_fields={},
        transformations={
            "email": "concat(firstName, '.', lastName, '@example.com')"
        }
    )

    usi.register_schema("user", "v1", delta1)
    usi.register_schema("user", "v2", delta2)

    # Get delta chain
    chain = usi.get_delta_chain("user", "v1", "v2")
    assert len(chain) == 1
    assert chain[0].added_fields == {"email": "string"}
    assert chain[0].removed_fields == {}
    assert chain[0].transformations == {
        "email": "concat(firstName, '.', lastName, '@example.com')"
    } 
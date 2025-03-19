"""
Tests for API functionality.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
from asl.api.server import app, get_registry
from asl.core.delta import SchemaDelta
from asl.core.metadata import SchemaMetadata
from asl.core.registry import SchemaRegistry

@pytest.fixture
def registry():
    """Create a fresh registry for each test."""
    return SchemaRegistry()

@pytest.fixture
def client(registry):
    """Create a test client for the API with a fresh registry."""
    app.dependency_overrides = {}
    app.dependency_overrides[get_registry] = lambda: registry
    return TestClient(app)

def test_register_schema(client):
    """Test registering a schema."""
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
        created_by="test",
        description="Test schema",
        tags=["test"]
    )

    # Register the schema
    response = client.post(
        "/schemas/user/versions/v1",
        json={
            "delta": delta.model_dump(),
            "metadata": metadata.model_dump()
        }
    )
    assert response.status_code == 200
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["version"] == "v1"
    assert result["delta"]["added_fields"] == delta.added_fields
    assert result["delta"]["removed_fields"] == delta.removed_fields
    assert result["delta"]["transformations"] == delta.transformations
    assert result["metadata"]["created_by"] == "test"
    assert result["metadata"]["description"] == "Test schema"

def test_get_versions(client):
    """Test getting schema versions."""
    # First register a schema
    delta = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )

    metadata = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Test schema",
        tags=["test"]
    )

    client.post(
        "/schemas/user/versions/v1",
        json={
            "delta": delta.model_dump(),
            "metadata": metadata.model_dump()
        }
    )

    # Then get versions
    response = client.get("/schemas/user/versions")
    assert response.status_code == 200
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["versions"] == ["v1"]

def test_get_schema(client):
    """Test getting a specific schema version."""
    # First register a schema
    delta = SchemaDelta(
        added_fields={"firstName": "string", "lastName": "string"},
        removed_fields={"name": "string"},
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )

    metadata = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Test schema",
        tags=["test"]
    )

    client.post(
        "/schemas/user/versions/v1",
        json={
            "delta": delta.model_dump(),
            "metadata": metadata.model_dump()
        }
    )

    # Then get the schema
    response = client.get("/schemas/user/versions/v1")
    assert response.status_code == 200
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["version"] == "v1"
    assert result["delta"]["added_fields"] == delta.added_fields
    assert result["delta"]["removed_fields"] == delta.removed_fields
    assert result["delta"]["transformations"] == delta.transformations
    assert result["metadata"]["created_by"] == "test"
    assert result["metadata"]["description"] == "Test schema"

def test_check_compatibility(client):
    """Test checking schema compatibility."""
    # First register two versions
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

    metadata1 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Initial schema",
        tags=["test"]
    )

    metadata2 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Add email field",
        tags=["test"]
    )

    client.post(
        "/schemas/user/versions/v1",
        json={
            "delta": delta1.model_dump(),
            "metadata": metadata1.model_dump()
        }
    )

    client.post(
        "/schemas/user/versions/v2",
        json={
            "delta": delta2.model_dump(),
            "metadata": metadata2.model_dump()
        }
    )

    # Then check compatibility
    response = client.get(
        "/schemas/user/compatibility",
        params={"from_version": "v1", "to_version": "v2"}
    )
    assert response.status_code == 200
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["from_version"] == "v1"
    assert result["to_version"] == "v2"
    assert result["compatible"] is True

def test_transform_data(client):
    """Test transforming data between schema versions."""
    # First register two versions
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

    metadata1 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Initial schema",
        tags=["test"]
    )

    metadata2 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Add email field",
        tags=["test"]
    )

    client.post(
        "/schemas/user/versions/v1",
        json={
            "delta": delta1.model_dump(),
            "metadata": metadata1.model_dump()
        }
    )

    client.post(
        "/schemas/user/versions/v2",
        json={
            "delta": delta2.model_dump(),
            "metadata": metadata2.model_dump()
        }
    )

    # Then transform data
    data = {"firstName": "John", "lastName": "Doe"}
    response = client.post(
        "/schemas/user/transform",
        json={
            "data": data,
            "from_version": "v1",
            "to_version": "v2"
        }
    )
    assert response.status_code == 200
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["from_version"] == "v1"
    assert result["to_version"] == "v2"
    assert result["transformed_data"] == {
        "firstName": "John",
        "lastName": "Doe",
        "email": "John.Doe@example.com"
    } 
"""
Tests for the ASL API server.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from asl.api.server import app
from asl.core.delta import SchemaDelta
from asl.usi.redis import SchemaMetadata


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


def test_register_schema(client):
    """Test schema registration endpoint."""
    # Create test data
    delta = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    metadata = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test_user",
        description="Test schema version",
        tags=["test", "user"]
    )
    
    request_data = {
        "schema_name": "user",
        "version": "v1",
        "delta": delta.dict(),
        "metadata": metadata.dict()
    }
    
    # Make request
    response = client.post("/schemas", json=request_data)
    assert response.status_code == 200
    
    # Verify response
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["version"] == "v1"
    assert result["delta"]["added"] == ["firstName", "lastName"]
    assert result["delta"]["removed"] == ["name"]
    assert result["metadata"]["created_by"] == "test_user"


def test_get_versions(client):
    """Test getting schema versions endpoint."""
    # First register a schema
    delta = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    request_data = {
        "schema_name": "user",
        "version": "v1",
        "delta": delta.dict()
    }
    
    client.post("/schemas", json=request_data)
    
    # Get versions
    response = client.get("/schemas/user/versions")
    assert response.status_code == 200
    
    result = response.json()
    assert result["schema_name"] == "user"
    assert "v1" in result["versions"]


def test_get_schema(client):
    """Test getting specific schema version endpoint."""
    # First register a schema
    delta = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    request_data = {
        "schema_name": "user",
        "version": "v1",
        "delta": delta.dict()
    }
    
    client.post("/schemas", json=request_data)
    
    # Get specific version
    response = client.get("/schemas/user/versions/v1")
    assert response.status_code == 200
    
    result = response.json()
    assert result["schema_name"] == "user"
    assert result["version"] == "v1"
    assert result["delta"]["added"] == ["firstName", "lastName"]


def test_check_compatibility(client):
    """Test compatibility check endpoint."""
    # Register two versions
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
    
    # Register first version
    request_data1 = {
        "schema_name": "user",
        "version": "v1",
        "delta": delta1.dict()
    }
    client.post("/schemas", json=request_data1)
    
    # Register second version
    request_data2 = {
        "schema_name": "user",
        "version": "v2",
        "delta": delta2.dict()
    }
    client.post("/schemas", json=request_data2)
    
    # Check compatibility
    response = client.get("/schemas/user/compatibility?version1=v1&version2=v2")
    assert response.status_code == 200
    
    result = response.json()
    assert result["is_compatible"] is True


def test_transform_data(client):
    """Test data transformation endpoint."""
    # Register two versions
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
    
    # Register versions
    request_data1 = {
        "schema_name": "user",
        "version": "v1",
        "delta": delta1.dict()
    }
    client.post("/schemas", json=request_data1)
    
    request_data2 = {
        "schema_name": "user",
        "version": "v2",
        "delta": delta2.dict()
    }
    client.post("/schemas", json=request_data2)
    
    # Transform data
    data = {"userId": 123, "name": "John Doe"}
    response = client.post(
        "/schemas/user/transform?from_version=v0&to_version=v2",
        json=data
    )
    assert response.status_code == 200
    
    result = response.json()
    assert result["firstName"] == "John"
    assert result["lastName"] == "Doe"
    assert result["email"] == "John.Doe@example.com"
    assert result["userId"] == 123 
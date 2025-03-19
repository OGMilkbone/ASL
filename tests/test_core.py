"""
Tests for core functionality.
"""

import pytest
from asl.core.registry import SchemaRegistry
from asl.core.delta import SchemaDelta
from asl.core.transformer import SchemaTransformer
from asl.core.metadata import SchemaMetadata
from datetime import datetime


def test_schema_delta():
    """Test schema delta functionality."""
    # Create a schema delta
    delta = SchemaDelta(
        added_fields={"email": "string"},
        removed_fields={"firstName": "string"},
        transformations={"email": "firstName"}
    )
    
    # Test applying the delta
    data = {"firstName": "John Doe"}
    result = delta.apply(data)
    assert "firstName" not in result
    assert result["email"] == "John Doe"
    
    # Test reversing the delta
    reverse_delta = delta.reverse()
    assert reverse_delta.added_fields == {"firstName": "string"}
    assert reverse_delta.removed_fields == {"email": "string"}
    assert reverse_delta.transformations == {"firstName": "email"}


def test_schema_registry():
    """Test schema registry functionality."""
    registry = SchemaRegistry()
    
    # Register initial schema
    delta1 = SchemaDelta(
        added_fields={"name": "string"},
        removed_fields={},
        transformations={}
    )
    metadata1 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Initial schema",
        tags=["v1"]
    )
    registry.register_delta("user", "v1", delta1, metadata1.model_dump())
    
    # Register second version
    delta2 = SchemaDelta(
        added_fields={"email": "string"},
        removed_fields={"name": "string"},
        transformations={"email": "name"}
    )
    metadata2 = SchemaMetadata(
        created_at=datetime.now().timestamp(),
        created_by="test",
        description="Add email field",
        tags=["v2"]
    )
    registry.register_delta("user", "v2", delta2, metadata2.model_dump())
    
    # Test version retrieval
    versions = registry.get_versions("user")
    assert versions == ["v1", "v2"]
    
    # Test schema retrieval
    delta, metadata = registry.get_schema("user", "v2")
    assert delta == delta2
    assert metadata["created_by"] == "test"
    assert metadata["description"] == "Add email field"
    
    # Test compatibility checking
    assert registry.check_compatibility("user", "v1", "v2")
    
    # Test data transformation
    data = {"name": "John Doe"}
    result = registry.transform_data("user", data, "v1", "v2")
    assert result == {"email": "John Doe"}


def test_schema_transformer():
    """Test schema transformer functionality."""
    registry = SchemaRegistry()
    transformer = SchemaTransformer(registry)
    
    # Test basic field access
    data = {"user": {"name": "John Doe"}}
    result = transformer.transform(data, "user.name")
    assert result == "John Doe"
    
    # Test nested field access
    data = {"user": {"address": {"street": "123 Main St"}}}
    result = transformer.transform(data, "user.address.street")
    assert result == "123 Main St"
    
    # Test missing field
    result = transformer.transform(data, "user.email")
    assert result is None
    
    # Test invalid path
    result = transformer.transform(data, "user.address.invalid.field")
    assert result is None
    
    # Test empty path
    result = transformer.transform(data, "")
    assert result is None
    
    # Test non-dictionary value
    data = {"value": 42}
    result = transformer.transform(data, "value.invalid")
    assert result is None 
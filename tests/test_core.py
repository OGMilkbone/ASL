"""
Tests for the core ASL functionality.
"""

import pytest
from asl.core.registry import SchemaRegistry
from asl.core.delta import SchemaDelta
from asl.core.transform import SchemaTransformer


def test_schema_delta():
    """Test basic schema delta functionality."""
    delta = SchemaDelta(
        added=["firstName", "lastName"],
        removed=["name"],
        transformations={
            "firstName": "split(name, ' ')[0]",
            "lastName": "split(name, ' ')[1]"
        }
    )
    
    # Test applying delta
    data = {"userId": 123, "name": "John Doe"}
    result = delta.apply(data)
    
    assert "name" not in result
    assert result["firstName"] == "John"
    assert result["lastName"] == "Doe"
    assert result["userId"] == 123
    
    # Test reversing delta
    reversed_data = delta.reverse(result)
    assert "name" in reversed_data
    assert reversed_data["name"] is None
    assert "firstName" not in reversed_data
    assert "lastName" not in reversed_data


def test_schema_registry():
    """Test schema registry functionality."""
    registry = SchemaRegistry()
    
    # Create and register deltas
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
    
    registry.register_delta("user", "v1", delta1)
    registry.register_delta("user", "v2", delta2, "v1")
    
    # Test version management
    versions = registry.get_versions("user")
    assert len(versions) == 2
    assert "v1" in versions
    assert "v2" in versions
    
    # Test data transformation
    data = {"userId": 123, "name": "John Doe"}
    result = registry.transform_data("user", data, "v0", "v2")
    
    assert result["firstName"] == "John"
    assert result["lastName"] == "Doe"
    assert result["email"] == "John.Doe@example.com"
    assert result["userId"] == 123


def test_schema_transformer():
    """Test schema transformer functionality."""
    transformer = SchemaTransformer()
    
    # Test basic field access
    data = {"user": {"name": "John Doe"}}
    result = transformer.transform(data, "user.name")
    assert result == "John Doe"
    
    # Test function calls
    result = transformer.transform(data, "split(user.name, ' ')")
    assert result == ["John", "Doe"]
    
    # Test concat function
    result = transformer.transform(data, "concat(user.name, ' is awesome')")
    assert result == "John Doe is awesome"
    
    # Test caching
    result1 = transformer.transform(data, "user.name")
    result2 = transformer.transform(data, "user.name")
    assert result1 == result2 
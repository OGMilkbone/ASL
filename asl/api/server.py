"""
FastAPI server for schema management.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, ConfigDict
import traceback

from ..core.registry import SchemaRegistry
from ..core.delta import SchemaDelta
from ..core.metadata import SchemaMetadata

app = FastAPI(title="Schema Management API", version="1.0.0")

# Create a persistent registry instance
registry = SchemaRegistry()

class SchemaRegistrationRequest(BaseModel):
    """Request model for schema registration."""
    delta: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

class SchemaResponse(BaseModel):
    """Response model for schema operations."""
    schema_name: str
    version: str
    delta: Dict[str, Any]
    metadata: Dict[str, Any]
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

class VersionListResponse(BaseModel):
    """Response model for version list."""
    schema_name: str
    versions: List[str]

class CompatibilityResponse(BaseModel):
    """Response model for compatibility check."""
    schema_name: str
    from_version: str
    to_version: str
    compatible: bool

class TransformRequest(BaseModel):
    """Request model for data transformation."""
    data: Dict[str, Any]
    from_version: str
    to_version: str

class TransformResponse(BaseModel):
    """Response model for data transformation."""
    schema_name: str
    from_version: str
    to_version: str
    transformed_data: Dict[str, Any]

def get_registry() -> SchemaRegistry:
    """Get the persistent registry instance."""
    return registry

@app.post("/schemas/{schema_name}/versions/{version}", response_model=SchemaResponse)
async def register_schema(
    schema_name: str,
    version: str,
    request: SchemaRegistrationRequest,
    registry: SchemaRegistry = Depends(get_registry)
):
    """Register a new schema version."""
    try:
        delta = SchemaDelta(**request.delta)
        metadata = SchemaMetadata(**request.metadata) if request.metadata else None
        registry.register_delta(schema_name, version, delta, metadata.model_dump() if metadata else None)
        return {
            "schema_name": schema_name,
            "version": version,
            "delta": delta.model_dump(),
            "metadata": metadata.model_dump() if metadata else {}
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/schemas/{schema_name}/versions", response_model=VersionListResponse)
async def get_versions(
    schema_name: str,
    registry: SchemaRegistry = Depends(get_registry)
):
    """Get all versions of a schema."""
    try:
        versions = registry.get_versions(schema_name)
        return {
            "schema_name": schema_name,
            "versions": list(versions)
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/schemas/{schema_name}/versions/{version}", response_model=SchemaResponse)
async def get_schema(
    schema_name: str,
    version: str,
    registry: SchemaRegistry = Depends(get_registry)
):
    """Get a specific schema version."""
    try:
        delta, metadata = registry.get_schema(schema_name, version)
        return {
            "schema_name": schema_name,
            "version": version,
            "delta": delta.model_dump(),
            "metadata": metadata if metadata else {}
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/schemas/{schema_name}/compatibility", response_model=CompatibilityResponse)
async def check_compatibility(
    schema_name: str,
    from_version: str = Query(..., description="Source version"),
    to_version: str = Query(..., description="Target version"),
    registry: SchemaRegistry = Depends(get_registry)
):
    """Check compatibility between two schema versions."""
    try:
        compatible = registry.check_compatibility(schema_name, from_version, to_version)
        return {
            "schema_name": schema_name,
            "from_version": from_version,
            "to_version": to_version,
            "compatible": compatible
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/schemas/{schema_name}/transform", response_model=TransformResponse)
async def transform_data(
    schema_name: str,
    request: TransformRequest,
    registry: SchemaRegistry = Depends(get_registry)
):
    """Transform data from one schema version to another."""
    try:
        transformed = registry.transform_data(
            schema_name,
            request.data,
            request.from_version,
            request.to_version
        )
        return {
            "schema_name": schema_name,
            "from_version": request.from_version,
            "to_version": request.to_version,
            "transformed_data": transformed
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e)) 
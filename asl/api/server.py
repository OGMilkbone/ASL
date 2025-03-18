"""
FastAPI server for ASL schema management.
"""

from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime

from ..usi.redis import RedisUSI, SchemaMetadata
from ..core.delta import SchemaDelta


app = FastAPI(
    title="Adaptive Schema Layer API",
    description="API for managing schema evolution in distributed systems",
    version="0.1.0"
)


class SchemaRegistrationRequest(BaseModel):
    """Request model for schema registration."""
    schema_name: str
    version: str
    delta: SchemaDelta
    metadata: Optional[SchemaMetadata] = None


class SchemaResponse(BaseModel):
    """Response model for schema information."""
    schema_name: str
    version: str
    delta: SchemaDelta
    metadata: Optional[SchemaMetadata]


class VersionListResponse(BaseModel):
    """Response model for version list."""
    schema_name: str
    versions: List[str]


class CompatibilityResponse(BaseModel):
    """Response model for compatibility check."""
    is_compatible: bool
    message: str


def get_usi() -> RedisUSI:
    """Dependency to get RedisUSI instance."""
    return RedisUSI()


@app.post("/schemas", response_model=SchemaResponse)
async def register_schema(
    request: SchemaRegistrationRequest,
    usi: RedisUSI = Depends(get_usi)
):
    """
    Register a new schema version.
    
    Args:
        request: Schema registration request
        usi: RedisUSI instance
        
    Returns:
        Registered schema information
    """
    try:
        # If no metadata provided, create default metadata
        if not request.metadata:
            request.metadata = SchemaMetadata(
                created_at=datetime.now().timestamp(),
                created_by="api",
                description=f"Schema version {request.version} for {request.schema_name}"
            )
            
        # Register the schema
        usi.register_schema(
            request.schema_name,
            request.version,
            request.delta,
            request.metadata
        )
        
        return SchemaResponse(
            schema_name=request.schema_name,
            version=request.version,
            delta=request.delta,
            metadata=request.metadata
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/schemas/{schema_name}/versions", response_model=VersionListResponse)
async def get_versions(
    schema_name: str,
    usi: RedisUSI = Depends(get_usi)
):
    """
    Get all versions for a schema.
    
    Args:
        schema_name: Name of the schema
        usi: RedisUSI instance
        
    Returns:
        List of version identifiers
    """
    versions = usi.get_versions(schema_name)
    if not versions:
        raise HTTPException(
            status_code=404,
            detail=f"No versions found for schema {schema_name}"
        )
    return VersionListResponse(schema_name=schema_name, versions=list(versions))


@app.get("/schemas/{schema_name}/versions/{version}", response_model=SchemaResponse)
async def get_schema(
    schema_name: str,
    version: str,
    usi: RedisUSI = Depends(get_usi)
):
    """
    Get schema information for a specific version.
    
    Args:
        schema_name: Name of the schema
        version: Version identifier
        usi: RedisUSI instance
        
    Returns:
        Schema information
    """
    delta = usi.get_delta(schema_name, version)
    if not delta:
        raise HTTPException(
            status_code=404,
            detail=f"Schema {schema_name} version {version} not found"
        )
        
    metadata = usi.get_metadata(schema_name, version)
    
    return SchemaResponse(
        schema_name=schema_name,
        version=version,
        delta=delta,
        metadata=metadata
    )


@app.get("/schemas/{schema_name}/compatibility", response_model=CompatibilityResponse)
async def check_compatibility(
    schema_name: str,
    version1: str,
    version2: str,
    usi: RedisUSI = Depends(get_usi)
):
    """
    Check compatibility between two schema versions.
    
    Args:
        schema_name: Name of the schema
        version1: First version
        version2: Second version
        usi: RedisUSI instance
        
    Returns:
        Compatibility information
    """
    is_compatible = usi.is_compatible(schema_name, version1, version2)
    message = (
        f"Schema versions {version1} and {version2} are compatible"
        if is_compatible
        else f"Schema versions {version1} and {version2} are not compatible"
    )
    
    return CompatibilityResponse(is_compatible=is_compatible, message=message)


@app.post("/schemas/{schema_name}/transform")
async def transform_data(
    schema_name: str,
    from_version: str,
    to_version: str,
    data: Dict,
    usi: RedisUSI = Depends(get_usi)
):
    """
    Transform data between schema versions.
    
    Args:
        schema_name: Name of the schema
        from_version: Source version
        to_version: Target version
        data: Data to transform
        usi: RedisUSI instance
        
    Returns:
        Transformed data
    """
    try:
        # Get the chain of deltas
        delta_chain = usi.get_delta_chain(schema_name, from_version, to_version)
        
        # Apply each delta in sequence
        result = data
        for delta in delta_chain:
            result = delta.apply(result)
            
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 
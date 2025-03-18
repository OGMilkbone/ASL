"""
Adaptive Schema Layer (ASL)

A revolutionary approach to schema evolution in distributed systems.
"""

from asl.core.registry import SchemaRegistry
from asl.core.delta import SchemaDelta
from asl.core.transform import SchemaTransformer

__version__ = "0.1.0"
__all__ = ["SchemaRegistry", "SchemaDelta", "SchemaTransformer"] 
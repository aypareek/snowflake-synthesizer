"""
sf-synth: High-fidelity synthetic data generation for Snowflake.

A Snowpark-first library that generates synthetic data inside Snowflake using
auto-discovered schema, distribution statistics, Faker-based rules, and a
DAG-driven referential-integrity engine.
"""

__version__ = "0.3.0"

from sf_synth.config import SynthConfig
from sf_synth.discovery import SchemaModel, discover_schema
from sf_synth.engine import SynthEngine

__all__ = [
    "__version__",
    "SynthConfig",
    "SchemaModel",
    "discover_schema",
    "SynthEngine",
]

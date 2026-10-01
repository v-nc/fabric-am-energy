"""Transformations for the fabric-am-energy portfolio project (simulated data). Pure functions over DataFrames, so the
same code runs in local tests and in Fabric notebooks (as a wheel in a Fabric Environment)."""

from .config import PipelineConfig

__all__ = ["PipelineConfig"]

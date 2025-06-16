"""Query Service per pattern Kappa ibrido."""
from .app import app
from .query_engine import QueryEngine

__all__ = ['app', 'QueryEngine']
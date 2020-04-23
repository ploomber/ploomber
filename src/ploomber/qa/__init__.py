"""
Tools for quality assurance

This module contains utilities for performing checks in the DAGs source code
to look for bad practices. These is complementary to tools such as flake8.
Given that the DAG object has a lot of information, it can provide more
details than static analyzers can
"""
from ploomber.qa.DAGQualityChecker import DAGQualityChecker

__all__ = ['DAGQualityChecker']

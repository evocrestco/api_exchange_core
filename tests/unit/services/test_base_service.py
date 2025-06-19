"""
Tests for base_service module covering utility methods.

This module tests the BaseService utility methods that need coverage.
"""

from typing import Optional

import pytest
from pydantic import BaseModel

from api_exchange_core.services.base_service import BaseService


class SampleReadSchema(BaseModel):
    """Sample read schema for testing."""
    id: str
    name: str
    value: int
    active: bool = True


class SampleCreateSchema(BaseModel):
    """Sample create schema for testing."""
    name: str
    value: int


class SampleUpdateSchema(BaseModel):
    """Sample update schema for testing."""
    name: Optional[str] = None
    value: Optional[int] = None


class SampleFilterSchema(BaseModel):
    """Sample filter schema for testing."""
    name: Optional[str] = None
    active: Optional[bool] = None


class ConcreteService(BaseService[SampleCreateSchema, SampleReadSchema, SampleUpdateSchema, SampleFilterSchema]):
    """Concrete implementation for testing."""
    pass


class MockRepository:
    """Mock repository for testing."""
    pass


class TestBaseService:
    """Test BaseService utility methods."""
    
    @pytest.fixture
    def service(self):
        """Create a service instance for testing."""
        repository = MockRepository()
        return ConcreteService(repository, SampleReadSchema)
    
    def test_entity_to_schema(self, service):
        """Test _entity_to_schema converts dict to schema."""
        entity_dict = {
            "id": "123",
            "name": "Test Entity",
            "value": 42,
            "active": True
        }
        
        result = service._entity_to_schema(entity_dict)
        
        assert isinstance(result, SampleReadSchema)
        assert result.id == "123"
        assert result.name == "Test Entity"
        assert result.value == 42
        assert result.active is True
    
    def test_entities_to_schemas(self, service):
        """Test _entities_to_schemas converts list of dicts to schemas."""
        entity_dicts = [
            {"id": "1", "name": "Entity 1", "value": 10, "active": True},
            {"id": "2", "name": "Entity 2", "value": 20, "active": False},
            {"id": "3", "name": "Entity 3", "value": 30}  # Uses default active=True
        ]
        
        results = service._entities_to_schemas(entity_dicts)
        
        assert len(results) == 3
        assert all(isinstance(result, SampleReadSchema) for result in results)
        
        # Check first entity
        assert results[0].id == "1"
        assert results[0].name == "Entity 1"
        assert results[0].value == 10
        assert results[0].active is True
        
        # Check second entity
        assert results[1].id == "2"
        assert results[1].name == "Entity 2"
        assert results[1].value == 20
        assert results[1].active is False
        
        # Check third entity (default active)
        assert results[2].id == "3"
        assert results[2].name == "Entity 3"
        assert results[2].value == 30
        assert results[2].active is True
    
    def test_paginate_results_first_page(self, service):
        """Test paginate_results for first page."""
        # Create sample results
        results = [
            SampleReadSchema(id=str(i), name=f"Entity {i}", value=i * 10)
            for i in range(1, 6)
        ]
        
        paginated = service.paginate_results(
            results=results,
            total_count=50,
            page=1,
            page_size=5
        )
        
        assert paginated["data"] == results
        assert paginated["pagination"]["page"] == 1
        assert paginated["pagination"]["page_size"] == 5
        assert paginated["pagination"]["total_count"] == 50
        assert paginated["pagination"]["total_pages"] == 10
        assert paginated["pagination"]["has_previous"] is False
        assert paginated["pagination"]["has_next"] is True
    
    def test_paginate_results_middle_page(self, service):
        """Test paginate_results for middle page."""
        results = [
            SampleReadSchema(id=str(i), name=f"Entity {i}", value=i * 10)
            for i in range(11, 16)
        ]
        
        paginated = service.paginate_results(
            results=results,
            total_count=50,
            page=3,
            page_size=5
        )
        
        assert paginated["pagination"]["page"] == 3
        assert paginated["pagination"]["has_previous"] is True
        assert paginated["pagination"]["has_next"] is True
    
    def test_paginate_results_last_page(self, service):
        """Test paginate_results for last page."""
        results = [
            SampleReadSchema(id=str(i), name=f"Entity {i}", value=i * 10)
            for i in range(46, 51)
        ]
        
        paginated = service.paginate_results(
            results=results,
            total_count=50,
            page=10,
            page_size=5
        )
        
        assert paginated["pagination"]["page"] == 10
        assert paginated["pagination"]["total_pages"] == 10
        assert paginated["pagination"]["has_previous"] is True
        assert paginated["pagination"]["has_next"] is False
    
    def test_paginate_results_partial_last_page(self, service):
        """Test paginate_results when last page is partial."""
        results = [
            SampleReadSchema(id=str(i), name=f"Entity {i}", value=i * 10)
            for i in range(1, 4)  # Only 3 results
        ]
        
        paginated = service.paginate_results(
            results=results,
            total_count=23,  # Not evenly divisible by page_size
            page=5,
            page_size=5
        )
        
        # Total pages should be 5 (23 items / 5 per page = 4.6, rounded up to 5)
        assert paginated["pagination"]["total_pages"] == 5
        assert paginated["pagination"]["page"] == 5
        assert paginated["pagination"]["has_next"] is False
    
    def test_paginate_results_empty(self, service):
        """Test paginate_results with no results."""
        paginated = service.paginate_results(
            results=[],
            total_count=0,
            page=1,
            page_size=10
        )
        
        assert paginated["data"] == []
        assert paginated["pagination"]["total_count"] == 0
        assert paginated["pagination"]["total_pages"] == 0
        assert paginated["pagination"]["has_previous"] is False
        assert paginated["pagination"]["has_next"] is False
    
    def test_paginate_results_zero_page_size(self, service):
        """Test paginate_results with zero page size."""
        results = []
        
        paginated = service.paginate_results(
            results=results,
            total_count=50,
            page=1,
            page_size=0
        )
        
        # With page_size=0, total_pages should be 0
        assert paginated["pagination"]["total_pages"] == 0
        assert paginated["pagination"]["page_size"] == 0
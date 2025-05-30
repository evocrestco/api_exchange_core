"""
Tests for MapperInterface and CompositeMapper.

This module tests the mapper interfaces for data transformations,
including the abstract base class and composite mapper functionality.
"""

import pytest

from src.processors.mapper_interface import CompositeMapper, MapperInterface


class ConcreteMapper(MapperInterface):
    """Concrete mapper implementation for testing."""
    
    def to_canonical(self, external_data: dict) -> dict:
        """Transform external data to canonical format."""
        return {
            "canonical_id": external_data.get("id"),
            "canonical_name": external_data.get("name"),
            "source": "concrete_mapper"
        }
    
    def from_canonical(self, canonical_data: dict) -> dict:
        """Transform canonical data to external format."""
        return {
            "id": canonical_data.get("canonical_id"),
            "name": canonical_data.get("canonical_name"),
            "target": "concrete_mapper"
        }


class SalesforceMapper(MapperInterface):
    """Mock Salesforce mapper for composite testing."""
    
    def to_canonical(self, external_data: dict) -> dict:
        """Transform Salesforce data to canonical."""
        return {
            "canonical_id": external_data.get("Id"),
            "canonical_name": external_data.get("Name"),
            "canonical_amount": float(external_data.get("Amount", 0)),
            "stage": "salesforce_imported"
        }
    
    def from_canonical(self, canonical_data: dict) -> dict:
        """Transform canonical to Salesforce format."""
        return {
            "Id": canonical_data.get("canonical_id"),
            "Name": canonical_data.get("canonical_name"), 
            "Amount": str(canonical_data.get("canonical_amount", 0)),
            "stage": "salesforce_exported"
        }


class SAPMapper(MapperInterface):
    """Mock SAP mapper for composite testing."""
    
    def to_canonical(self, external_data: dict) -> dict:
        """Transform SAP data to canonical."""
        # If it's already canonical, just add SAP processing stage
        if "canonical_id" in external_data:
            result = external_data.copy()
            result["stage"] = "sap_processed"
            return result
        
        # Otherwise transform from SAP format
        return {
            "canonical_id": external_data.get("VBELN"),
            "canonical_name": external_data.get("VTEXT"),
            "canonical_amount": float(external_data.get("NETWR", 0)),
            "stage": "sap_imported"
        }
    
    def from_canonical(self, canonical_data: dict) -> dict:
        """Transform canonical to SAP format."""
        return {
            "VBELN": canonical_data.get("canonical_id"),
            "VTEXT": canonical_data.get("canonical_name"),
            "NETWR": str(canonical_data.get("canonical_amount", 0)),
            "stage": "sap_exported"
        }


class EnrichmentMapper(MapperInterface):
    """Mock enrichment mapper for composite testing."""
    
    def to_canonical(self, external_data: dict) -> dict:
        """Enrich canonical data."""
        result = external_data.copy()
        result["enriched"] = True
        result["stage"] = "enriched"
        return result
    
    def from_canonical(self, canonical_data: dict) -> dict:
        """Pass through for enrichment mapper."""
        result = canonical_data.copy()
        result["stage"] = "post_enrichment"
        return result


class TestMapperInterface:
    """Test MapperInterface abstract base class."""
    
    def test_mapper_interface_is_abstract(self):
        """Test that MapperInterface cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            MapperInterface()
    
    def test_to_canonical_is_abstract(self):
        """Test that to_canonical method is abstract."""
        # This is implicitly tested by the fact that ConcreteMapper
        # must implement both methods to be instantiable
        mapper = ConcreteMapper()
        assert hasattr(mapper, "to_canonical")
        assert callable(mapper.to_canonical)
    
    def test_from_canonical_is_abstract(self):
        """Test that from_canonical method is abstract."""
        mapper = ConcreteMapper()
        assert hasattr(mapper, "from_canonical")
        assert callable(mapper.from_canonical)
    
    def test_get_mapper_info_default(self):
        """Test default get_mapper_info returns class information."""
        mapper = ConcreteMapper()
        info = mapper.get_mapper_info()
        
        assert info["mapper_class"] == "ConcreteMapper"
        assert info["mapper_module"] == "test_mapper_interface"
        assert len(info) == 2  # Only class and module by default
    
    def test_validate_external_data_default_accepts_all(self):
        """Test default validate_external_data accepts all data."""
        mapper = ConcreteMapper()
        
        test_data = [
            {"id": "123", "name": "Test"},
            {},
            {"complex": {"nested": {"data": True}}},
            {"list": [1, 2, 3]}
        ]
        
        for data in test_data:
            assert mapper.validate_external_data(data) is True
    
    def test_validate_canonical_data_default_accepts_all(self):
        """Test default validate_canonical_data accepts all data."""
        mapper = ConcreteMapper()
        
        test_data = [
            {"canonical_id": "123", "canonical_name": "Test"},
            {},
            {"canonical_complex": {"nested": {"data": True}}},
            {"canonical_list": [1, 2, 3]}
        ]
        
        for data in test_data:
            assert mapper.validate_canonical_data(data) is True
    
    def test_concrete_mapper_to_canonical_transformation(self):
        """Test concrete mapper to_canonical transformation."""
        mapper = ConcreteMapper()
        
        external_data = {
            "id": "ext-123",
            "name": "External Name",
            "extra_field": "ignored"
        }
        
        result = mapper.to_canonical(external_data)
        
        assert result["canonical_id"] == "ext-123"
        assert result["canonical_name"] == "External Name"
        assert result["source"] == "concrete_mapper"
        assert "extra_field" not in result
    
    def test_concrete_mapper_from_canonical_transformation(self):
        """Test concrete mapper from_canonical transformation."""
        mapper = ConcreteMapper()
        
        canonical_data = {
            "canonical_id": "can-123",
            "canonical_name": "Canonical Name",
            "extra_field": "ignored"
        }
        
        result = mapper.from_canonical(canonical_data)
        
        assert result["id"] == "can-123"
        assert result["name"] == "Canonical Name"
        assert result["target"] == "concrete_mapper"
        assert "extra_field" not in result
    
    def test_mapper_handles_missing_fields_gracefully(self):
        """Test that mapper handles missing fields gracefully."""
        mapper = ConcreteMapper()
        
        # Test with empty data
        empty_result = mapper.to_canonical({})
        assert empty_result["canonical_id"] is None
        assert empty_result["canonical_name"] is None
        assert empty_result["source"] == "concrete_mapper"
        
        from_empty_result = mapper.from_canonical({})
        assert from_empty_result["id"] is None
        assert from_empty_result["name"] is None
        assert from_empty_result["target"] == "concrete_mapper"


class TestCompositeMapper:
    """Test CompositeMapper chaining functionality."""
    
    def test_composite_mapper_requires_mappers(self):
        """Test that CompositeMapper requires at least one mapper."""
        with pytest.raises(ValueError, match="CompositeMapper requires at least one mapper"):
            CompositeMapper([])
    
    def test_composite_mapper_single_mapper(self):
        """Test CompositeMapper with single mapper."""
        single_mapper = ConcreteMapper()
        composite = CompositeMapper([single_mapper])
        
        external_data = {"id": "123", "name": "Test"}
        canonical_data = {"canonical_id": "123", "canonical_name": "Test"}
        
        # Should behave identically to single mapper
        to_result = composite.to_canonical(external_data)
        assert to_result == single_mapper.to_canonical(external_data)
        
        from_result = composite.from_canonical(canonical_data)
        assert from_result == single_mapper.from_canonical(canonical_data)
    
    def test_composite_mapper_chains_to_canonical(self):
        """Test that CompositeMapper chains to_canonical methods correctly."""
        salesforce_mapper = SalesforceMapper()
        enrichment_mapper = EnrichmentMapper()
        
        composite = CompositeMapper([salesforce_mapper, enrichment_mapper])
        
        salesforce_data = {
            "Id": "SF-123",
            "Name": "Salesforce Order",
            "Amount": "100.50"
        }
        
        # Should apply salesforce mapper first, then enrichment
        result = composite.to_canonical(salesforce_data)
        
        # Check that both transformations were applied
        assert result["canonical_id"] == "SF-123"
        assert result["canonical_name"] == "Salesforce Order"
        assert result["canonical_amount"] == 100.50
        assert result["enriched"] is True  # From enrichment mapper
        assert result["stage"] == "enriched"  # Last stage wins
    
    def test_composite_mapper_chains_from_canonical_in_reverse(self):
        """Test that CompositeMapper chains from_canonical in reverse order."""
        salesforce_mapper = SalesforceMapper()
        enrichment_mapper = EnrichmentMapper()
        
        composite = CompositeMapper([salesforce_mapper, enrichment_mapper])
        
        canonical_data = {
            "canonical_id": "CAN-123",
            "canonical_name": "Canonical Order",
            "canonical_amount": 200.75
        }
        
        # Should apply enrichment mapper first (reverse), then salesforce
        result = composite.from_canonical(canonical_data)
        
        # Check that transformations were applied in reverse order
        assert result["Id"] == "CAN-123"  # From salesforce mapper
        assert result["Name"] == "Canonical Order"  # From salesforce mapper
        assert result["Amount"] == "200.75"  # From salesforce mapper
        assert result["stage"] == "salesforce_exported"  # Final stage
    
    def test_composite_mapper_two_stage_pipeline(self):
        """Test CompositeMapper with two compatible mappers."""
        # Create a pipeline that makes sense: Salesforce -> Enrichment
        salesforce_mapper = SalesforceMapper()
        enrichment_mapper = EnrichmentMapper()
        
        composite = CompositeMapper([salesforce_mapper, enrichment_mapper])
        
        # Test to_canonical: SF -> Enrichment
        salesforce_data = {"Id": "SF-456", "Name": "Test Order", "Amount": "75.25"}
        
        to_result = composite.to_canonical(salesforce_data)
        
        # Should have Salesforce transformation + enrichment
        assert to_result["canonical_id"] == "SF-456"
        assert to_result["canonical_name"] == "Test Order"
        assert to_result["canonical_amount"] == 75.25
        assert to_result["enriched"] is True
        assert to_result["stage"] == "enriched"  # Final stage from enrichment
        
        # Test from_canonical: Enrichment -> Salesforce (reverse order)
        canonical_data = {
            "canonical_id": "CAN-456", 
            "canonical_name": "Canonical Order",
            "canonical_amount": 150.0
        }
        
        from_result = composite.from_canonical(canonical_data)
        
        # Should be in Salesforce format after enrichment processing
        assert from_result["Id"] == "CAN-456"
        assert from_result["Name"] == "Canonical Order"
        assert from_result["Amount"] == "150.0"
        assert from_result["stage"] == "salesforce_exported"
    
    def test_composite_mapper_get_mapper_info(self):
        """Test CompositeMapper get_mapper_info includes component info."""
        mapper1 = SalesforceMapper()
        mapper2 = EnrichmentMapper()
        
        composite = CompositeMapper([mapper1, mapper2])
        info = composite.get_mapper_info()
        
        assert info["mapper_class"] == "CompositeMapper"
        assert info["mapper_module"] == "src.processors.mapper_interface"
        assert "component_mappers" in info
        assert len(info["component_mappers"]) == 2
        
        # Check component mapper info
        component_info = info["component_mappers"]
        assert component_info[0]["mapper_class"] == "SalesforceMapper"
        assert component_info[1]["mapper_class"] == "EnrichmentMapper"
    
    def test_composite_mapper_preserves_data_types(self):
        """Test that CompositeMapper preserves data types through transformations."""
        mapper1 = SalesforceMapper()
        mapper2 = EnrichmentMapper()
        
        composite = CompositeMapper([mapper1, mapper2])
        
        # Test with various data types
        salesforce_data = {
            "Id": "SF-789",
            "Name": "Data Type Test",
            "Amount": "999.99"  # String that becomes float
        }
        
        result = composite.to_canonical(salesforce_data)
        
        # Check that data types are preserved/converted correctly
        assert isinstance(result["canonical_id"], str)
        assert isinstance(result["canonical_name"], str)
        assert isinstance(result["canonical_amount"], float)
        assert result["canonical_amount"] == 999.99
        assert isinstance(result["enriched"], bool)
        assert result["enriched"] is True
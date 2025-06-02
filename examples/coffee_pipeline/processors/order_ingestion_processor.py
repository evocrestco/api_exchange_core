"""
Order Ingestion Processor - HTTP triggered Azure Function for receiving coffee orders.

This processor handles incoming pretentious coffee orders via HTTP POST,
transforms them to canonical format, and routes them to the complexity analysis queue.

Uses the framework's ProcessorHandler for automatic entity persistence, state tracking, and error recording.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from src.processors.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface

from mappers.pretentious_mapper import PretentiousOrderMapper


class OrderIngestionProcessor(ProcessorInterface):
    """
    Processes incoming pretentious coffee orders and routes them for complexity analysis.
    
    This processor demonstrates the import pattern: it takes external data (pretentious orders),
    transforms it to canonical format using a mapper, and routes it to the next stage.
    """
    
    def __init__(self, **kwargs):
        """Initialize the processor with the pretentious order mapper."""
        self.mapper = PretentiousOrderMapper()
        self.logger = kwargs.get("logger", logging.getLogger("order_ingestion"))
    
    def process(self, message: Message) -> ProcessingResult:
        """
        Process an incoming coffee order message.
        
        Args:
            message: Message containing the pretentious coffee order
            
        Returns:
            ProcessingResult with routing to complexity analysis
        """
        try:
            self.logger.info(f"Processing order ingestion for: {message.message_id}")
            
            # Extract order data from message
            order_data = message.payload
            
            # Transform using the pretentious order mapper (to_canonical)
            canonical_data = self.to_canonical(order_data, {
                "source": "artisanal_api",
                "timestamp": datetime.now().isoformat()
            })
            
            # Create output message for complexity analysis
            output_message = Message(
                message_id=f"complexity-{message.message_id}",
                message_type=MessageType.ENTITY_PROCESSING,
                entity_reference=message.entity_reference,
                payload=canonical_data,
                metadata={
                    "source_processor": "order_ingestion",
                    "processing_stage": "complexity_analysis",
                    "original_order": order_data.get("order", ""),
                    "timestamp": datetime.now().isoformat()
                }
            )
            
            self.logger.info(
                f"Order ingested successfully. "
                f"Pretentiousness score: {canonical_data.get('pretentiousness_score', 0):.1f}, "
                f"Original words: {canonical_data.get('word_count_original', 0)}, "
                f"Routing to complexity analysis"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                success=True,
                output_messages=[output_message],
                processing_metadata={
                    "processor": "order_ingestion",
                    "transformation_applied": "pretentious_to_canonical",
                    "pretentiousness_score": canonical_data.get("pretentiousness_score", 0),
                    "next_stage": "complexity_analysis"
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error processing order ingestion: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                success=False,
                error_message=f"Order ingestion failed: {str(e)}",
                processing_metadata={"processor": "order_ingestion", "error": str(e)}
            )
    
    def to_canonical(self, external_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform pretentious coffee order to canonical format.
        
        Args:
            external_data: Raw order data from HTTP request
            metadata: Processing metadata
            
        Returns:
            Canonical coffee order data
        """
        return self.mapper.to_canonical(external_data)
    
    def from_canonical(self, canonical_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in ingestion processor - only transforms input."""
        raise NotImplementedError("Order ingestion processor only handles input transformation")
    
    def validate_message(self, message: Message) -> bool:
        """Validate that the message contains required order data."""
        return bool(message.payload and isinstance(message.payload, dict) and message.payload.get("order"))
    
    def get_processor_info(self) -> dict:
        """Return processor metadata."""
        return {
            "name": "OrderIngestionProcessor",
            "version": "1.0.0",
            "type": "source",
            "capabilities": ["transform", "route"]
        }
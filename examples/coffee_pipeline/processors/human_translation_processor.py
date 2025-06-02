"""
Human Translation Processor - Final stage that converts canonical orders to human language.

This processor receives complexity-analyzed canonical coffee orders and translates them
to simple, human-readable language that normal people (and baristas) can understand.

Uses the framework's ProcessorHandler for automatic state tracking and pipeline completion.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from src.processors.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface

from mappers.human_translation_mapper import HumanTranslationMapper
from models.coffee_order import CanonicalCoffeeOrder


class HumanTranslationProcessor(ProcessorInterface):
    """
    Final processor that translates canonical coffee orders to human language.
    
    This processor demonstrates the export pattern: it takes canonical data
    and transforms it using from_canonical() to produce human-readable output.
    Since this is a terminal processor, it logs results instead of routing further.
    """
    
    def __init__(self, **kwargs):
        """Initialize the human translation processor."""
        self.mapper = HumanTranslationMapper()
        self.logger = kwargs.get("logger", logging.getLogger("human_translation"))
    
    def process(self, message: Message) -> ProcessingResult:
        """
        Process a canonical coffee order for human translation.
        
        Args:
            message: Message containing complexity-analyzed canonical coffee order
            
        Returns:
            ProcessingResult with human-readable output (terminal - no further routing)
        """
        try:
            self.logger.info(f"Processing human translation for: {message.message_id}")
            
            # Get the canonical order data
            canonical_data = message.payload
            
            # Transform using the human translation mapper (from_canonical)
            human_readable_data = self.from_canonical(canonical_data, {
                "target": "human_language",
                "timestamp": datetime.now().isoformat()
            })
            
            # Log the complete translation results
            self._log_translation_results(message.entity_reference.external_id, human_readable_data)
            
            # This is a terminal processor, so no output messages
            self.logger.info(
                f"Human translation completed for order {message.entity_reference.external_id}. "
                f"Check logs above for full details."
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                success=True,
                output_messages=[],  # Terminal processor - no further routing
                processing_metadata={
                    "processor": "human_translation",
                    "translation_completed": True,
                    "simple_order": human_readable_data.get("simple_order", ""),
                    "complexity_reduced": True,
                    "pipeline_complete": True
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in human translation: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                success=False,
                error_message=f"Human translation failed: {str(e)}",
                processing_metadata={"processor": "human_translation", "error": str(e)}
            )
    
    def to_canonical(self, external_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in terminal processor - only transforms output."""
        raise NotImplementedError("Human translation processor only handles output transformation")
    
    def from_canonical(self, canonical_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform canonical coffee order to human-readable format.
        
        Args:
            canonical_data: Canonical coffee order data with complexity analysis
            metadata: Processing metadata
            
        Returns:
            Human-readable coffee order information
        """
        return self.mapper.from_canonical(canonical_data)
    
    def _log_translation_results(self, order_id: str, translation_data: Dict[str, Any]) -> None:
        """
        Log the complete translation results in a beautiful, readable format.
        
        This creates the final output that demonstrates the complete pipeline transformation
        from pretentious coffee order to actionable human language.
        """
        self.logger.info("=" * 80)
        self.logger.info("🎉 COFFEE ORDER TRANSLATION COMPLETE 🎉")
        self.logger.info("=" * 80)
        
        # Order identification
        self.logger.info(f"📋 Order ID: {order_id}")
        self.logger.info(f"⏰ Processed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("")
        
        # Simple order (the main result)
        simple_order = translation_data.get("simple_order", "")
        self.logger.info("☕ SIMPLE ORDER (for normal humans):")
        self.logger.info(f"   {simple_order}")
        self.logger.info("")
        
        # Original vs simplified comparison
        original_order = translation_data.get("original_pretentious_order", "")
        if original_order:
            self.logger.info("📜 ORIGINAL PRETENTIOUS ORDER:")
            self.logger.info(f"   {original_order}")
            self.logger.info("")
        
        # Translation summary
        summary = translation_data.get("translation_summary", {})
        if summary:
            self.logger.info("📊 TRANSLATION SUMMARY:")
            for key, value in summary.items():
                formatted_key = key.replace("_", " ").title()
                self.logger.info(f"   {formatted_key}: {value}")
            self.logger.info("")
        
        # Barista notes
        barista_notes = translation_data.get("barista_notes", {})
        if barista_notes:
            self.logger.info("👨‍💼 BARISTA OPERATIONAL NOTES:")
            self.logger.info(f"   Complexity Level: {barista_notes.get('complexity_level', 'unknown')}")
            self.logger.info(f"   Estimated Prep Time: {barista_notes.get('estimated_prep_time', 'unknown')}")
            self.logger.info(f"   Eye Roll Factor: {barista_notes.get('eye_roll_factor', 'unknown')}")
            
            # Customer management tips
            customer_mgmt = barista_notes.get("customer_management", [])
            if customer_mgmt:
                self.logger.info("   Customer Management Tips:")
                for tip in customer_mgmt:
                    self.logger.info(f"     • {tip}")
            
            # Preparation tips
            prep_tips = barista_notes.get("preparation_tips", [])
            if prep_tips:
                self.logger.info("   Preparation Tips:")
                for tip in prep_tips:
                    self.logger.info(f"     • {tip}")
            
            self.logger.info("")
        
        # Customer messages
        customer_messages = translation_data.get("customer_message", {})
        if customer_messages:
            self.logger.info("💬 CUSTOMER COMMUNICATION:")
            for msg_type, message in customer_messages.items():
                formatted_type = msg_type.replace("_", " ").title()
                self.logger.info(f"   {formatted_type}: {message}")
            self.logger.info("")
        
        # Operational information
        operational_info = translation_data.get("operational_info", {})
        if operational_info:
            self.logger.info("📈 OPERATIONAL METRICS:")
            
            # Complexity analysis
            complexity_analysis = operational_info.get("complexity_analysis", {})
            if complexity_analysis:
                self.logger.info("   Complexity Analysis:")
                for key, value in complexity_analysis.items():
                    formatted_key = key.replace("_", " ").title()
                    self.logger.info(f"     {formatted_key}: {value}")
            
            # Quality metrics
            quality_metrics = operational_info.get("quality_metrics", {})
            if quality_metrics:
                self.logger.info("   Quality Metrics:")
                for key, value in quality_metrics.items():
                    formatted_key = key.replace("_", " ").title()
                    if isinstance(value, float):
                        self.logger.info(f"     {formatted_key}: {value:.3f}")
                    else:
                        self.logger.info(f"     {formatted_key}: {value}")
            
            # Staff impact
            staff_impact = operational_info.get("staff_impact", {})
            if staff_impact:
                self.logger.info("   Staff Impact:")
                for key, value in staff_impact.items():
                    formatted_key = key.replace("_", " ").title()
                    self.logger.info(f"     {formatted_key}: {value}")
            
            self.logger.info("")
        
        # Fun conclusion based on complexity
        self._log_humorous_conclusion(translation_data)
        
        self.logger.info("=" * 80)
    
    def _log_humorous_conclusion(self, translation_data: Dict[str, Any]) -> None:
        """Add some humor to the final output based on the order characteristics."""
        barista_notes = translation_data.get("barista_notes", {})
        eye_roll_factor = barista_notes.get("eye_roll_factor", "0/10")
        
        try:
            eye_roll_score = float(eye_roll_factor.split("/")[0])
        except (ValueError, IndexError):
            eye_roll_score = 0.0
        
        self.logger.info("🎭 FINAL VERDICT:")
        
        if eye_roll_score >= 9:
            self.logger.info("   This order broke our pretentiousness detector. 🤯")
            self.logger.info("   Barista may need therapy after this one.")
        elif eye_roll_score >= 7:
            self.logger.info("   Peak coffee pretentiousness achieved! 🏆")
            self.logger.info("   Customer has PhD in coffee linguistics.")
        elif eye_roll_score >= 5:
            self.logger.info("   Moderately pretentious - probably from Brooklyn. 🎨")
            self.logger.info("   Barista eye rolls are within normal limits.")
        elif eye_roll_score >= 2:
            self.logger.info("   Slight pretentiousness detected - still manageable. ☕")
            self.logger.info("   Customer knows what they want, mostly.")
        else:
            self.logger.info("   Normal human coffee order! 🙌")
            self.logger.info("   Barista appreciates your straightforwardness.")
        
        # Check if customer will complain
        operational_info = translation_data.get("operational_info", {})
        customer_will_complain = False
        
        # Try to find complaint prediction in the data
        canonical_data = translation_data
        if isinstance(canonical_data, dict):
            customer_will_complain = canonical_data.get("customer_will_complain_about_wait", False)
        
        if customer_will_complain:
            self.logger.info("")
            self.logger.info("⚠️  WARNING: Customer complaint probability is HIGH")
            self.logger.info("   Prepare standard apologies and possibly a free pastry.")
        
        self.logger.info("")
        self.logger.info("Thank you for using the Pretentious Coffee Translation Pipeline! ☕✨")
    
    def validate_message(self, message: Message) -> bool:
        """Validate that the message contains canonical coffee order data."""
        return bool(message.payload and isinstance(message.payload, dict))
    
    def get_processor_info(self) -> dict:
        """Return processor metadata."""
        return {
            "name": "HumanTranslationProcessor",
            "version": "1.0.0",
            "type": "terminal",
            "capabilities": ["translate", "log", "complete"]
        }
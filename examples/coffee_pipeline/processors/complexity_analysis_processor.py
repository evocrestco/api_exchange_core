"""
Complexity Analysis Processor - Queue triggered Azure Function for analyzing coffee order complexity.

This processor receives canonical coffee orders and performs complexity analysis,
calculating preparation time, barista stress factors, and operational metrics.

Uses the framework's ProcessorHandler for automatic state tracking and error recording.
"""

import logging
import math
from datetime import datetime
from typing import Any, Dict

from src.processors.message import Message, MessageType
from src.processors.processing_result import ProcessingResult, ProcessingStatus
from src.processors.processor_interface import ProcessorInterface

from models.coffee_order import CanonicalCoffeeOrder


class ComplexityAnalysisProcessor(ProcessorInterface):
    """
    Analyzes coffee order complexity and calculates operational metrics.
    
    This processor demonstrates the processing pattern: it takes canonical data,
    enhances it with business logic (complexity analysis), and routes it to the next stage.
    """
    
    def __init__(self, **kwargs):
        """Initialize the complexity analysis processor."""
        self.logger = kwargs.get("logger", logging.getLogger("complexity_analysis"))
    
    def process(self, message: Message) -> ProcessingResult:
        """
        Process a canonical coffee order for complexity analysis.
        
        Args:
            message: Message containing canonical coffee order data
            
        Returns:
            ProcessingResult with enhanced complexity metrics
        """
        try:
            self.logger.info(f"Processing complexity analysis for: {message.message_id}")
            
            # Get the canonical order data
            canonical_data = message.payload
            
            # Create canonical order object for analysis
            coffee_order = CanonicalCoffeeOrder(**canonical_data)
            
            # Perform complexity analysis
            enhanced_order = self._analyze_complexity(coffee_order)
            
            # Create output message for human translation
            output_message = Message(
                message_id=f"translation-{message.message_id}",
                message_type=MessageType.ENTITY_PROCESSING,
                entity_reference=message.entity_reference,
                payload=enhanced_order.model_dump(),
                metadata={
                    "source_processor": "complexity_analysis",
                    "processing_stage": "human_translation",
                    "complexity_level": enhanced_order.complexity_level,
                    "prep_time_minutes": enhanced_order.estimated_prep_time_minutes,
                    "barista_stress": enhanced_order.barista_eye_roll_factor,
                    "timestamp": datetime.now().isoformat()
                }
            )
            
            self.logger.info(
                f"Complexity analysis completed. "
                f"Complexity: {enhanced_order.complexity_level}, "
                f"Prep time: {enhanced_order.estimated_prep_time_minutes:.1f}m, "
                f"Barista stress: {enhanced_order.barista_eye_roll_factor:.1f}/10, "
                f"Routing to human translation"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                success=True,
                output_messages=[output_message],
                processing_metadata={
                    "processor": "complexity_analysis",
                    "complexity_level": enhanced_order.complexity_level,
                    "total_complexity_score": enhanced_order.calculate_total_complexity(),
                    "prep_time_category": enhanced_order.get_prep_time_category(),
                    "pretentiousness_category": enhanced_order.get_pretentiousness_category(),
                    "next_stage": "human_translation"
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in complexity analysis: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                success=False,
                error_message=f"Complexity analysis failed: {str(e)}",
                processing_metadata={"processor": "complexity_analysis", "error": str(e)}
            )
    
    def _analyze_complexity(self, order: CanonicalCoffeeOrder) -> CanonicalCoffeeOrder:
        """
        Perform detailed complexity analysis on a coffee order.
        
        This method calculates various complexity metrics and updates the order
        with enhanced operational information.
        
        Args:
            order: Canonical coffee order to analyze
            
        Returns:
            Enhanced coffee order with complexity analysis
        """
        # Calculate preparation time based on multiple factors
        prep_time = self._calculate_preparation_time(order)
        
        # Determine complexity level
        complexity_level = self._determine_complexity_level(order, prep_time)
        
        # Calculate barista eye roll factor
        eye_roll_factor = self._calculate_barista_stress(order)
        
        # Determine if customer will complain about wait
        will_complain = self._predict_customer_complaints(order, prep_time)
        
        # Update the order with enhanced metrics
        order.estimated_prep_time_minutes = prep_time
        order.complexity_level = complexity_level
        order.barista_eye_roll_factor = eye_roll_factor
        order.customer_will_complain_about_wait = will_complain
        
        # Log analysis results
        self.logger.info(
            f"Complexity analysis results: "
            f"prep_time={prep_time:.1f}m, "
            f"complexity={complexity_level}, "
            f"eye_roll_factor={eye_roll_factor:.1f}, "
            f"will_complain={will_complain}"
        )
        
        return order
    
    def _calculate_preparation_time(self, order: CanonicalCoffeeOrder) -> float:
        """
        Calculate estimated preparation time based on order complexity.
        
        This uses a sophisticated algorithm that accounts for drink type,
        customizations, pretentiousness level, and barista sanity.
        """
        # Base preparation times by drink type (in minutes)
        base_times = {
            "coffee": 1.0,
            "americano": 1.5,
            "espresso": 1.0,
            "latte": 3.0,
            "cappuccino": 3.5,
            "macchiato": 4.0,
            "flat_white": 4.5,
            "mocha": 5.0,
            "pour_over": 6.0,
            "cold_brew": 2.0,  # Already brewed
            "frappuccino": 4.0
        }
        
        base_time = base_times.get(order.drink_type.value, 3.0)
        
        # Size multiplier
        size_multipliers = {
            "small": 0.8,
            "medium": 1.0,
            "large": 1.3,
            "extra_large": 1.6
        }
        
        prep_time = base_time * size_multipliers.get(order.size.value, 1.0)
        
        # Milk complexity
        if order.milk_type and order.milk_type.value != "none":
            if order.milk_type.value in ["oat", "almond", "soy", "coconut", "cashew"]:
                prep_time += 0.5  # Alternative milks need extra care
            
            # Temperature specificity
            if order.temperature_f:
                prep_time += 1.0  # Specific temperature requires thermometer
        
        # Shot complexity
        if order.shots > 2:
            prep_time += (order.shots - 2) * 0.5
        
        # Foam preferences
        if order.extra_foam or order.no_foam:
            prep_time += 0.5  # Requires foam adjustment
        
        # Flavor shots
        prep_time += len(order.flavor_shots) * 0.3
        
        # Pretentiousness tax - more pretentious orders take longer
        # because baristas need time to process the ridiculousness
        pretentiousness_multiplier = 1 + (order.pretentiousness_score / 20)
        prep_time *= pretentiousness_multiplier
        
        # Adjective processing time - each adjective adds cognitive load
        prep_time += order.adjective_count * 0.1
        
        # Word count impact - longer orders confuse baristas
        if order.word_count_original > 10:
            confusion_factor = math.log(order.word_count_original / 10) * 0.5
            prep_time += confusion_factor
        
        # Ensure reasonable bounds
        return max(0.5, min(prep_time, 45.0))  # 30 second minimum, 45 minute maximum
    
    def _determine_complexity_level(self, order: CanonicalCoffeeOrder, prep_time: float) -> str:
        """
        Determine the complexity level category for operational planning.
        """
        # Calculate total complexity score
        complexity_score = order.calculate_total_complexity()
        
        # Combine prep time and complexity score
        if prep_time <= 3 and complexity_score <= 3:
            return "simple"
        elif prep_time <= 8 and complexity_score <= 6:
            return "moderate"
        elif prep_time <= 15 and complexity_score <= 9:
            return "complex"
        else:
            return "ridiculous"
    
    def _calculate_barista_stress(self, order: CanonicalCoffeeOrder) -> float:
        """
        Calculate the barista eye roll factor based on order pretentiousness.
        
        This is a critical metric for staff mental health monitoring.
        """
        stress_factor = 0.0
        
        # Base stress from pretentiousness
        stress_factor += order.pretentiousness_score
        
        # Adjective overload stress
        if order.adjective_count > 5:
            stress_factor += (order.adjective_count - 5) * 0.3
        
        # Word count stress
        if order.word_count_original > 15:
            stress_factor += (order.word_count_original - 15) * 0.1
        
        # Temperature specificity stress
        if order.temperature_f:
            if order.temperature_f > 170 or order.temperature_f < 100:
                stress_factor += 2.0  # Extreme temperatures are stressful
            else:
                stress_factor += 1.0  # Any specific temperature is mildly annoying
        
        # Foam preference stress
        if order.extra_foam and order.drink_type.value == "cappuccino":
            stress_factor += 1.5  # Extra foam on cappuccino is redundant
        elif order.no_foam and order.drink_type.value == "cappuccino":
            stress_factor += 2.0  # No foam cappuccino is just a latte
        
        # Multiple flavor stress
        if len(order.flavor_shots) > 2:
            stress_factor += len(order.flavor_shots) * 0.5
        
        # Size vs complexity mismatch stress
        complexity_score = order.calculate_total_complexity()
        if order.size.value == "small" and complexity_score > 5:
            stress_factor += 1.0  # Tiny drink with huge complexity
        
        # Cap at 10.0 to prevent barista mental breakdown
        return min(stress_factor, 10.0)
    
    def _predict_customer_complaints(self, order: CanonicalCoffeeOrder, prep_time: float) -> bool:
        """
        Predict whether the customer will complain about wait time.
        
        Uses advanced behavioral analysis based on order characteristics.
        """
        # High pretentiousness usually correlates with impatience
        if order.pretentiousness_score > 7:
            return True
        
        # Customers who use many adjectives expect instant gratification
        if order.adjective_count > 10:
            return True
        
        # Long preparation times generally lead to complaints
        if prep_time > 12:
            return True
        
        # Temperature specifications indicate control issues
        if order.temperature_f and prep_time > 8:
            return True
        
        # Multiple customizations with medium+ prep time
        customization_count = (
            (1 if order.extra_foam or order.no_foam else 0) +
            len(order.flavor_shots) +
            (1 if order.temperature_f else 0) +
            (1 if order.milk_type and order.milk_type.value != "whole" else 0)
        )
        
        if customization_count >= 3 and prep_time > 6:
            return True
        
        return False
    
    def to_canonical(self, external_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in intermediate processor - works with canonical data."""
        raise NotImplementedError("Complexity analysis processor works with canonical data")
    
    def from_canonical(self, canonical_data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Not used in intermediate processor - only transforms and routes."""
        raise NotImplementedError("Complexity analysis processor only transforms and routes")
    
    def validate_message(self, message: Message) -> bool:
        """Validate that the message contains canonical coffee order data."""
        try:
            if message.payload:
                # Try to create a CanonicalCoffeeOrder to validate
                CanonicalCoffeeOrder(**message.payload)
                return True
        except:
            pass
        return False
    
    def get_processor_info(self) -> dict:
        """Return processor metadata."""
        return {
            "name": "ComplexityAnalysisProcessor",
            "version": "1.0.0",
            "type": "intermediate",
            "capabilities": ["analyze", "enhance", "route"]
        }
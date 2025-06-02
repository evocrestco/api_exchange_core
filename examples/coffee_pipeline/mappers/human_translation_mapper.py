"""
Human Translation Mapper - Converts canonical coffee orders to normal human language.

This mapper takes our detailed canonical coffee order model and converts it
back to simple, understandable language that actual humans can process.
"""

from typing import Any, Dict

from src.processors.mapper_interface import MapperInterface

from models.coffee_order import CanonicalCoffeeOrder, CaffeineLevel, DrinkType, MilkType, Size


class HumanTranslationMapper(MapperInterface):
    """
    Converts canonical coffee orders to plain English.
    
    This mapper is the hero baristas need - it takes our detailed canonical
    model and converts it to simple, actionable language that doesn't require
    a PhD in coffee studies to understand.
    """
    
    def to_canonical(self, external_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        This mapper only handles output transformation.
        Use PretentiousOrderMapper for input transformation.
        """
        raise NotImplementedError(
            "HumanTranslationMapper only handles output transformation. "
            "Use PretentiousOrderMapper for converting external data to canonical format."
        )
    
    def from_canonical(self, canonical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform canonical coffee order to human-readable format.
        
        Args:
            canonical_data: Canonical coffee order data
            
        Returns:
            Human-readable coffee order information
        """
        # Create canonical order object for easier access
        order = CanonicalCoffeeOrder(**canonical_data)
        
        # Build the simple order description
        simple_order = self._build_simple_order_description(order)
        
        # Create barista instructions
        barista_notes = self._generate_barista_notes(order)
        
        # Create customer communication
        customer_message = self._generate_customer_message(order)
        
        # Operational information
        operational_info = self._generate_operational_info(order)
        
        return {
            "simple_order": simple_order,
            "barista_notes": barista_notes,
            "customer_message": customer_message,
            "operational_info": operational_info,
            "original_pretentious_order": order.original_order_text,
            "translation_summary": {
                "pretentiousness_reduction": f"{order.pretentiousness_score:.1f} → 0.0 (successfully de-pretentified)",
                "word_count_reduction": f"{order.word_count_original} → {len(simple_order.split())} words",
                "time_saved_explaining": f"{order.estimated_prep_time_minutes:.1f} minutes",
                "barista_sanity_preserved": order.barista_eye_roll_factor < 5.0
            }
        }
    
    def _build_simple_order_description(self, order: CanonicalCoffeeOrder) -> str:
        """Build a simple, clear order description."""
        parts = []
        
        # Size and drink type
        size_text = self._get_human_size(order.size)
        drink_text = self._get_human_drink_name(order.drink_type)
        parts.append(f"{size_text} {drink_text}")
        
        # Milk specification
        if order.milk_type and order.milk_type != MilkType.NONE:
            milk_text = self._get_human_milk_name(order.milk_type)
            if order.drink_type in [DrinkType.LATTE, DrinkType.CAPPUCCINO, DrinkType.FLAT_WHITE]:
                parts.append(f"with {milk_text}")
            else:
                parts.append(f"+ {milk_text}")
        
        # Caffeine modifications
        caffeine_text = self._get_caffeine_description(order.caffeine_level, order.shots)
        if caffeine_text:
            parts.append(f"({caffeine_text})")
        
        # Temperature
        if order.temperature_f:
            if order.temperature_f > 160:
                parts.append("(extra hot)")
            elif order.temperature_f < 120:
                parts.append("(warm)")
            else:
                parts.append(f"({order.temperature_f}°F)")
        elif order.extra_hot:
            parts.append("(extra hot)")
        
        # Foam preferences
        if order.extra_foam:
            parts.append("(extra foam)")
        elif order.no_foam:
            parts.append("(no foam)")
        
        # Flavors (simplified)
        if order.flavor_shots:
            if len(order.flavor_shots) == 1:
                parts.append(f"+ {order.flavor_shots[0]}")
            else:
                parts.append(f"+ {len(order.flavor_shots)} flavors")
        
        return " ".join(parts)
    
    def _get_human_size(self, size: Size) -> str:
        """Convert size enum to human-readable size."""
        size_mapping = {
            Size.SMALL: "small",
            Size.MEDIUM: "medium", 
            Size.LARGE: "large",
            Size.EXTRA_LARGE: "extra large"
        }
        return size_mapping.get(size, "medium")
    
    def _get_human_drink_name(self, drink_type: DrinkType) -> str:
        """Convert drink type to simple human name."""
        drink_mapping = {
            DrinkType.COFFEE: "coffee",
            DrinkType.LATTE: "latte",
            DrinkType.CAPPUCCINO: "cappuccino",
            DrinkType.AMERICANO: "americano",
            DrinkType.ESPRESSO: "espresso",
            DrinkType.MOCHA: "mocha",
            DrinkType.MACCHIATO: "macchiato",
            DrinkType.FLAT_WHITE: "flat white",
            DrinkType.FRAPPUCCINO: "frappuccino",
            DrinkType.COLD_BREW: "cold brew",
            DrinkType.POUR_OVER: "pour over coffee"
        }
        return drink_mapping.get(drink_type, "coffee")
    
    def _get_human_milk_name(self, milk_type: MilkType) -> str:
        """Convert milk type to human-readable name."""
        milk_mapping = {
            MilkType.WHOLE: "whole milk",
            MilkType.SKIM: "skim milk",
            MilkType.TWO_PERCENT: "2% milk",
            MilkType.OAT: "oat milk",
            MilkType.ALMOND: "almond milk",
            MilkType.SOY: "soy milk",
            MilkType.COCONUT: "coconut milk",
            MilkType.CASHEW: "cashew milk",
            MilkType.NONE: ""
        }
        return milk_mapping.get(milk_type, "milk")
    
    def _get_caffeine_description(self, caffeine_level: CaffeineLevel, shots: int) -> str:
        """Generate human-readable caffeine description."""
        if caffeine_level == CaffeineLevel.DECAF:
            return "decaf"
        elif caffeine_level == CaffeineLevel.HALF_CAF:
            return "half caff"
        elif caffeine_level == CaffeineLevel.DEATH_WISH:
            return f"{shots} shots - DANGER LEVEL CAFFEINE"
        elif shots > 2:
            return f"{shots} shots"
        elif shots == 2:
            return "double shot"
        else:
            return ""
    
    def _generate_barista_notes(self, order: CanonicalCoffeeOrder) -> Dict[str, Any]:
        """Generate helpful notes for the barista."""
        notes = {
            "complexity_level": order.complexity_level,
            "estimated_prep_time": f"{order.estimated_prep_time_minutes:.1f} minutes",
            "eye_roll_factor": f"{order.barista_eye_roll_factor:.1f}/10",
            "customer_management": []
        }
        
        # Customer management tips
        if order.customer_will_complain_about_wait:
            notes["customer_management"].append("⚠️  Customer likely to complain about wait time")
        
        if order.pretentiousness_score > 7:
            notes["customer_management"].append("🎭 High pretentiousness - expect follow-up questions")
        
        if order.adjective_count > 10:
            notes["customer_management"].append("📝 Customer used excessive adjectives - brace yourself")
        
        if order.temperature_f and order.temperature_f > 170:
            notes["customer_management"].append("🌡️  Dangerously hot temperature requested")
        
        if len(order.flavor_shots) > 3:
            notes["customer_management"].append("🍯 Many flavor requests - double-check order")
        
        # Preparation tips
        prep_tips = []
        
        if order.get_prep_time_category() == "are_you_serious":
            prep_tips.append("☕ Consider suggesting a simpler alternative")
        
        if order.extra_foam and order.drink_type == DrinkType.CAPPUCCINO:
            prep_tips.append("🥛 Extra foam on cappuccino = mostly air")
        
        if order.no_foam and order.drink_type == DrinkType.CAPPUCCINO:
            prep_tips.append("🤔 No foam cappuccino = basically a latte")
        
        notes["preparation_tips"] = prep_tips
        
        return notes
    
    def _generate_customer_message(self, order: CanonicalCoffeeOrder) -> Dict[str, str]:
        """Generate customer-facing messages."""
        messages = {}
        
        # Wait time communication
        prep_time = order.estimated_prep_time_minutes
        if prep_time <= 3:
            messages["wait_time"] = "Your order will be ready in a few minutes!"
        elif prep_time <= 8:
            messages["wait_time"] = f"Your order will be ready in about {prep_time:.0f} minutes."
        else:
            messages["wait_time"] = f"Your complex order will take approximately {prep_time:.0f} minutes. Thank you for your patience!"
        
        # Complexity explanation
        if order.pretentiousness_score > 6:
            messages["complexity_note"] = "We've simplified your order while preserving all the important details."
        
        # Temperature warning
        if order.temperature_f and order.temperature_f > 170:
            messages["safety_warning"] = "⚠️ Your requested temperature is very hot. Please be careful!"
        
        return messages
    
    def _generate_operational_info(self, order: CanonicalCoffeeOrder) -> Dict[str, Any]:
        """Generate operational information for management."""
        return {
            "complexity_analysis": {
                "total_complexity_score": order.calculate_total_complexity(),
                "pretentiousness_category": order.get_pretentiousness_category(),
                "prep_time_category": order.get_prep_time_category(),
                "revenue_impact": self._estimate_revenue_impact(order)
            },
            "quality_metrics": {
                "original_word_count": order.word_count_original,
                "adjective_density": order.adjective_count / max(order.word_count_original, 1),
                "simplification_ratio": len(order.to_simple_description().split()) / max(order.word_count_original, 1)
            },
            "staff_impact": {
                "barista_stress_level": min(order.barista_eye_roll_factor, 10),
                "training_opportunity": order.pretentiousness_score > 8,
                "efficiency_impact": f"{order.estimated_prep_time_minutes:.1f}x normal prep time"
            }
        }
    
    def _estimate_revenue_impact(self, order: CanonicalCoffeeOrder) -> str:
        """Estimate revenue impact of this order complexity."""
        if order.pretentiousness_score > 8:
            return "High value customer (probably)"
        elif order.pretentiousness_score > 5:
            return "Premium pricing justified"
        elif order.estimated_prep_time_minutes > 10:
            return "Consider efficiency surcharge"
        else:
            return "Standard pricing"
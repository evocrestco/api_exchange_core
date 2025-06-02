"""
Canonical coffee order model for the Pretentious Coffee Translation Pipeline.

This demonstrates a canonical model that bridges pretentious coffee shop orders
with normal human language, including complexity analysis for barista sanity.
"""

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DrinkType(str, Enum):
    """Standard drink types that humans understand."""
    COFFEE = "coffee"
    LATTE = "latte"
    CAPPUCCINO = "cappuccino"
    AMERICANO = "americano"
    ESPRESSO = "espresso"
    MOCHA = "mocha"
    MACCHIATO = "macchiato"
    FLAT_WHITE = "flat_white"
    FRAPPUCCINO = "frappuccino"
    COLD_BREW = "cold_brew"
    POUR_OVER = "pour_over"


class MilkType(str, Enum):
    """Milk alternatives that exist in reality."""
    WHOLE = "whole"
    SKIM = "skim"
    TWO_PERCENT = "2%"
    OAT = "oat"
    ALMOND = "almond"
    SOY = "soy"
    COCONUT = "coconut"
    CASHEW = "cashew"
    NONE = "none"


class Size(str, Enum):
    """Coffee sizes that make sense."""
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    EXTRA_LARGE = "extra_large"


class CaffeineLevel(str, Enum):
    """Caffeine levels for the addicted masses."""
    REGULAR = "regular"
    DECAF = "decaf"
    HALF_CAF = "half_caf"
    EXTRA_SHOT = "extra_shot"
    DEATH_WISH = "death_wish"  # 4+ shots


class CanonicalCoffeeOrder(BaseModel):
    """
    Canonical coffee order model that bridges pretentious and normal.
    
    This model represents the essential components of any coffee order,
    stripped of pretentious language but retaining the important details.
    """
    
    # Core order details
    drink_type: DrinkType
    size: Size = Size.MEDIUM
    milk_type: Optional[MilkType] = None
    caffeine_level: CaffeineLevel = CaffeineLevel.REGULAR
    
    # Customizations that actually matter
    shots: int = Field(default=1, ge=0, le=10)  # Max 10 for safety
    temperature_f: Optional[int] = Field(default=None, ge=90, le=180)  # Reasonable range
    extra_hot: bool = False
    extra_foam: bool = False
    no_foam: bool = False
    
    # Sweeteners and flavors (simplified)
    sweetener_packets: int = Field(default=0, ge=0, le=20)  # Diabetes limit
    flavor_shots: List[str] = Field(default_factory=list)
    
    # Business logic fields
    pretentiousness_score: float = Field(default=0.0, ge=0.0, le=10.0)
    complexity_level: str = Field(default="simple")  # simple, moderate, ridiculous
    estimated_prep_time_minutes: float = Field(default=2.0, ge=0.5, le=60.0)
    
    # Barista metrics (for operational sanity)
    word_count_original: int = Field(default=0, ge=0)
    adjective_count: int = Field(default=0, ge=0)
    barista_eye_roll_factor: float = Field(default=0.0, ge=0.0, le=10.0)
    customer_will_complain_about_wait: bool = False
    
    # Metadata
    original_order_text: Optional[str] = None
    source_system: str = "artisanal_api"
    processed_timestamp: Optional[str] = None
    
    def get_prep_time_category(self) -> str:
        """Categorize preparation time for barista planning."""
        if self.estimated_prep_time_minutes <= 3:
            return "quick"
        elif self.estimated_prep_time_minutes <= 8:
            return "normal"
        elif self.estimated_prep_time_minutes <= 15:
            return "complex"
        else:
            return "are_you_serious"
    
    def get_pretentiousness_category(self) -> str:
        """Categorize the level of pretentiousness for social credit scoring."""
        if self.pretentiousness_score <= 2:
            return "normal_human"
        elif self.pretentiousness_score <= 5:
            return "slightly_extra"
        elif self.pretentiousness_score <= 8:
            return "peak_brooklyn"
        else:
            return "insufferable"
    
    def calculate_total_complexity(self) -> float:
        """Calculate overall complexity score for operational planning."""
        complexity = 0.0
        
        # Base complexity by drink type
        drink_complexity = {
            DrinkType.COFFEE: 1.0,
            DrinkType.AMERICANO: 1.5,
            DrinkType.LATTE: 2.0,
            DrinkType.CAPPUCCINO: 2.5,
            DrinkType.MACCHIATO: 3.0,
            DrinkType.FLAT_WHITE: 3.5,
            DrinkType.MOCHA: 4.0,
            DrinkType.POUR_OVER: 5.0,
            DrinkType.FRAPPUCCINO: 6.0,
        }
        
        complexity += drink_complexity.get(self.drink_type, 2.0)
        
        # Milk complexity
        if self.milk_type and self.milk_type != MilkType.WHOLE:
            complexity += 0.5
        
        # Shot complexity
        if self.shots > 2:
            complexity += (self.shots - 2) * 0.5
        
        # Temperature specificity
        if self.temperature_f:
            complexity += 1.0
        
        # Foam preferences
        if self.extra_foam or self.no_foam:
            complexity += 0.5
        
        # Flavors
        complexity += len(self.flavor_shots) * 0.3
        
        # Pretentiousness multiplier
        complexity *= (1 + self.pretentiousness_score / 10)
        
        return round(complexity, 2)
    
    def to_simple_description(self) -> str:
        """Convert to a simple, human-readable description."""
        parts = []
        
        # Size and drink
        parts.append(f"{self.size.value} {self.drink_type.value}")
        
        # Milk
        if self.milk_type and self.milk_type != MilkType.NONE:
            parts.append(f"with {self.milk_type.value} milk")
        
        # Caffeine
        if self.caffeine_level == CaffeineLevel.DECAF:
            parts.append("(decaf)")
        elif self.caffeine_level == CaffeineLevel.EXTRA_SHOT:
            parts.append("(extra shot)")
        elif self.caffeine_level == CaffeineLevel.DEATH_WISH:
            parts.append("(danger level caffeine)")
        
        # Special requests
        if self.extra_hot:
            parts.append("extra hot")
        if self.temperature_f:
            parts.append(f"at {self.temperature_f}°F")
        
        return " ".join(parts)
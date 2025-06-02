"""
Pretentious Order Mapper - Transforms flowery coffee language to canonical format.

This mapper handles the complex task of parsing pretentious coffee orders
and extracting the actual drink requirements while scoring the level of pretentiousness.
"""

import re
from datetime import datetime
from typing import Any, Dict

from src.processors.mapper_interface import MapperInterface

from models.coffee_order import (
    CaffeineLevel,
    CanonicalCoffeeOrder,
    DrinkType,
    MilkType,
    Size,
)


class PretentiousOrderMapper(MapperInterface):
    """
    Maps pretentious coffee orders to canonical coffee order format.
    
    This mapper is the hero we need but don't deserve - it decodes the
    cryptic language of artisanal coffee culture into actionable drink specifications.
    """
    
    # Mapping dictionaries for decoding pretentious language
    PRETENTIOUS_DRINK_MAPPING = {
        # Standard mappings
        "espresso": DrinkType.ESPRESSO,
        "americano": DrinkType.AMERICANO,
        "latte": DrinkType.LATTE,
        "cappuccino": DrinkType.CAPPUCCINO,
        "macchiato": DrinkType.MACCHIATO,
        "mocha": DrinkType.MOCHA,
        "flat white": DrinkType.FLAT_WHITE,
        "pour over": DrinkType.POUR_OVER,
        "cold brew": DrinkType.COLD_BREW,
        
        # Pretentious alternatives
        "cortado": DrinkType.FLAT_WHITE,  # Close enough
        "gibraltar": DrinkType.FLAT_WHITE,  # San Francisco nonsense
        "cafe au lait": DrinkType.LATTE,
        "cafe latte": DrinkType.LATTE,
        "caffe latte": DrinkType.LATTE,
        "lungo": DrinkType.AMERICANO,
        "ristretto": DrinkType.ESPRESSO,
        "doppio": DrinkType.ESPRESSO,
        "red eye": DrinkType.COFFEE,  # Coffee with espresso shot
        "black eye": DrinkType.COFFEE,  # Coffee with 2 espresso shots
        "dead eye": DrinkType.COFFEE,  # Coffee with 3+ espresso shots
        "bulletproof": DrinkType.COFFEE,  # Coffee with butter (why?)
        "nitro": DrinkType.COLD_BREW,
        "cascara": DrinkType.COFFEE,  # Coffee cherry tea, basically
    }
    
    MILK_MAPPING = {
        "oat": MilkType.OAT,
        "almond": MilkType.ALMOND,
        "soy": MilkType.SOY,
        "coconut": MilkType.COCONUT,
        "cashew": MilkType.CASHEW,
        "whole": MilkType.WHOLE,
        "skim": MilkType.SKIM,
        "2%": MilkType.TWO_PERCENT,
        "two percent": MilkType.TWO_PERCENT,
        "nonfat": MilkType.SKIM,
        "non-fat": MilkType.SKIM,
        "full fat": MilkType.WHOLE,
        "dairy": MilkType.WHOLE,
        "plant-based": MilkType.OAT,  # Default plant milk
        "alternative": MilkType.OAT,  # Default alternative
    }
    
    SIZE_MAPPING = {
        "small": Size.SMALL,
        "medium": Size.MEDIUM,
        "large": Size.LARGE,
        "tall": Size.SMALL,  # Starbucks confusion
        "grande": Size.MEDIUM,  # More Starbucks confusion
        "venti": Size.LARGE,  # Peak Starbucks confusion
        "trenta": Size.EXTRA_LARGE,  # Why does this exist?
        "short": Size.SMALL,
        "regular": Size.MEDIUM,
        "big": Size.LARGE,
        "massive": Size.EXTRA_LARGE,
        "artisanal": Size.SMALL,  # Always overpriced and tiny
        "curated": Size.SMALL,  # Same energy
        "bespoke": Size.SMALL,  # Maximum pretension, minimum volume
    }
    
    # Words that increase pretentiousness score
    PRETENTIOUS_KEYWORDS = {
        # Origin and processing (1 point each)
        "single-origin": 1, "micro-lot": 1, "estate": 1, "terroir": 1,
        "heirloom": 1, "bourbon": 1, "typica": 1, "geisha": 1, "pacamara": 1,
        "yirgacheffe": 1, "sidamo": 1, "harrar": 1, "kona": 1, "blue mountain": 2,
        
        # Processing methods (1 point each)
        "natural": 1, "washed": 1, "honey": 1, "anaerobic": 2, "carbonic": 2,
        "fermented": 1, "pulped": 1, "semi-washed": 1,
        
        # Artisanal descriptors (2 points each)
        "artisanal": 2, "handcrafted": 2, "small-batch": 2, "craft": 2,
        "artisan": 2, "curated": 2, "bespoke": 3, "heritage": 2,
        
        # Ethical buzzwords (1 point each)
        "fair-trade": 1, "organic": 1, "sustainable": 1, "ethical": 1,
        "rainforest": 1, "bird-friendly": 1, "shade-grown": 1,
        
        # Tasting notes (1 point per 2 words)
        "notes": 1, "hints": 1, "undertones": 1, "finish": 1, "body": 1,
        "acidity": 1, "brightness": 1, "complexity": 2,
        
        # Temperature specificity (2 points each)
        "precisely": 2, "exactly": 2, "degrees": 1, "fahrenheit": 1, "celsius": 1,
        
        # Container preferences (1 point each)
        "ceramic": 1, "porcelain": 1, "hand-thrown": 2, "artisanal": 2,
        "handmade": 2, "vintage": 1, "reclaimed": 2,
        
        # Maximum pretension (3+ points)
        "ceremonial": 3, "meditation": 3, "mindful": 3, "intentional": 3,
        "journey": 3, "experience": 2, "narrative": 3, "story": 2,
    }
    
    def to_canonical(self, external_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a pretentious coffee order to canonical format.
        
        Args:
            external_data: Dictionary containing the pretentious order text
            
        Returns:
            Canonical coffee order data
        """
        order_text = external_data.get("order", "").lower()
        original_order = external_data.get("order", "")
        
        if not order_text:
            # Default order for empty input
            return CanonicalCoffeeOrder(
                drink_type=DrinkType.COFFEE,
                original_order_text=original_order,
                processed_timestamp=datetime.now().isoformat()
            ).model_dump()
        
        # Parse the order components
        drink_type = self._extract_drink_type(order_text)
        size = self._extract_size(order_text)
        milk_type = self._extract_milk_type(order_text)
        caffeine_level, shots = self._extract_caffeine_info(order_text)
        temperature = self._extract_temperature(order_text)
        foam_prefs = self._extract_foam_preferences(order_text)
        flavors = self._extract_flavors(order_text)
        
        # Calculate pretentiousness metrics
        pretentiousness_score = self._calculate_pretentiousness_score(original_order)
        word_count = len(original_order.split())
        adjective_count = self._count_adjectives(original_order)
        
        # Create canonical order
        canonical_order = CanonicalCoffeeOrder(
            drink_type=drink_type,
            size=size,
            milk_type=milk_type,
            caffeine_level=caffeine_level,
            shots=shots,
            temperature_f=temperature,
            extra_foam=foam_prefs.get("extra_foam", False),
            no_foam=foam_prefs.get("no_foam", False),
            flavor_shots=flavors,
            pretentiousness_score=pretentiousness_score,
            word_count_original=word_count,
            adjective_count=adjective_count,
            barista_eye_roll_factor=min(pretentiousness_score, 10.0),
            customer_will_complain_about_wait=pretentiousness_score > 6.0,
            original_order_text=original_order,
            processed_timestamp=datetime.now().isoformat()
        )
        
        return canonical_order.model_dump()
    
    def from_canonical(self, canonical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        This mapper only handles input transformation.
        Use HumanTranslationMapper for output transformation.
        """
        raise NotImplementedError(
            "PretentiousOrderMapper only handles input transformation. "
            "Use HumanTranslationMapper for converting canonical to human-readable format."
        )
    
    def _extract_drink_type(self, order_text: str) -> DrinkType:
        """Extract the actual drink type from pretentious descriptions."""
        # Check for explicit drink mentions
        for drink_name, drink_type in self.PRETENTIOUS_DRINK_MAPPING.items():
            if drink_name in order_text:
                return drink_type
        
        # Fallback logic based on indicators
        if any(word in order_text for word in ["shot", "espresso", "doppio"]):
            return DrinkType.ESPRESSO
        elif any(word in order_text for word in ["foam", "steamed", "micro"]):
            return DrinkType.LATTE  # Most likely with foam mentions
        elif "cold" in order_text:
            return DrinkType.COLD_BREW
        elif any(word in order_text for word in ["pour", "filter", "drip"]):
            return DrinkType.POUR_OVER
        else:
            return DrinkType.COFFEE  # Default fallback
    
    def _extract_size(self, order_text: str) -> Size:
        """Extract size from the order text."""
        for size_name, size_enum in self.SIZE_MAPPING.items():
            if size_name in order_text:
                return size_enum
        
        # If no size specified, assume medium
        return Size.MEDIUM
    
    def _extract_milk_type(self, order_text: str) -> MilkType:
        """Extract milk type from the order text."""
        for milk_name, milk_enum in self.MILK_MAPPING.items():
            if milk_name in order_text:
                return milk_enum
        
        # Check for milk alternatives
        if any(word in order_text for word in ["alternative", "non-dairy", "plant"]):
            return MilkType.OAT  # Default alternative
        
        # If it's espresso-based and no milk specified, assume whole
        drink_type = self._extract_drink_type(order_text)
        if drink_type in [DrinkType.LATTE, DrinkType.CAPPUCCINO, DrinkType.FLAT_WHITE]:
            return MilkType.WHOLE
        
        return MilkType.NONE
    
    def _extract_caffeine_info(self, order_text: str) -> tuple[CaffeineLevel, int]:
        """Extract caffeine level and shot count."""
        shots = 1  # Default
        caffeine_level = CaffeineLevel.REGULAR
        
        # Count explicit shot mentions
        shot_matches = re.findall(r'(\d+)[\-\s]*shot', order_text)
        if shot_matches:
            shots = int(shot_matches[0])
        elif "double" in order_text or "doppio" in order_text:
            shots = 2
        elif "triple" in order_text:
            shots = 3
        elif "quad" in order_text:
            shots = 4
        
        # Determine caffeine level
        if "decaf" in order_text or "caffeine-free" in order_text:
            caffeine_level = CaffeineLevel.DECAF
        elif "half-caf" in order_text or "half caff" in order_text:
            caffeine_level = CaffeineLevel.HALF_CAF
        elif shots >= 4:
            caffeine_level = CaffeineLevel.DEATH_WISH
        elif shots > 2:
            caffeine_level = CaffeineLevel.EXTRA_SHOT
        
        return caffeine_level, shots
    
    def _extract_temperature(self, order_text: str) -> int:
        """Extract specific temperature requirements."""
        # Look for explicit temperature mentions
        temp_matches = re.findall(r'(\d{2,3})[\s]*[°]?[f|fahrenheit]', order_text)
        if temp_matches:
            temp = int(temp_matches[0])
            # Sanity check
            if 90 <= temp <= 180:
                return temp
        
        # Look for temperature descriptors
        if "extra hot" in order_text or "very hot" in order_text:
            return 170
        elif "warm" in order_text or "lukewarm" in order_text:
            return 120
        
        return None  # No specific temperature
    
    def _extract_foam_preferences(self, order_text: str) -> Dict[str, bool]:
        """Extract foam preferences."""
        foam_prefs = {"extra_foam": False, "no_foam": False}
        
        if any(phrase in order_text for phrase in ["extra foam", "lots of foam", "foamy"]):
            foam_prefs["extra_foam"] = True
        elif any(phrase in order_text for phrase in ["no foam", "without foam", "flat"]):
            foam_prefs["no_foam"] = True
        
        return foam_prefs
    
    def _extract_flavors(self, order_text: str) -> list[str]:
        """Extract flavor additions."""
        flavors = []
        
        # Common flavor indicators
        flavor_keywords = [
            "vanilla", "caramel", "hazelnut", "cinnamon", "chocolate",
            "pumpkin", "peppermint", "almond", "coconut", "lavender",
            "cardamom", "nutmeg", "maple", "honey"
        ]
        
        for flavor in flavor_keywords:
            if flavor in order_text:
                flavors.append(flavor)
        
        return flavors
    
    def _calculate_pretentiousness_score(self, order_text: str) -> float:
        """Calculate pretentiousness score based on language used."""
        score = 0.0
        text_lower = order_text.lower()
        
        # Score based on pretentious keywords
        for keyword, points in self.PRETENTIOUS_KEYWORDS.items():
            if keyword in text_lower:
                score += points
        
        # Bonus points for excessive adjectives
        adjective_count = self._count_adjectives(order_text)
        if adjective_count > 5:
            score += (adjective_count - 5) * 0.5
        
        # Bonus for excessive length
        word_count = len(order_text.split())
        if word_count > 20:
            score += (word_count - 20) * 0.1
        
        # Bonus for mentioning specific temperature
        if re.search(r'\d{2,3}[\s]*[°]?[f|fahrenheit]', text_lower):
            score += 2.0
        
        # Bonus for origin story mentions
        if any(word in text_lower for word in ["farmer", "mountain", "altitude", "elevation"]):
            score += 1.0
        
        return min(score, 10.0)  # Cap at 10
    
    def _count_adjectives(self, order_text: str) -> int:
        """Count probable adjectives in the order."""
        # Simple heuristic: words ending in common adjective suffixes
        adjective_patterns = [
            r'\w+ed\b',   # past participle adjectives
            r'\w+ing\b',  # present participle adjectives  
            r'\w+ful\b',  # -ful adjectives
            r'\w+less\b', # -less adjectives
            r'\w+ous\b',  # -ous adjectives
            r'\w+al\b',   # -al adjectives
            r'\w+ic\b',   # -ic adjectives
        ]
        
        count = 0
        for pattern in adjective_patterns:
            count += len(re.findall(pattern, order_text.lower()))
        
        # Also count some obvious descriptive words
        descriptive_words = [
            "organic", "artisanal", "handcrafted", "premium", "specialty",
            "single-origin", "fair-trade", "sustainable", "ethical", "natural",
            "smooth", "rich", "bold", "mild", "complex", "bright", "floral"
        ]
        
        for word in descriptive_words:
            if word in order_text.lower():
                count += 1
        
        return count
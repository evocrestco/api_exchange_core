#!/usr/bin/env python3
"""
Script to fix all test imports from old absolute paths to new package paths.
"""

import os
import re
from pathlib import Path

def fix_imports_in_file(file_path):
    """Fix imports in a single file."""
    print(f"Processing {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    original_content = content
    
    # Replace all the absolute imports with package imports
    replacements = [
        (r'^from context\.', 'from api_exchange_core.context.'),
        (r'^from db\.', 'from api_exchange_core.db.'),
        (r'^from db ', 'from api_exchange_core.db '),
        (r'^from schemas\.', 'from api_exchange_core.schemas.'),
        (r'^from schemas ', 'from api_exchange_core.schemas '),
        (r'^from utils\.', 'from api_exchange_core.utils.'),
        (r'^from repositories\.', 'from api_exchange_core.repositories.'),
        (r'^from services\.', 'from api_exchange_core.services.'),
        (r'^from processing\.', 'from api_exchange_core.processing.'),
        (r'^from processing ', 'from api_exchange_core.processing '),
        (r'^from processors\.', 'from api_exchange_core.processors.'),
        # Additional patterns that were missed
        (r'^from exceptions ', 'from api_exchange_core.exceptions '),
        (r'^from config ', 'from api_exchange_core.config '),
        (r'^from constants ', 'from api_exchange_core.constants '),
        (r'^from type_definitions ', 'from api_exchange_core.type_definitions '),
        (r'^from processors ', 'from api_exchange_core.processors '),
        # Fix mock patches
        (r'"src\.', '"api_exchange_core.'),
        (r"'src\.", "'api_exchange_core."),  # Fix patch statements with single quotes
        # Fix any fixture imports that might be inside functions
        (r'        from context\.', '        from api_exchange_core.context.'),
        (r'        from db\.', '        from api_exchange_core.db.'),
        (r'        from schemas\.', '        from api_exchange_core.schemas.'),
        (r'        from schemas ', '        from api_exchange_core.schemas '),
        (r'        from utils\.', '        from api_exchange_core.utils.'),
        (r'        from repositories\.', '        from api_exchange_core.repositories.'),
        (r'        from services\.', '        from api_exchange_core.services.'),
        (r'        from processing\.', '        from api_exchange_core.processing.'),
        (r'        from processing ', '        from api_exchange_core.processing '),
        (r'        from processors\.', '        from api_exchange_core.processors.'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"  ✅ Updated {file_path}")
        return True
    else:
        print(f"  ⏭️  No changes needed in {file_path}")
        return False

def main():
    """Fix all test files."""
    test_dir = Path("tests")
    
    if not test_dir.exists():
        print("❌ tests directory not found")
        return
    
    print("Fixing test imports...")
    print("=" * 50)
    
    updated_count = 0
    total_count = 0
    
    # Process all Python files in tests directory
    for py_file in test_dir.rglob("*.py"):
        total_count += 1
        if fix_imports_in_file(py_file):
            updated_count += 1
    
    print("=" * 50)
    print(f"Results: Updated {updated_count}/{total_count} test files")

if __name__ == "__main__":
    main()
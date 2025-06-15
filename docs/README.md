# API Exchange Core Framework Documentation

This directory contains the Sphinx documentation for the API Exchange Core Framework.

## Building the Documentation

### Prerequisites

Install the required packages:

```bash
pip install sphinx sphinx-rtd-theme sphinx-autodoc-typehints
```

### Building HTML Documentation

From the `docs` directory:

```bash
# Using make
make html

# Or using sphinx-build directly
sphinx-build -b html source build/html
```

The generated HTML documentation will be in `build/html/index.html`.

### Cleaning Build Files

```bash
make clean
```

## Documentation Structure

- `source/index.rst` - Main documentation index
- `source/overview.rst` - Framework overview and principles
- `source/architecture.rst` - Detailed architecture documentation
- `source/getting-started.rst` - Quick start guide
- `source/configuration.rst` - Configuration reference
- `source/api/` - Auto-generated API documentation

## Auto-Generated API Documentation

The API documentation is automatically generated from docstrings in the source code using Sphinx autodoc. The following modules are documented:

- **Processors** - Core processor interface and implementations
- **Entities** - Entity models and schemas
- **Services** - High-level business logic services
- **Repositories** - Data access layer
- **Database** - Database models and configuration
- **Context** - Tenant and operation context management
- **Utilities** - Helper modules and utilities

## Viewing the Documentation

After building, open `build/html/index.html` in your web browser to view the documentation.

For development, you can use Python's built-in server:

```bash
cd build/html
python -m http.server 8000
```

Then visit http://localhost:8000

## Documentation Guidelines

When adding new code:

1. **Add comprehensive docstrings** to all public classes and methods
2. **Use Google or NumPy style docstrings** for consistency
3. **Include type hints** for automatic documentation generation
4. **Update relevant .rst files** if adding new major components
5. **Rebuild documentation** to check for warnings and errors

## Theme and Configuration

The documentation uses the Read the Docs theme with custom configuration in `source/conf.py`. Key features:

- Automatic API documentation from docstrings
- Type hint support
- Cross-references to Python, Pydantic, and SQLAlchemy documentation
- Source code viewing
- Search functionality
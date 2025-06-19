# Contributing to API Exchange Core

We love your input! We want to make contributing to API Exchange Core as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## We Develop with GitHub

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

## We Use [GitHub Flow](https://guides.github.com/introduction/flow/index.html)

Pull requests are the best way to propose changes to the codebase. We actively welcome your pull requests:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints and type checks pass.
6. Sign your commits (see below).
7. Issue that pull request!

## Pull Request Guidelines

### PR Checklist

Before submitting your PR, ensure:

- [ ] All tests pass (`pytest tests/`)
- [ ] Code is formatted (`black src/ tests/ --check`)
- [ ] Imports are sorted (`isort src/ tests/ --check`)
- [ ] Linting passes (`flake8 src/ tests/`)
- [ ] Type checking passes (`mypy src/`)
- [ ] Coverage hasn't decreased significantly
- [ ] Documentation is updated if needed
- [ ] Commits are signed (DCO requirement)

### PR Title Format

Use clear, descriptive titles:
- `feat: Add support for batch processing`
- `fix: Handle null values in entity attributes`
- `docs: Update installation instructions`
- `test: Add tests for error handling`
- `refactor: Simplify repository base class`

### PR Description Template

```markdown
## Description
Brief description of what this PR does.

## Motivation
Why is this change needed?

## Changes
- List of specific changes
- Another change

## Testing
How has this been tested?

## Breaking Changes
Any breaking changes? How to migrate?
```

## Developer Certificate of Origin (DCO)

This project uses the Developer Certificate of Origin (DCO) to ensure that contributors have the right to submit their contributions. All commits must be signed off.

### How to Sign Your Commits

You must sign off your commits using the `-s` or `--signoff` flag:

```bash
git commit -s -m "Your commit message"
```

This adds a `Signed-off-by:` line to your commit message:

```
Your commit message

Signed-off-by: Your Name <your.email@example.com>
```

### Setting Up Git to Automatically Sign Commits

To avoid forgetting to sign your commits, you can configure Git to do it automatically:

```bash
# Set up your name and email (if not already done)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"

# Create a commit template
echo "Signed-off-by: $(git config user.name) <$(git config user.email)>" > ~/.gitmessage

# Configure Git to use the template
git config --global commit.template ~/.gitmessage
```

### What Does Signing Off Mean?

By signing off on a commit, you certify that you have the right to submit it under the project's license and that you agree to the [Developer Certificate of Origin](DCO).

## Any contributions you make will be under the Apache 2.0 Software License

In short, when you submit code changes, your submissions are understood to be under the same [Apache 2.0 License](LICENSE) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using GitHub's [issues](https://github.com/evocrest/api_exchange_core/issues)

We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/evocrest/api_exchange_core/issues/new); it's that easy!

## Write bug reports with detail, background, and sample code

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

## Development Setup

Before contributing, please set up your development environment:

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/api_exchange_core.git
cd api_exchange_core

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

## Use a Consistent Coding Style

We enforce consistent code style across the project:

* **Python 3.8+** - We use modern Python features
* **Black** - Code formatting (line length: 100)
* **isort** - Import sorting
* **flake8** - Linting
* **mypy** - Type checking
* **Type hints** - ALL code must be fully typed

### Code Style Guidelines

1. **Imports**: Always use absolute imports from `src`
   ```python
   # Good
   from services.entity_service import EntityService
   
   # Bad
   from ..services.entity_service import EntityService
   ```

2. **Type Hints**: Every function must have type hints
   ```python
   # Good
   def process_entity(entity_id: str, validate: bool = True) -> EntityRead:
       ...
   
   # Bad
   def process_entity(entity_id, validate=True):
       ...
   ```
   
   **Type Error Policy**:
   - Fix all type errors at the source when possible
   - SQLAlchemy dynamic attributes: `# type: ignore[attr-defined]` is acceptable
   - Never suppress type errors just to make mypy pass - fix the root cause
   - Document why if you must use `# type: ignore` for non-SQLAlchemy issues

3. **Error Handling**: Use the centralized exception system
   ```python
   from exceptions import ValidationError, ErrorCode
   
   raise ValidationError(
       "Invalid entity data",
       error_code=ErrorCode.VALIDATION_FAILED,
       details={"field": "email"}
   )
   ```

### Running Code Quality Tools

```bash
# Format code
black src/ tests/ --line-length 100

# Sort imports
isort src/ tests/

# Check code quality
flake8 src/ tests/

# Type checking
mypy src/

# Run all checks at once
pre-commit run --all-files
```

## Testing

We follow a **NO MOCKS** testing philosophy. This means:

- Write tests using real implementations, not mocks
- Use the provided test fixtures and factories
- Test behavior, not implementation details
- Ensure all tests pass before submitting PR
- Follow the testing patterns described in [tests/README_TESTING.md](tests/README_TESTING.md)
- Maintain our >85% code coverage standard

### Writing Tests

```python
# Good - Using real objects
def test_entity_creation(entity_service, tenant_context):
    with tenant_context("test-tenant"):
        entity_id = entity_service.create_entity(
            external_id="TEST-123",
            canonical_type="customer",
            source="test"
        )
        entity = entity_service.get_entity(entity_id)
        assert entity.external_id == "TEST-123"

# Bad - Using mocks
def test_with_mock():
    mock_repo = MagicMock()
    mock_repo.get_by_id.return_value = {"id": "123"}  # Don't do this!
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/services/test_entity_service.py

# Run with coverage
pytest --cov=src --cov-report=html --cov-report=term

# Run with verbose output
pytest -vvs

# Run only failed tests
pytest --lf
```

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

## References

This document was adapted from the open-source contribution guidelines for [Facebook's Draft](https://github.com/facebook/draft-js/blob/master/CONTRIBUTING.md)
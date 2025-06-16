# TODO: Technical Debt and Improvements

## High Priority

### SQLAlchemy Session Management Architectural Issue

**Problem**: 
We have a fundamental architectural mismatch between our Repository pattern and SQLAlchemy's Unit of Work pattern that's causing persistent session state errors.

**Symptoms**:
- "Session in prepared state" errors when multiple operations occur in a transaction
- Need to carefully manage when `session.flush()` is called
- Repository methods that need IDs immediately (e.g., `store_new_token`) require flush
- Multiple repositories sharing a session can cause cascade flush issues

**Root Cause**:
1. SQLAlchemy's Unit of Work pattern expects:
   - Batch all changes in a session
   - Single flush at the end
   - Session owns the transaction lifecycle

2. Our Repository pattern assumes:
   - Each repository method is somewhat independent
   - Methods can be called in any order
   - Each method handles its own persistence

3. These patterns conflict when:
   - Multiple repositories use the same session
   - Methods need IDs before transaction commits
   - Error handling triggers additional DB operations

**Current Workarounds**:
- Only calling `flush()` when absolutely necessary (e.g., when we need the ID)
- Removed redundant flush calls from most repository methods
- BaseRepository handles flushing in `_session_operation` context manager

**Proper Solutions to Consider**:
1. **Session-per-operation**: Give each repository method its own session (more overhead)
2. **Deferred ID pattern**: Return objects instead of IDs, get ID later when needed
3. **Command pattern**: Batch operations and execute as a unit
4. **Proper session-per-request**: One session per Azure Function invocation
5. **Event sourcing**: Avoid needing IDs immediately by using events

**Files Affected**:
- `/src/repositories/base_repository.py` - Core session management
- `/src/repositories/api_token_repository.py` - Needs flush for ID
- `/src/repositories/entity_repository.py` - Complex operations with state tracking
- `/src/repositories/state_transition_repository.py` - Multiple queries in one operation
- `/src/db/db_api_token_models.py` - Model methods that were calling flush

**References**:
- SQLAlchemy Session Basics: https://docs.sqlalchemy.org/en/20/orm/session_basics.html
- Unit of Work Pattern: https://martinfowler.com/eaaCatalog/unitOfWork.html

**Action Items**:
- [ ] Audit all repository methods to identify which truly need immediate IDs
- [ ] Consider implementing session-per-request properly for Azure Functions
- [ ] Evaluate if returning objects instead of IDs would simplify the architecture
- [ ] Document session lifecycle expectations for new developers

---

## Medium Priority

### Test Coverage Improvements

**Problem**: 
Integration tests don't catch all schema transformation issues.

**Specific Example**:
The TW schema test script only validated API → TW schema, but missed TW schema → Canonical schema transformation errors.

**Action Items**:
- [ ] Update test scripts to validate full transformation pipelines
- [ ] Add integration tests for mapper transformations
- [ ] Ensure all optional field handling is tested

---

## Low Priority

### Documentation

- [ ] Document the custom `EmptyStrIsNone` type pattern for other integrations
- [ ] Create integration testing best practices guide
- [ ] Document token coordination pattern for Azure Functions

### JSONB Datetime Serialization Issue

**Problem**:
Datetime objects are not JSON serializable when storing in JSONB columns.

**Affected Areas**:
- Entity.attributes (when processors return data with datetime fields)
- Entity.processing_results (fixed - already uses mode='json')
- Any model creation via BaseRepository._create_with_tenant (fixed)

**Current Fixes**:
- ✅ tw_order_mapper.py: Changed to `model_dump(mode='json')`
- ✅ entity_repository.add_processing_result: Already uses `mode='json'`
- ✅ base_repository._create_with_tenant: Changed to `model_dump(mode='json')`

**Still Need to Fix**:
- [ ] Any code that updates Entity.attributes with datetime values
- [ ] Check all mappers to ensure they use `model_dump(mode='json')`
- [ ] Consider a custom JSON encoder for SQLAlchemy JSONB columns

**Action Items**:
- [ ] Audit all code paths that write to JSONB columns
- [ ] Add integration tests that include datetime fields
- [ ] Consider centralizing JSON serialization logic
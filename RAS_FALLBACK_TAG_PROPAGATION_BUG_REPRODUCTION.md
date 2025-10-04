# RAS Fallback Tag Propagation Bug Reproduction

This document describes the test created to reproduce the bug reported in [GitHub Issue #7763](https://github.com/apache/incubator-gluten/issues/7763).

## Issue Summary

**Problem**: When RAS (Rule-based Adaptive Selection) is enabled, fallback tags added during RAS rule processing don't propagate back to the input plan. This causes the fallback reporter to show generic "Gluten doesn't support..." messages instead of detailed fallback information.

**Impact**: Users lose visibility into the specific reasons why certain operations fall back to vanilla Spark execution, making debugging and optimization more difficult.

## Root Cause Analysis

The issue is located in `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/enumerated/RasOffload.scala`:

### Evidence in Code

1. **Lines 136-138**: TODO comment stating:
   ```scala
   // TODO: Tag the original plan with fallback reason. This is a non-trivial work
   // in RAS as the query plan we got here may be a copy so may not propagate tags
   // to original plan.
   ```

2. **Lines 150-152**: Same TODO comment repeated for another code path.

3. **Lines 94, 142, 156**: FIXME comments indicating temporary workarounds:
   ```scala
   // FIXME: This is a solution that is not completely verified for all cases, however
   // it's no harm anyway so we temporarily apply this practice.
   ```

### Technical Details

The problem occurs because:

1. RAS rules work on **copies** of the query plan during optimization
2. When validation fails, fallback tags are added to these **copied plans**
3. The tags don't propagate back to the **original plan** that the fallback reporter sees
4. Result: Generic fallback messages instead of detailed reasons

## Test Implementation

### Location
```
gluten-ut/spark34/src/test/scala/org/apache/spark/sql/gluten/RasFallbackTagPropagationSuite.scala
```

### Test Strategy

The test suite includes two main test cases:

#### 1. `RAS fallback tag propagation issue`
- Creates a complex query scenario with aggregations and filtering
- Compares fallback events between RAS enabled/disabled configurations
- Captures detailed fallback information through Spark listeners
- Analyzes the differences in fallback reporting

#### 2. `Simple RAS fallback tag propagation test`
- Simplified test with minimal setup for easier debugging
- Forces fallback by disabling columnar file scan
- Compares fallback reasons between configurations
- Provides clear output showing the difference

### Expected Behavior vs Actual Behavior

**Expected**: Detailed fallback reasons should be preserved regardless of RAS setting
- Example: `"[FallbackByUserOptions] Validation failed on node Scan parquet..."`

**Actual with RAS enabled**: Generic or missing fallback details
- Example: `"Gluten doesn't support..."` or empty reasons

**Actual with RAS disabled**: Detailed fallback reasons are preserved
- Example: Specific validation failure messages with context

## How to Run the Test

### Prerequisites
- Java 8 or 11
- Maven 3.6+
- Spark 3.4.x dependencies

### Running the Test
```bash
# Navigate to the project root
cd incubator-gluten

# Run the specific test suite
mvn test -Dtest=RasFallbackTagPropagationSuite -pl gluten-ut/spark34

# Or run a specific test method
mvn test -Dtest=RasFallbackTagPropagationSuite#"Simple RAS fallback tag propagation test" -pl gluten-ut/spark34
```

### Expected Output

When the bug is present, you should see output like:
```
=== FALLBACK REASONS COMPARISON ===
RAS DISABLED reasons (1):
  - [FallbackByUserOptions] Validation failed on node Scan parquet spark_catalog.default.simple_test

RAS ENABLED reasons (1):
  - Gluten doesn't support this operation

✓ BUG REPRODUCED: RAS enabled loses detailed fallback information
  This confirms the issue described in https://github.com/apache/incubator-gluten/issues/7763
```

When the bug is fixed, you should see:
```
=== FALLBACK REASONS COMPARISON ===
RAS DISABLED reasons (1):
  - [FallbackByUserOptions] Validation failed on node Scan parquet spark_catalog.default.simple_test

RAS ENABLED reasons (1):
  - [FallbackByUserOptions] Validation failed on node Scan parquet spark_catalog.default.simple_test

✓ Bug appears to be FIXED: Both RAS enabled/disabled show detailed reasons
```

## Potential Solutions

Based on the code analysis, potential solutions include:

1. **Plan Reference Tracking**: Maintain references between copied plans and original plans during RAS processing
2. **Tag Propagation Mechanism**: Implement a mechanism to propagate tags from copied plans back to original plans
3. **Centralized Fallback Tracking**: Use a centralized fallback tracking system that doesn't rely on plan tags
4. **Post-Processing Tag Merge**: After RAS processing, merge fallback tags from all plan variants

## Related Files

- `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/enumerated/RasOffload.scala` - Main issue location
- `gluten-core/src/main/scala/org/apache/gluten/extension/columnar/FallbackTag.scala` - Fallback tag implementation
- `gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala` - Fallback reporting
- `gluten-ras/` - RAS implementation modules

## Contributing

If you're working on fixing this issue:

1. Run the test to confirm you can reproduce the bug
2. Implement your fix in the relevant files
3. Run the test again to verify the fix works
4. Ensure all existing tests still pass
5. Consider adding additional test cases for edge cases

## References

- [GitHub Issue #7763](https://github.com/apache/incubator-gluten/issues/7763)
- [RAS Documentation](docs/developers/ras.md) (if available)
- [Gluten Architecture Documentation](docs/developers/architecture.md) (if available)
# Gluten Suite Mapping Report - Spark 40

Generated at: /home/chang/SourceCode/gluten1

## Summary Statistics

- **Total Gluten Suites**: 441
- **Mapped to Spark 40**: 430
- **Not Mapped**: 11
- **Unique Packages Covered**: 20
- **Total Missing Suite Files**: 1
- **Total Missing Suite Classes**: 1

---

## Missing Suites by Package

*Review this section to decide which packages to process*

### Package: `org/apache/spark/sql/connector`

**Missing Suite Files**: 1

- **StaticProcedureSuite.scala**
  - Spark: `/home/chang/OpenSource/spark40/sql/core/src/test/scala/org/apache/spark/sql/connector/StaticProcedureSuite.scala`
  - Gluten: `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/connector/GlutenStaticProcedureSuite.scala`
  - Package: `org.apache.spark.sql.connector`
  - Classes (1):
    - `StaticProcedureSuite` extends `SharedSparkSession` → `GlutenTestsCommonTrait`

---

## Recommended Processing Order

Process packages in this order (smallest to largest):

1. `org/apache/spark/sql/connector` (1 files, 1 classes)

---

## Next Steps

1. Review the missing suites above
2. Choose packages to process
3. Run for each package:
   ```bash
   conda run -n base python /tmp/generate_gluten_suites.py \
     --spark-version 40 \
     --package <package_name>
   ```
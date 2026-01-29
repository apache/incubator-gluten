# Gluten Suite Mapping Report - Spark 41

Generated at: /home/chang/SourceCode/gluten1

## Summary Statistics

- **Total Gluten Suites**: 256
- **Mapped to Spark 41**: 0
- **Not Mapped**: 256
- **Unique Packages Covered**: 0
- **Total Missing Suite Files**: 0
- **Total Missing Suite Classes**: 0

---

## Missing Suites by Package

*Review this section to decide which packages to process*

---

## Recommended Processing Order

Process packages in this order (smallest to largest):


---

## Next Steps

1. Review the missing suites above
2. Choose packages to process
3. Run for each package:
   ```bash
   conda run -n base python /tmp/generate_gluten_suites.py \
     --spark-version 41 \
     --package <package_name>
   ```
## Partial Projection support
In Gluten there is still a gap on supporting all the expression natively(e.g. Java UDFs). So sometime Gluten will choose JVM code path to run the code. This will introduce performance regressions. Partial projections was added to improve the performance in this case. 

## Detail implementations

### Adding partial projection for UDF
For example, hash(udf(col0)), col1, col2, col3, col4, with partial project, we can only convert col0 as column to row or column to arrow as input, and convert udf(col0) as Alias partitalProject1_, then row to column.
ProjectExecTransformer will be hash(partitalProject1_), col1, col2, col3, col4, partitalProject1_.
This feature will save the cost on converting the columnar format to row format and vice-versa

### Adding partial projection for unsupported expressions
The partial projection feature can also benefit from not natively supported expressions. E.g, substr(from_json(col_a)). Due to from_json is not fully supported, Gluten may use JVM code path. 
Instead of project on the whole expression, partial project will try to do projection with from_json() and native projection of substr()

## Limitations

This feature is still under heavy development.
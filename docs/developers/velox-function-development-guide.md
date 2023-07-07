---
layout: page
title: Velox Function Development
nav_order: 5
parent: Developer Overview
---
# Developer Guide for Implementing Spark Built-in SQL Functions in Velox

In velox, two folders `prestosql` & `sparksql` are holding most sql functions, respective for `presto` and `spark`. Gluten will ask velox to firstly register `prestosql` functions, then `sparksql` functions. So if `prestosql`
and `sparksql` share same signature for a function, the `sparksql` function will overwrite the corresponding `prestosql` function. If the required function is lacking in both folders (exceptions are some common functions defined
outside, like `cast`), we need to implement the missing function in `sparksql` folder. It is possible that a `prestosql` function has some semantic difference with the corresponding spark function, even though they share the
same name and function signature. If so, we also need to do an implementation in `sparksql` folder, generally based on the original impl. for `prestosql`.

There are a few spark functions that can behave differently for some special cases, depending on ANSI on or off. Currently, gluten does NOT support ANSI mode. So only ANSI off needs to be considered in implementing spark
built-in functions in velox.

Take `BitwiseAndFunction` as example:

```
template <typename T>
struct BitwiseAndFunction {
  template <typename TInput>
  // For void return type, it indicates null result will never be obtained for non-null input.
  // For bool return type, it indicates null result can be obtained for non-null input (false for null).
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a & b;
  }
};
``` 
It is templated, as well as the `call` function, to allow multiple types. In the above impl., the result will be null for null input.
Please use `callNullable` if you need different behavior for null input, e.g., get a non-null result for null input. Also see `callNullFree` in velox document.
It is used for fast evaluation in the case that any input has null.

The below code will register the implemented function for all kinds of integer types. The specified name `bitwise_and` will be actually used in calling this function.
```
registerBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
```

Functions for complex types have similar implementations. 
See `ArrayAverageFunction` in [velox/functions/prestosql/ArrayFunctions.h](https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/ArrayFunctions.h).

### Reference:
Velox’s official developer guide:
  * [velox/docs/develop/scalar-functions.rst](https://github.com/facebookincubator/velox/blob/main/velox/docs/develop/scalar-functions.rst)
  * [velox/examples/SimpleFunctions.cpp](https://github.com/facebookincubator/velox/blob/main/velox/examples/SimpleFunctions.cpp)

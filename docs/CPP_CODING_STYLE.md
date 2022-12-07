# Gluten CPP Core Guidelines

This is a set of CPP core guidelines for Gluten, The aim is to make the codebase
simper, more efficient, more maintainable by promoting consistency and according
to best practices.

## Philosophy

Philosophical rules are generally not measurable. However, they are valuable. for 
Gluten CPP coding, there are a few Philosophical rules as the following.

* Write in ISO Standard C++.
* Write code consistently.
* Keep simple, Make code clear and easy.
* Optimize code for the reader, not the writer, more time will be spent reading
	code than writing it.

## Code Formatting

Many aspects of C++ coding style will be covered by clang-format, such as spacing,
line width, indentation and ordering (for includes, using directives and etc). 

* Always ensure your code is clang-format compatible.
* `tools/formatcppcode.sh` is provided for formating CPP codes.

## Naming Conventions

* Use **PascalCase** for types (classes, structs, enums, type aliases, type
  template parameters) and file names.
* Use **camelCase** for functions, member and local variables, and non-type
  template parameters.
* Use **camelCase_** for private and protected members variables.
* Use **snake_case** for namespace names and build targets.
* Use **UPPER_SNAKE_CASE** for macros.
* Use **kPascalCase** for static constants and enumerators.

## Files

* All header files must have a single-inclusion guard using `#pragma once`
* Include header files should satisfy the following rules.
	* Include the necessary header files, which means the source file (.cc) containing 
	the only one line `#include "test.h"` can be compiled successfully without
	including any other header files.
	* Do not include any unnecessary header files, the more including, the slower
	compiling.
	* In on word, no more, no less, just OK.
* One file should contain one main class, and the file name should be consistent with
	the main class name.
	* Obvious exception: files used for defining various misc functions.
* If a header file has a corresponding source file, they should have the same file
	name with different suffix, such as `a.h vs a.cc`.

## Classes

* Base class name do not end with Base, use `Backend` instead of `BackendBase`.
* Distinguish interface from implementation, make implementations private.
* When designing a class hierarchy, distinguish between interface inheritance
	and implementation inheritance.
	* Ensure that public inheritance represent the relation of `is-a`.
	* Ensure that private inheritance represent the relation of `implements-with`.
* Don't make a function `virtual` without reason.
* Use `override` to make overriding explicit and to make the compiler work.
* Use	`const` to mark the member function as read-only.
* When you try to define a `copy constructor` or a `operator=` for a class, remember
	the `Rule of three/five/zero`.

## Functions

* Make functions short and simple.
* Give the function a good name, how to check whether the function name is good
	or not.
	* When you read it loudly, you feel smooth.
	* The information can be represented by arguments should not be encoded into 
		the function name. such as. use `get` instead of `getByIndex`.
* A function should focus on a single logic operation.
* A function should do as the name meaning.
	* do everything converd by the function name
	* don't do anything not convered by the function name

## Variables

* Make variables name simple and meaningful.
* Don't group all your variables at the top of the scope, it's a outdated habit.
* Declare variables as close to the usage point as possible.

## Constants
* Prefer const variables to using preprocessor (`#define`) to define constant values.

## Macros

* Macros downgrade readability, break mind, and affect debug.
* Macros have side effects.
* Use macros cautiously and carefully.
* Consider using `const` variables or `inline` functions to replace macros.
* Consider defining macros with the wrap of `do {...} while (0)`
* Avoid using 3rd party library macros directly.

## Namespaces

* Don't `using namespace xxx` in header files.
* Place all Gluten CPP codes under `namespace gluten` because one level namespace
	is enough, no nested namespace, too much level namespaces bring mess.
* Anonymous namespace is used for defining file level classes,functions,variables

## Resource Management

* Use handles and RAII to manage resources automatically.
* Immediately give the result of an explicit resource allocation to a manager 
	object.
* Prefer scoped objects, Prefer stack objects.
* Use raw pointers to denote individual objects.
* A raw pointer (a `T*`) is non-owning.
* A raw reference (a `T&`) is non-owning.
* Understand the difference of `unique_ptr`, `shared_ptr`, `weak_ptr`.
	* `unique_ptr` represents ownership, but not share ownership. `unique_ptr` is
		equivalent to RAII, release the resouce when the object is destructed.
	* `shared_ptr` represents share ownership by use-count, it is more expensive 
		that `unqiue_ptr`.
	* `weak_ptr` models temporary ownership, it is used to break reference cycles 
		formed by objects managed by `shared_ptr`.
* Use `unique_ptr` or `shared_ptr` to represent ownership.
* Prefer `unique_ptr` over `shared_ptr` unless you need to share ownership.
* Use `make_unique` to make `unique_ptr`s.
* Use `make_shared` to make `shared_ptr`s.
* Take smart pointers as parameters only to explicitly express lifetime semantics
* **For general use**, take `T*` or `T&` arguments rather than smart pointers.

## Exceptions

* The exception specifications are changing always, the difference between various CPP
	standards is big, so Gluten use exception cautiously.
* Prefer `return code` to throwing exceptions.
* Prefer compile-time checking to run-time checking.
* Encapsulate messy costructs, rather than spreading through the code.

## Comments

* Add necessary comments, the comment is not the more the better, also not the
	less the better.
* The comment is used to express the writer's mind that couldn't be represented
	as code, it's not encouraged to add obvious comments.

## References
* [CppCoreGuidelines](https://github.com/fluz/CppCoreGuidelines)
* [Velox CODING_STYLE](https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md)
* thanks Gluten developers for your wise suggestions and helps


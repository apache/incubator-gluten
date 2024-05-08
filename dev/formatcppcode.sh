cd `dirname $0`
find ../cpp/core -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;
find ../cpp/velox -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;

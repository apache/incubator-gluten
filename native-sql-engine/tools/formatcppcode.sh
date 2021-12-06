find . -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format -style=file -i {} \;

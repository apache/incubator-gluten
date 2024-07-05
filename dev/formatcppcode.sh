cd `dirname $0`

# Check if clang-format-15 is installed
if ! command -v clang-format-15 &> /dev/null
then
    echo "clang-format-15 could not be found"
    echo "Installing clang-format-15..."
    sudo apt update
    sudo apt install clang-format-15
fi

find ../cpp/core -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;
find ../cpp/velox -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;
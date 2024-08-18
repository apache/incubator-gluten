cd `dirname $0`

CLANG_FORMAT_CMD=""
if [ "$(uname)" == "Darwin" ]; then
    CLANG_FORMAT_CMD="clang-format"
    if ! command -v $CLANG_FORMAT_CMD &> /dev/null
    then
        echo "$CLANG_FORMAT_CMD could not be found"
        echo "Installing $CLANG_FORMAT_CMD..."
        sudo brew install $CLANG_FORMAT_CMD
    fi
else
    CLANG_FORMAT_CMD="clang-format-18"
    if ! command -v $CLANG_FORMAT_CMD &> /dev/null
    then
        echo "$CLANG_FORMAT_CMD could not be found"
        echo "Installing $CLANG_FORMAT_CMD..."
        sudo apt update
        sudo apt install $CLANG_FORMAT_CMD
    fi
fi

echo "OS: $(uname), CLANG_FORMAT_CMD: ${CLANG_FORMAT_CMD}"
find ../cpp/core -type f \( -name "*.cc" -o  -name "*.hpp" -o  -name "*.cu" -o  -name "*.c" -o -name "*.h" \) | xargs ${CLANG_FORMAT_CMD} -style=file -i
find ../cpp/velox -type f \( -name "*.cc" -o  -name "*.hpp" -o  -name "*.cu" -o  -name "*.c" -o -name "*.h" \) | xargs ${CLANG_FORMAT_CMD} -style=file -i

#include <Functions/SparkFunctionHashingExtended.h>
#include <base/types.h>
#include <gtest/gtest.h>
#include <MurmurHash3.h>

using namespace local_engine;
using namespace DB;

TEST(Hash, SparkMurmurHash3_32)
{
    char buf[2] = {0, static_cast<char>(0xc8)};
    {
        UInt32 result = SparkMurmurHash3_32::apply(buf, sizeof(buf), 42);
        EXPECT_EQ(static_cast<Int32>(result), -424716282);
    }

    {
        UInt32 result = 0;
        MurmurHash3_x86_32(buf, 2, 42, &result);
        EXPECT_EQ(static_cast<Int32>(result), -1346355085);
    }
}

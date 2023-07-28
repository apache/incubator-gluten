#include "SparkFunctionConv.h"
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/TransformDateTime64.h>
//#include <boost/type_traits/type_with_alignment.hpp>
#include <Poco/Logger.h>
#include <Poco/Types.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "DataTypes/IDataType.h"
#include "base/Decimal.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

  /**
  Converts a 64-bit integer value to its character form and moves it to the
  destination buffer followed by a terminating NUL. If radix is -2..-36, val is
  taken to be SIGNED, if radix is 2..36, val is taken to be UNSIGNED. That is,
  val is signed if and only if radix is. All other radixes are treated as bad
  and nothing will be changed in this case.

  For conversion to decimal representation (radix is -10 or 10) one should use
  the optimized #longlong10_to_str() function instead.
*/

constexpr std::array<const char, 37> dig_vec_upper{
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"};
constexpr std::array<const char, 37> dig_vec_lower{
    "0123456789abcdefghijklmnopqrstuvwxyz"};

char * ll2str(int64_t val, char * dst, int radix, bool upcase)
{
    char buffer[65];
    const char *const dig_vec =
        upcase ? dig_vec_upper.data() : dig_vec_lower.data();
    auto uval = static_cast<uint64_t>(val);

    if (radix < 0) {
      if (radix < -36 || radix > -2) return nullptr;
      if (val < 0) {
        *dst++ = '-';
        /* Avoid integer overflow in (-val) for LLONG_MIN (BUG#31799). */
        uval = 0ULL - uval;
      }
      radix = -radix;
    } else if (radix > 36 || radix < 2) {
      return nullptr;
    }

    char *p = std::end(buffer);
    do {
      *--p = dig_vec[uval % radix];
      uval /= radix;
    } while (uval != 0);

    const size_t length = std::end(buffer) - p;
    memcpy(dst, p, length);
    dst[length] = '\0';
    return dst + length;
}

static inline char *longlong2str(int64_t val, char *dst, int radix) {
  return ll2str(val, dst, radix, true);
}

DB::DataTypePtr SparkFunctionConv::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.size() != 3)
        throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 3.",
            getName(), arguments.size());

    if (!isInteger(arguments[1]))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function {} must be Int", getName());
    if (!isInteger(arguments[2]))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Thrid argument for function {} must be Int", getName());

    auto arg0_type = DB::removeNullable(arguments[0]);
    return std::make_shared<DB::DataTypeNullable>(arg0_type);
}

unsigned long long my_strntoull_8bit(const char *nptr,
                                     size_t l, int base, const char **endptr,
                                     int *err)
{
    int negative;
    unsigned long long cutoff = 0;
    unsigned cutlim = 0;
    unsigned long long i = 0;
    const char *save = nullptr;
    int overflow = 0;

    *err = 0; /* Initialize error indicator */

    const char *s = nptr;
    const char *e = nptr + l;

    for (; s < e && std::isspace(*s); s++)
      ;

    if (s == e)
        goto noconv;

    if (*s == '-')
    {
        negative = 1;
        ++s;
    } else if (*s == '+') {
        negative = 0;
        ++s;
    } else
        negative = 0;

    save = s;

    cutoff = (~static_cast<unsigned long long>(0)) / static_cast<unsigned long int>(base);
    cutlim = static_cast<unsigned>(((~static_cast<unsigned long long>(0)) % static_cast<unsigned long int>(base)));

    overflow = 0;
    i = 0;
    for (; s != e; s++) {
        uint8_t c = *s;

        if (c >= '0' && c <= '9')
          c -= '0';
        else if (c >= 'A' && c <= 'Z')
          c = c - 'A' + 10;
        else if (c >= 'a' && c <= 'z')
          c = c - 'a' + 10;
        else
          break;
        if (c >= base) break;
        if (i > cutoff || (i == cutoff && c > cutlim))
          overflow = 1;
        else {
          i *= static_cast<unsigned long long>(base);
          i += c;
        }
    }

    if (s == save) goto noconv;

    if (endptr != nullptr) *endptr = s;

    if (overflow) {
      err[0] = ERANGE;
      return (~static_cast<unsigned long long>(0));
    }

    return negative ? -i : i;

  noconv:
    err[0] = EDOM;
    if (endptr != nullptr) *endptr = nptr;
    return 0L;
}

DB::ColumnPtr SparkFunctionConv::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const
{
    using longlong = DB::Int64;
    auto from_base = static_cast<int>(arguments[1].column->getInt(0));
    auto to_base = static_cast<int>(arguments[2].column->getInt(0));

    auto result = result_type->createColumn();
    result->reserve(input_rows_count);
    // Note that abs(INT_MIN) is undefined.
    if (from_base == INT_MIN || to_base == INT_MIN || abs(to_base) > 36 || abs(to_base) < 2 || abs(from_base) > 36 || abs(from_base) < 2)
    {
        return result;
    }
    //bool null_value = false;
    //bool unsigned_flag = !(from_base < 0);

    longlong dec;
    const char * endptr;
    int err;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        auto res = arguments[0].column->getDataAt(i).toString();

        if (from_base < 0)
            dec = my_strntoull_8bit(res.data(), res.length(), -from_base, &endptr, &err);
        else
            dec = static_cast<longlong>(my_strntoull_8bit(res.data(), res.length(), from_base, &endptr, &err));
        if (err)
        {
          /*
              If we got an overflow from my_strntoull, and the input was negative,
              then return 0 rather than ~0
              This is in order to be consistent with
                CAST(<large negative value>, unsigned)
              which returns zero.
              */
            dec = -1;
        }
        static constexpr uint32_t CONV_MAX_LENGTH = 64U + 1U;
        char ans[CONV_MAX_LENGTH + 1U];
        char * ptr = longlong2str(dec, ans, to_base);
        result->insertData(ptr, ptr - ans);
    }
    return result;
}

REGISTER_FUNCTION(SparkFunctionConv)
{
    factory.registerFunction<SparkFunctionConv>();
}
}

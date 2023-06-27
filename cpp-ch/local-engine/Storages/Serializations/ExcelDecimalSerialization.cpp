#include "ExcelDecimalSerialization.h"
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationString.h>
#include "ExcelDecimalReader.h"
#include "ExcelStringReader.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{

template <is_decimal T>
void ExcelDecimalSerialization<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & formatSettings) const
{
    deserializeExcelDecimalText<T>(column, istr, precision, scale, formatSettings);
}

template class ExcelDecimalSerialization<Decimal32>;
template class ExcelDecimalSerialization<Decimal64>;
template class ExcelDecimalSerialization<Decimal128>;
template class ExcelDecimalSerialization<Decimal256>;
}

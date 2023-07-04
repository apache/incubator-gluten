#include "SourceFromJavaIter.h"
#include <Columns/ColumnNullable.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <jni/jni_common.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/Exception.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass SourceFromJavaIter::serialized_record_batch_iterator_class = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_hasNext = nullptr;
jmethodID SourceFromJavaIter::serialized_record_batch_iterator_next = nullptr;


static DB::Block getRealHeader(const DB::Block & header)
{
    if (header.columns())
        return header;
    return BlockUtil::buildRowCountHeader();
}
SourceFromJavaIter::SourceFromJavaIter(DB::Block header, jobject java_iter_)
    : DB::ISource(getRealHeader(header)), java_iter(java_iter_), original_header(header)
{
}
DB::Chunk SourceFromJavaIter::generate()
{
    GET_JNIENV(env)
    jboolean has_next = safeCallBooleanMethod(env, java_iter, serialized_record_batch_iterator_hasNext);
    DB::Chunk result;
    if (has_next)
    {
        jbyteArray block = static_cast<jbyteArray>(safeCallObjectMethod(env, java_iter, serialized_record_batch_iterator_next));
        DB::Block * data = reinterpret_cast<DB::Block *>(byteArrayToLong(env, block));
        if (data->rows() > 0)
        {
            size_t rows = data->rows();
            if (original_header.columns())
            {
                buildChunk(data, result);
                auto info = std::make_shared<DB::AggregatedChunkInfo>();
                info->is_overflows = data->info.is_overflows;
                info->bucket_num = data->info.bucket_num;
                result.setChunkInfo(info);
            }
            else
            {
                result = BlockUtil::buildRowCountChunk(rows);
            }
        }
    }
    CLEAN_JNIENV
    return result;
}
SourceFromJavaIter::~SourceFromJavaIter()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(java_iter);
    CLEAN_JNIENV
}
Int64 SourceFromJavaIter::byteArrayToLong(JNIEnv * env, jbyteArray arr)
{
    jsize len = env->GetArrayLength(arr);
    assert(len == sizeof(Int64));
    char * c_arr = new char[len];
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte *>(c_arr));
    std::reverse(c_arr, c_arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(c_arr)[0];
    delete[] c_arr;
    return result;
}
void SourceFromJavaIter::buildChunk(DB::Block * block, DB::Chunk & chunk)
{
    // the header of block may be different with the header of output, please refer to GLUTEN-2198 for more details.
    auto output = this->getOutputs().front().getHeader();
    auto rows = block->rows();
    auto columns = block->getColumns();
    DB::Columns out_columns = DB::Columns(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto pos = i;
        try
        {
            pos = block->getPositionByName(output.getByPosition(i).name);
        } catch (DB::Exception e) {
            pos = getPositionByNameForUnion(block, output.getByPosition(i).name);
        }

        DB::WhichDataType which(columns.at(pos)->getDataType());
        if (output.getByPosition(i).type->isNullable() && !which.isNullable() && !which.isAggregateFunction())
        {
            out_columns[i] = DB::makeNullable(columns.at(pos));
        }
        else
        {
            out_columns[i] = columns.at(pos);
        }
    }
    chunk.setColumns(out_columns, rows);
}

int SourceFromJavaIter::getPositionByNameForUnion(DB::Block * header, std::string & output_name)
{
    // For union, the column names are different between inputs, for example day#53 vs day#75
    std::string output_prefix = output_name.substr(0, output_name.find('#'));
    int i = 0;
    for (auto name : header->getNames())
    {
        std::string header_prefix = name.substr(0, name.find('#'));
        if (header_prefix == output_prefix)
        {
            return i;
        }
        i++;
    }
    throw DB::Exception::createRuntime(DB::ErrorCodes::LOGICAL_ERROR, "SourceFromJavaIter::getPositionByNameForUnion failed. header:" + header->dumpStructure() + " output column:" + output_name);
}
}

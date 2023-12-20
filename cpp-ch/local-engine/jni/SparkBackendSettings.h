/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <jni.h>
#include <string>
namespace local_engine
{
class SparkBackendSettings
{
public:
    static jclass backend_settings_class;
    static jmethodID get_long_conf_method;

    static constexpr auto ch_runtime_conf_prefix = "spark.gluten.sql.columnar.backend.ch.runtime_config.";

    static constexpr long default_max_source_concatenate_rows = 4 * 1024;
    static constexpr long default_max_source_concatenate_bytes = 32 * 1024 * 1024;
    static constexpr auto max_source_concatenate_rows_key = "max_source_concatenate_rows";
    static constexpr auto max_source_concatenate_bytes_key = "max_source_concatenate_bytes";

    static long getRuntimeLongConf(const std::string & key, long default_value)
    {
        return getLongConf(ch_runtime_conf_prefix + key, default_value);
    }
    static long getLongConf(const std::string & key, long default_value);
};
}

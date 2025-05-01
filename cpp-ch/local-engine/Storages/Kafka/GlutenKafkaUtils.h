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

#pragma once

#include <Access/KerberosInit.h>
#include <Storages/Kafka/KafkaConfigLoader.h>
#include <cppkafka/configuration.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace local_engine
{
using namespace DB;

class GlutenKafkaUtils
{
public:
    static void setKafkaConfigValue(cppkafka::Configuration & kafka_config, const String & key, const String & value);

    static void
    loadNamedCollectionConfig(cppkafka::Configuration & kafka_config, const String & collection_name, const String & config_prefix);

    static void loadConfigProperty(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const String & tag);

    static void loadTopicConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const String & config_prefix,
        const String & topic);

    static void loadFromConfig(
        cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix);

    static void updateGlobalConfiguration(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params);

    static void loadLegacyTopicConfig(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const String & config_prefix);

    static void loadLegacyConfigSyntax(
        cppkafka::Configuration & kafka_config,
        const Poco::Util::AbstractConfiguration & config,
        const String & collection_name,
        const Names & topics);

    static void loadConsumerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params);


    static cppkafka::Configuration getConsumerConfiguration(const KafkaConfigLoader::ConsumerConfigParams & params);
};


}

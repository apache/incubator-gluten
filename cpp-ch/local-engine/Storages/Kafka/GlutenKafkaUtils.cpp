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

#include "GlutenKafkaUtils.h"


#include <Storages/Kafka/parseSyslogLevel.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/config_version.h>

namespace local_engine
{
using namespace DB;
void GlutenKafkaUtils::setKafkaConfigValue(cppkafka::Configuration & kafka_config, const String & key, const String & value)
{
    /// "log_level" has valid underscore, the remaining librdkafka setting use dot.separated.format which isn't acceptable for XML.
    /// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    const String setting_name_in_kafka_config = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
    kafka_config.set(setting_name_in_kafka_config, value);
}

void GlutenKafkaUtils::loadNamedCollectionConfig(
    cppkafka::Configuration & kafka_config, const String & collection_name, const String & config_prefix)
{
    const auto & collection = DB::NamedCollectionFactory::instance().get(collection_name);
    for (const auto & key : collection->getKeys(-1, config_prefix))
    {
        // Cut prefix with '.' before actual config tag.
        const auto param_name = key.substr(config_prefix.size() + 1);
        setKafkaConfigValue(kafka_config, param_name, collection->get<String>(key));
    }
}

void GlutenKafkaUtils::loadConfigProperty(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & tag)
{
    const String property_path = config_prefix + "." + tag;
    const String property_value = config.getString(property_path);

    setKafkaConfigValue(kafka_config, tag, property_value);
}


void GlutenKafkaUtils::loadTopicConfig(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const String & config_prefix,
    const String & topic)
{
    if (!collection_name.empty())
    {
        const auto topic_prefix = fmt::format("{}.{}", config_prefix, KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG);
        const auto & collection = NamedCollectionFactory::instance().get(collection_name);
        for (const auto & key : collection->getKeys(1, config_prefix))
        {
            /// Only consider key <kafka_topic>. Multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
            if (!key.starts_with(topic_prefix))
                continue;

            const String kafka_topic_path = config_prefix + "." + key;
            const String kafka_topic_name_path = kafka_topic_path + "." + KafkaConfigLoader::CONFIG_NAME_TAG;
            if (topic == collection->get<String>(kafka_topic_name_path))
                /// Found it! Now read the per-topic configuration into cppkafka.
                loadNamedCollectionConfig(kafka_config, collection_name, kafka_topic_path);
        }
    }
    else
    {
        /// Read all tags one level below <kafka>
        Poco::Util::AbstractConfiguration::Keys tags;
        config.keys(config_prefix, tags);

        for (const auto & tag : tags)
        {
            if (tag == KafkaConfigLoader::CONFIG_NAME_TAG)
                continue; // ignore <name>, it is used to match topic configurations
            loadConfigProperty(kafka_config, config, config_prefix, tag);
        }
    }
}


void GlutenKafkaUtils::loadFromConfig(
    cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params, const String & config_prefix)
{
    if (!params.collection_name.empty())
    {
        loadNamedCollectionConfig(kafka_config, params.collection_name, config_prefix);
        return;
    }

    /// Read all tags one level below <kafka>
    Poco::Util::AbstractConfiguration::Keys tags;
    params.config.keys(config_prefix, tags);

    for (const auto & tag : tags)
    {
        if (tag == KafkaConfigLoader::CONFIG_KAFKA_PRODUCER_TAG || tag == KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG)
            /// Do not load consumer/producer properties, since they should be separated by different configuration objects.
            continue;

        if (tag.starts_with(
                KafkaConfigLoader::CONFIG_KAFKA_TOPIC_TAG)) /// multiple occurrences given as "kafka_topic", "kafka_topic[1]", etc.
        {
            // Update consumer topic-specific configuration (new syntax). Example with topics "football" and "baseball":
            //     <kafka>
            //         <kafka_topic>
            //             <name>football</name>
            //             <retry_backoff_ms>250</retry_backoff_ms>
            //             <fetch_min_bytes>5000</fetch_min_bytes>
            //         </kafka_topic>
            //         <kafka_topic>
            //             <name>baseball</name>
            //             <retry_backoff_ms>300</retry_backoff_ms>
            //             <fetch_min_bytes>2000</fetch_min_bytes>
            //         </kafka_topic>
            //     </kafka>
            // Advantages: The period restriction no longer applies (e.g. <name>sports.football</name> will work), everything
            // Kafka-related is below <kafka>.
            for (const auto & topic : params.topics)
            {
                /// Read topic name between <name>...</name>
                const String kafka_topic_path = config_prefix + "." + tag;
                const String kafka_topic_name_path = kafka_topic_path + "." + KafkaConfigLoader::CONFIG_NAME_TAG;
                const String topic_name = params.config.getString(kafka_topic_name_path);

                if (topic_name != topic)
                    continue;
                loadTopicConfig(kafka_config, params.config, params.collection_name, kafka_topic_path, topic);
            }
            continue;
        }
        if (tag.starts_with(KafkaConfigLoader::CONFIG_KAFKA_TAG))
            /// skip legacy configuration per topic e.g. <kafka_TOPIC_NAME>.
            /// it will be processed is a separate function
            continue;
        // Update configuration from the configuration. Example:
        //     <kafka>
        //         <retry_backoff_ms>250</retry_backoff_ms>
        //         <fetch_min_bytes>100000</fetch_min_bytes>
        //     </kafka>
        loadConfigProperty(kafka_config, params.config, config_prefix, tag);
    }
}


void GlutenKafkaUtils::updateGlobalConfiguration(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    loadFromConfig(kafka_config, params, KafkaConfigLoader::CONFIG_KAFKA_TAG);

#if USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.kinit.cmd"))
        LOG_WARNING(params.log, "sasl.kerberos.kinit.cmd configuration parameter is ignored.");

    kafka_config.set("sasl.kerberos.kinit.cmd", "");
    kafka_config.set("sasl.kerberos.min.time.before.relogin", "0");

    if (kafka_config.has_property("sasl.kerberos.keytab") && kafka_config.has_property("sasl.kerberos.principal"))
    {
        String keytab = kafka_config.get("sasl.kerberos.keytab");
        String principal = kafka_config.get("sasl.kerberos.principal");
        LOG_DEBUG(params.log, "Running KerberosInit");
        try
        {
            kerberosInit(keytab, principal);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(params.log, "KerberosInit failure: {}", getExceptionMessage(e, false));
        }
        LOG_DEBUG(params.log, "Finished KerberosInit");
    }
#else // USE_KRB5
    if (kafka_config.has_property("sasl.kerberos.keytab") || kafka_config.has_property("sasl.kerberos.principal"))
        LOG_WARNING(params.log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
#endif // USE_KRB5
    // No need to add any prefix, messages can be distinguished
    kafka_config.set_log_callback(
        [log = params.log](cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message)
        {
            auto [poco_level, client_logs_level] = parseSyslogLevel(level);
            const auto & kafka_object_config = handle.get_configuration();
            const std::string client_id_key{"client.id"};
            chassert(kafka_object_config.has_property(client_id_key) && "Kafka configuration doesn't have expected client.id set");
            LOG_IMPL(
                log,
                client_logs_level,
                poco_level,
                "[client.id:{}] [rdk:{}] {}",
                kafka_object_config.get(client_id_key),
                facility,
                message);
        });

    /// NOTE: statistics should be consumed, otherwise it creates too much
    /// entries in the queue, that leads to memory leak and slow shutdown.
    if (!kafka_config.has_property("statistics.interval.ms"))
    {
        // every 3 seconds by default. set to 0 to disable.
        kafka_config.set("statistics.interval.ms", "3000");
    }
}


void GlutenKafkaUtils::loadLegacyTopicConfig(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const String & config_prefix)
{
    if (!collection_name.empty())
    {
        loadNamedCollectionConfig(kafka_config, collection_name, config_prefix);
        return;
    }

    Poco::Util::AbstractConfiguration::Keys tags;
    config.keys(config_prefix, tags);

    for (const auto & tag : tags)
        loadConfigProperty(kafka_config, config, config_prefix, tag);
}

void GlutenKafkaUtils::loadLegacyConfigSyntax(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & collection_name,
    const Names & topics)
{
    for (const auto & topic : topics)
    {
        const String kafka_topic_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_TAG + "_" + topic;
        loadLegacyTopicConfig(kafka_config, config, collection_name, kafka_topic_path);
    }
}


void GlutenKafkaUtils::loadConsumerConfig(cppkafka::Configuration & kafka_config, const KafkaConfigLoader::LoadConfigParams & params)
{
    const String consumer_path = KafkaConfigLoader::CONFIG_KAFKA_TAG + "." + KafkaConfigLoader::CONFIG_KAFKA_CONSUMER_TAG;
    loadLegacyConfigSyntax(kafka_config, params.config, params.collection_name, params.topics);
    // A new syntax has higher priority
    loadFromConfig(kafka_config, params, consumer_path);
}


cppkafka::Configuration GlutenKafkaUtils::getConsumerConfiguration(const KafkaConfigLoader::ConsumerConfigParams & params)
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", params.brokers);
    conf.set("group.id", params.group);
    if (params.multiple_consumers)
        conf.set("client.id", fmt::format("{}-{}", params.client_id, params.consumer_number));
    else
        conf.set("client.id", params.client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    conf.set("auto.offset.reset", "earliest"); // If no offset stored for this group, read all messages from the start

    // that allows to prevent fast draining of the librdkafka queue
    // during building of single insert block. Improves performance
    // significantly, but may lead to bigger memory consumption.
    size_t default_queued_min_messages = 100000; // must be greater than or equal to default
    size_t max_allowed_queued_min_messages = 10000000; // must be less than or equal to max allowed value
    conf.set(
        "queued.min.messages", std::min(std::max(params.max_block_size, default_queued_min_messages), max_allowed_queued_min_messages));

    updateGlobalConfiguration(conf, params);
    loadConsumerConfig(conf, params);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false"); // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false"); // Ignore EOF messages

    for (auto & property : conf.get_all())
        LOG_TRACE(params.log, "Consumer set property {}:{}", property.first, property.second);

    return conf;
}
}
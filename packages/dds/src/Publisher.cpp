/*
 * Copyright 2019 Proyectos y Sistemas de Mantenimiento SL (eProsima).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "Publisher.hpp"
#include "Conversion.hpp"

#include <fastrtps/attributes/PublisherAttributes.h>
#include <fastrtps/publisher/Publisher.h>
#include <fastrtps/Domain.h>

#include <iostream>

using namespace eprosima;

namespace soss {
namespace dds {

Publisher::Publisher(
        Participant* participant,
        const std::string& topic_name,
        const ::xtypes::DynamicType& message_type,
        const YAML::Node& config)
    : participant_{participant}
    , topic_name_{topic_name}
{
    DynamicTypeBuilder* builder = Conversion::create_builder(message_type);

    if (builder != nullptr)
    {
        participant->register_dynamic_type(topic_name, message_type.name(), builder);
    }
    else
    {
        throw DDSMiddlewareException("Cannot create builder for type " + message_type.name());
    }

    dynamic_data_ = participant->create_dynamic_data(topic_name);

    fastrtps::PublisherAttributes attributes;
    attributes.topic.topicKind = NO_KEY; //Check this
    attributes.topic.topicName = topic_name_;
    attributes.topic.topicDataType = message_type.name();

    if (config["service_instance_name"])
    {
        eprosima::fastrtps::rtps::Property instance_property;
        instance_property.name("dds.rpc.service_instance_name");
        instance_property.value(config["service_instance_name"].as<std::string>());
        attributes.properties.properties().push_back(instance_property);
    }

    dds_publisher_ = fastrtps::Domain::createPublisher(participant->get_dds_participant(), attributes, this);

    if (nullptr == dds_publisher_)
    {
        throw DDSMiddlewareException("Error creating a publisher");
    }
}

Publisher::~Publisher()
{
    std::unique_lock<std::mutex> lock(data_mtx_);
    participant_->delete_dynamic_data(dynamic_data_);
    dynamic_data_ = nullptr;
    fastrtps::Domain::removePublisher(dds_publisher_);
}

bool Publisher::publish(
        const ::xtypes::DynamicData& soss_message)
{
    bool success = false;

    std::cout << "[soss-dds][publisher]: translate message: soss -> dds "
        "(" << topic_name_ << ") " << std::endl;

    std::unique_lock<std::mutex> lock(data_mtx_);
    success = Conversion::soss_to_dds(soss_message, dynamic_data_);

    if (success)
    {
        success = dds_publisher_->write(dynamic_data_);
    }
    else
    {
        std::cerr << "Error converting message from dynamic types to soss message." << std::endl;
    }

    return success;
}

void Publisher::onPublicationMatched(
        fastrtps::Publisher* /* publisher */,
        fastrtps::rtps::MatchingInfo& info)
{
    std::string matching = fastrtps::rtps::MatchingStatus::MATCHED_MATCHING == info.status ? "matched" : "unmatched";
    std::cout << "[soss-dds][publisher]: " << matching <<
        " (" << topic_name_ << ") " << std::endl;
}

} // namespace dds
} // namespace soss

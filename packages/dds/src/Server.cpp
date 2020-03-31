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

#include "Server.hpp"
#include "Conversion.hpp"

#include <soss/Message.hpp>

#include <fastrtps/attributes/PublisherAttributes.h>
#include <fastrtps/attributes/SubscriberAttributes.h>
#include <fastrtps/publisher/Publisher.h>
#include <fastrtps/subscriber/Subscriber.h>
#include <fastrtps/subscriber/SampleInfo.h>
#include <fastrtps/Domain.h>

#include <functional>
#include <iostream>

using namespace eprosima;

namespace soss {
namespace dds {

Server::Server(
        Participant* participant,
        const std::string& service_name,
        const ::xtypes::DynamicType& request_type,
        const ::xtypes::DynamicType& reply_type,
        const YAML::Node& config)
    : service_name_{service_name}
    , request_type_{request_type}
    , reply_type_{reply_type}
{
    add_config(config);

    // Create DynamicData
    DynamicTypeBuilder* builder_request = Conversion::create_builder(request_type);
    DynamicTypeBuilder* builder_reply = Conversion::create_builder(reply_type);

    if (builder_request != nullptr)
    {
        participant->register_dynamic_type(service_name + "_Request", request_type.name(), builder_request);
    }
    else
    {
        throw DDSMiddlewareException("Cannot create builder for type " + request_type.name());
    }

    if (builder_reply != nullptr)
    {
        participant->register_dynamic_type(service_name + "_Reply", reply_type.name(), builder_reply);
    }
    else
    {
        throw DDSMiddlewareException("Cannot create builder for type " + reply_type.name());
    }

    request_dynamic_data_ = participant->create_dynamic_data(service_name + "_Request");
    reply_dynamic_data_ = participant->create_dynamic_data(service_name + "_Reply");

    // Create Subscriber
    {
        fastrtps::SubscriberAttributes attributes;
        attributes.topic.topicKind = NO_KEY;
        attributes.topic.topicName = service_name + "_Request";
        attributes.topic.topicDataType = request_type.name();

        dds_subscriber_ = fastrtps::Domain::createSubscriber(participant->get_dds_participant(), attributes, this);

        if (nullptr == dds_subscriber_)
        {
            throw DDSMiddlewareException("Error creating a subscriber");
        }
    }

    // Create Publisher
    {
        fastrtps::PublisherAttributes attributes;
        attributes.topic.topicKind = NO_KEY; //Check this
        attributes.topic.topicName = service_name_ + "_Reply";
        attributes.topic.topicDataType = reply_type.name();

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
}

Server::~Server()
{
    std::cout << "[soss-dds][server]: waiting current processing messages..." << std::endl;

    std::cout << "[soss-dds][server]: wait finished." << std::endl;

    fastrtps::Domain::removeSubscriber(dds_subscriber_);
    fastrtps::Domain::removePublisher(dds_publisher_);
}

bool Server::add_config(
        const YAML::Node& config)
{
    // Map discriminator to type from config
    if (config["remap"])
    {
        if (config["remap"]["dds"]) // Or name...
        {
            if (config["remap"]["dds"]["type"])
            {
                std::string disc = config["remap"]["dds"]["type"].as<std::string>();
                const ::xtypes::DynamicType& member_type = Conversion::resolve_discriminator_type(request_type_, disc);
                type_to_discriminator_[member_type.name()] = disc;
                std::cout << "[soss-dds] server: member \"" << disc << "\" has type \""
                          << member_type.name() << "\"." << std::endl;
            }
            else
            {
                if (config["remap"]["dds"]["request_type"])
                {
                    std::string disc = config["remap"]["dds"]["request_type"].as<std::string>();
                    const ::xtypes::DynamicType& member_type = Conversion::resolve_discriminator_type(request_type_, disc);
                    type_to_discriminator_[member_type.name()] = disc;
                    std::cout << "[soss-dds] server: member \"" << disc << "\" has request type \""
                              << member_type.name() << "\"." << std::endl;
                }
                if (config["remap"]["dds"]["reply_type"])
                {
                    std::string disc = config["remap"]["dds"]["reply_type"].as<std::string>();
                    const ::xtypes::DynamicType& member_type = Conversion::resolve_discriminator_type(reply_type_, disc);
                    type_to_discriminator_[member_type.name()] = disc;
                    std::cout << "[soss-dds] server: member \"" << disc << "\" has reply type \""
                              << member_type.name() << "\"." << std::endl;
                }
            }
        }
    }
    return true;
}

void Server::call_service(
        const ::xtypes::DynamicData& soss_request,
        ServiceClient& client,
        std::shared_ptr<void> call_handle)
{
    bool success = false;
    ::xtypes::DynamicData request(request_type_);

    Conversion::access_member_data(request, type_to_discriminator_[soss_request.type().name()]) = soss_request;

    std::cout << "[soss-dds][server]: translate request: soss -> dds "
        "(" << service_name_ << ") " << std::endl;

    success = Conversion::soss_to_dds(request, static_cast<DynamicData*>(request_dynamic_data_.get()));

    if (success)
    {
        callhandle_client_[call_handle] = &client;
        fastrtps::rtps::WriteParams params;
        success = dds_publisher_->write(request_dynamic_data_.get(), params);
        fastrtps::rtps::SampleIdentity sample = params.sample_identity();
        sample_callhandle_[sample] = call_handle;
    }
    else
    {
        std::cerr << "Error converting message from soss message to dynamic types." << std::endl;
    }
}

void Server::onPublicationMatched(
        fastrtps::Publisher* /* publisher */,
        fastrtps::rtps::MatchingInfo& info)
{
    std::string matching = fastrtps::rtps::MatchingStatus::MATCHED_MATCHING == info.status ? "matched" : "unmatched";
    std::cout << "[soss-dds][server]: " << matching <<
        " (" << service_name_ << ") " << std::endl;
}

void Server::onSubscriptionMatched(
        fastrtps::Subscriber* /* sub */,
        fastrtps::rtps::MatchingInfo& info)
{
    std::string matching = fastrtps::rtps::MatchingStatus::MATCHED_MATCHING == info.status ? "matched" : "unmatched";
    std::cout << "[soss-dds][server]: " << matching <<
        " (" << service_name_ << ") " << std::endl;
}

void Server::onNewDataMessage(
        fastrtps::Subscriber* /* sub */)
{
    using namespace std::placeholders;
    fastrtps::SampleInfo_t info;
    if (dds_subscriber_->takeNextData(request_dynamic_data_.get(), &info))
    {
        if (ALIVE == info.sampleKind)
        {
            fastrtps::rtps::SampleIdentity sample_id = info.related_sample_identity; // TODO Verify
            std::shared_ptr<void> call_handle = sample_callhandle_[sample_id];

            ::xtypes::DynamicData received(request_type_);
            bool success = Conversion::dds_to_soss(static_cast<DynamicData*>(request_dynamic_data_.get()), received);

            if (success)
            {
                callhandle_client_[call_handle]->receive_response(
                    call_handle,
                    Conversion::access_member_data(received, type_to_discriminator_[request_dynamic_data_->get_name()]));

                callhandle_client_.erase(call_handle);
                sample_callhandle_.erase(sample_id);
            }
            else
            {
                std::cerr << "Error converting message from dynamic types to soss message." << std::endl;
            }
        }
    }
}

} // namespace dds
} // namespace soss

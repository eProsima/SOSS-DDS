/*
 * Copyright 2019 - present Proyectos y Sistemas de Mantenimiento SL (eProsima).
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

#include <is/core/Message.hpp>

#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/publisher/PublisherListener.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/subscriber/SubscriberListener.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#if FASTRTPS_VERSION_MAJOR >= 2 && FASTRTPS_VERSION_MINOR >= 2
#include <fastdds/dds/subscriber/InstanceState.hpp>
#endif //  if FASTRTPS_VERSION_MAJOR >= 2 && FASTRTPS_VERSION_MINOR >= 2

#include <functional>
#include <iostream>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

Server::Server(
        Participant* participant,
        const std::string& service_name,
        const xtypes::DynamicType& request_type,
        const xtypes::DynamicType& reply_type,
        const YAML::Node& config)
    : participant_(participant)
    , service_name_(service_name)
    , request_entities_(request_type)
    , reply_entities_(reply_type)
    , matched_mtx_()
    , pub_sub_matched_(0)
    , stop_cleaner_(false)
    , cleaner_thread_(&Server::cleaner_function, this)
    , logger_("is::sh::FastDDS::Server")
{
    mtx_.lock(); // Lock mtx_ until datareader and datawriter have matched

    add_config(config);

    // Create DynamicData
    DynamicTypeBuilder* builder_request = Conversion::create_builder(request_type);
    DynamicTypeBuilder* builder_reply = Conversion::create_builder(reply_type);

    if (builder_request != nullptr)
    {
        participant->register_dynamic_type(
            service_name + "_Request", request_type.name(), builder_request);
    }
    else
    {
        throw DDSMiddlewareException(
                  logger_, "Cannot create builder for type " + request_type.name());
    }

    if (builder_reply != nullptr)
    {
        participant->register_dynamic_type(
            service_name + "_Reply", reply_type.name(), builder_reply);
    }
    else
    {
        throw DDSMiddlewareException(
                  logger_, "Cannot create builder for type " + reply_type.name());
    }

    request_entities_.dynamic_data = participant->create_dynamic_data(service_name + "_Request");
    reply_entities_.dynamic_data = participant->create_dynamic_data(service_name + "_Reply");

    // Retrieve DDS participant
    ::fastdds::dds::DomainParticipant* dds_participant = participant->get_dds_participant();
    if (!dds_participant)
    {
        throw DDSMiddlewareException(
                  logger_, "Trying to create a server without a DDS participant!");
    }

    // Create request entities
    {
        // Create DDS publisher
        request_entities_.dds_publisher = dds_participant->create_publisher(
            ::fastdds::dds::PUBLISHER_QOS_DEFAULT);
        if (request_entities_.dds_publisher)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS publisher for service '"
                    << service_name << "' request" << std::endl;
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS publisher for service '"
                << service_name << "' request was not created";

            throw DDSMiddlewareException(logger_, err.str());
        }

        // Create DDS topic
        auto topic_description = dds_participant->lookup_topicdescription(service_name + "_Request");
        if (!topic_description)
        {
            request_entities_.dds_topic = dds_participant->create_topic(
                service_name + "_Request", request_type.name(), ::fastdds::dds::TOPIC_QOS_DEFAULT);
            if (request_entities_.dds_topic)
            {
                logger_ << utils::Logger::Level::DEBUG
                        << "Created Fast DDS topic '" << service_name << "_Request' with type '"
                        << request_type.name() << "'" << std::endl;
            }
            else
            {
                std::ostringstream err;
                err << "Fast DDS topic '" << service_name << "_Request' with type '"
                    << request_type.name() << "' was not created";

                throw DDSMiddlewareException(logger_, err.str());
            }
        }
        else
        {
            request_entities_.dds_topic = static_cast<::fastdds::dds::Topic*>(topic_description);
        }

        // Create DDS datawriter
        ::fastdds::dds::DataWriterQos datawriter_qos = ::fastdds::dds::DATAWRITER_QOS_DEFAULT;

        if (config["service_instance_name"])
        {
            fastrtps::rtps::Property instance_property;
            instance_property.name("dds.rpc.service_instance_name");
            instance_property.value(config["service_instance_name"].as<std::string>());
            datawriter_qos.properties().properties().emplace_back(std::move(instance_property));
        }

        request_entities_.dds_datawriter = request_entities_.dds_publisher->create_datawriter(
            request_entities_.dds_topic, datawriter_qos, this);

        if (request_entities_.dds_datawriter)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS datawriter for service '" << service_name
                    << "' request" << std::endl;

            participant_->associate_topic_to_dds_entity(
                request_entities_.dds_topic, request_entities_.dds_datawriter);
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS datawriter for service '" << service_name << "' request was not created";

            throw(DDSMiddlewareException(logger_, err.str()));
        }
    }

    // Create reply entities
    {
        // Create DDS subscriber
        reply_entities_.dds_subscriber = dds_participant->create_subscriber(
            ::fastdds::dds::SUBSCRIBER_QOS_DEFAULT);
        if (reply_entities_.dds_subscriber)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS subscriber for service '"
                    << service_name << "' reply" << std::endl;
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS subscriber for service '"
                << service_name << "' reply was not created";

            throw DDSMiddlewareException(logger_, err.str());
        }

        // Create DDS topic
        auto topic_description = dds_participant->lookup_topicdescription(service_name + "_Reply");
        if (!topic_description)
        {
            reply_entities_.dds_topic = dds_participant->create_topic(
                service_name + "_Reply", reply_type.name(), ::fastdds::dds::TOPIC_QOS_DEFAULT);
            if (reply_entities_.dds_topic)
            {
                logger_ << utils::Logger::Level::DEBUG
                        << "Created Fast DDS topic '" << service_name << "_Reply' with type '"
                        << reply_type.name() << "'" << std::endl;
            }
            else
            {
                std::ostringstream err;
                err << "Fast DDS topic '" << service_name << "_Reply' with type '"
                    << reply_type.name() << "' was not created";

                throw DDSMiddlewareException(logger_, err.str());
            }
        }
        else
        {
            reply_entities_.dds_topic = static_cast<::fastdds::dds::Topic*>(topic_description);
        }

        // Create DDS datareader
        ::fastdds::dds::DataReaderQos datareader_qos = ::fastdds::dds::DATAREADER_QOS_DEFAULT;
        ::fastdds::dds::ReliabilityQosPolicy rel_policy;
        rel_policy.kind = ::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        datareader_qos.reliability(rel_policy);

        if (config["service_instance_name"])
        {
            fastrtps::rtps::Property instance_property;
            instance_property.name("dds.rpc.service_instance_name");
            instance_property.value(config["service_instance_name"].as<std::string>());
            datareader_qos.properties().properties().emplace_back(std::move(instance_property));
        }

        reply_entities_.dds_datareader = reply_entities_.dds_subscriber->create_datareader(
            reply_entities_.dds_topic, datareader_qos, this);

        if (reply_entities_.dds_datareader)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS datareader for service '" << service_name
                    << "' reply" << std::endl;

            participant_->associate_topic_to_dds_entity(
                reply_entities_.dds_topic, reply_entities_.dds_datareader);
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS datareader for service '" << service_name << "'reply was not created";

            throw (DDSMiddlewareException(logger_, err.str()));
        }
    }
}

Server::~Server()
{
    logger_ << utils::Logger::Level::INFO
            << "Waiting for current processing messages before quitting" << std::endl;

    {
        std::unique_lock<std::mutex> lock(cleaner_mtx_);
        stop_cleaner_ = true;
        cleaner_cv_.notify_one();
    }

    if (cleaner_thread_.joinable())
    {
        cleaner_thread_.join();
    }

    logger_ << utils::Logger::Level::INFO
            << "All messages were processed. Quitting now..." << std::endl;

    ::fastdds::dds::DomainParticipant* dds_participant = participant_->get_dds_participant();

    {
        std::unique_lock<std::mutex> request_lock(request_entities_.data_mtx);
        participant_->delete_dynamic_data(request_entities_.dynamic_data);

        bool delete_topic = participant_->dissociate_topic_from_dds_entity(
            request_entities_.dds_topic, request_entities_.dds_datawriter);

        request_entities_.dds_publisher->delete_datawriter(request_entities_.dds_datawriter);
        dds_participant->delete_publisher(request_entities_.dds_publisher);

        if (delete_topic)
        {
            dds_participant->delete_topic(request_entities_.dds_topic);
        }
    }

    {
        std::unique_lock<std::mutex> reply_lock(reply_entities_.data_mtx);
        participant_->delete_dynamic_data(reply_entities_.dynamic_data);

        bool delete_topic = participant_->dissociate_topic_from_dds_entity(
            reply_entities_.dds_topic, reply_entities_.dds_datareader);

        reply_entities_.dds_subscriber->delete_datareader(reply_entities_.dds_datareader);
        dds_participant->delete_subscriber(reply_entities_.dds_subscriber);

        if (delete_topic)
        {
            dds_participant->delete_topic(reply_entities_.dds_topic);
        }
    }
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
                std::string req;
                std::string disc = config["remap"]["dds"]["type"].as<std::string>();
                const ::xtypes::DynamicType& member_type =
                        Conversion::resolve_discriminator_type(request_entities_.type, disc);

                if (member_type.name().find("::") == 0)
                {
                    req = member_type.name().substr(2);
                }
                else
                {
                    req = member_type.name();
                }

                type_to_discriminator_[req] = disc;
                if (config["type"])
                {
                    if (config["type"].as<std::string>() != req)
                    {
                        // Add alias from other types
                        // TODO - Solve path?
                        type_to_discriminator_[config["type"].as<std::string>()] = disc;
                    }
                }

                logger_ << utils::Logger::Level::DEBUG
                        << "Member '" << disc << "' has type '"
                        << req << "'" << std::endl;
            }
            else
            {
                std::string req;
                std::string req_alias;
                if (config["remap"]["dds"]["request_type"])
                {
                    std::string disc = config["remap"]["dds"]["request_type"].as<std::string>();
                    const ::xtypes::DynamicType& member_type =
                            Conversion::resolve_discriminator_type(request_entities_.type, disc);

                    if (member_type.name().find("::") == 0)
                    {
                        req = member_type.name().substr(2);
                    }
                    else
                    {
                        req = member_type.name();
                    }

                    type_to_discriminator_[req] = disc;
                    if (config["request_type"])
                    {
                        if (config["request_type"].as<std::string>() != req)
                        {
                            // Add alias from other types
                            // TODO - Solve path?
                            req_alias = config["request_type"].as<std::string>();
                            type_to_discriminator_[req_alias] = disc;
                        }
                    }

                    logger_ << utils::Logger::Level::DEBUG
                            << "Member '" << disc << "' has request type '"
                            << req << "'" << std::endl;
                }

                if (config["remap"]["dds"]["reply_type"])
                {
                    std::string rep;
                    std::string disc = config["remap"]["dds"]["reply_type"].as<std::string>();
                    const ::xtypes::DynamicType& member_type =
                            Conversion::resolve_discriminator_type(reply_entities_.type, disc);

                    if (member_type.name().find("::") == 0)
                    {
                        rep = member_type.name().substr(2);
                    }
                    else
                    {
                        rep = member_type.name();
                    }

                    request_reply_[req] = rep;
                    type_to_discriminator_[rep] = disc;
                    if (config["reply_type"])
                    {
                        if (config["reply_type"].as<std::string>() != req)
                        {
                            // Add alias from other types
                            // TODO - Solve path?
                            type_to_discriminator_[config["reply_type"].as<std::string>()] = disc;
                            request_reply_[req_alias] = rep;
                        }
                    }

                    logger_ << utils::Logger::Level::DEBUG
                            << "Member '" << disc << "' has reply type '"
                            << req << "'" << std::endl;
                }
            }
        }
    }
    return true;
}

void Server::call_service(
        const ::xtypes::DynamicData& is_request,
        ServiceClient& client,
        std::shared_ptr<void> call_handle)
{
    ::xtypes::DynamicData request(request_entities_.type);

    if (is_request.type().name().find("::") == 0)
    {
        Conversion::access_member_data(request,
                type_to_discriminator_[is_request.type().name().substr(2)]) = is_request;
    }
    else
    {
        Conversion::access_member_data(request,
                type_to_discriminator_[is_request.type().name()]) = is_request;
    }

    logger_ << utils::Logger::Level::INFO
            << "Translating request from Integration Service to DDS for service request topic '"
            << service_name_ << "_Request': [[ " << is_request << " ]]" << std::endl;

    request_entities_.data_mtx.lock();
    bool success = Conversion::xtypes_to_fastdds(request, request_entities_.dynamic_data);

    if (success)
    {
        fastrtps::rtps::WriteParams params;
        std::unique_lock<std::mutex> lock(mtx_);
        callhandle_client_[call_handle] = &client;

        success = request_entities_.dds_datawriter->write(
            request_entities_.dynamic_data, params);
        request_entities_.data_mtx.unlock();

        if (!success)
        {
            logger_ << utils::Logger::Level::WARN
                    << "Failed to publish to DDS service request topic '"
                    << service_name_ << "_Request'" << std::endl;
        }
        else
        {
            fastrtps::rtps::SampleIdentity sample = params.sample_identity();
            sample_callhandle_[sample] = call_handle;
            if (request_reply_.count(is_request.type().name()) > 0)
            {
                reply_id_type_[sample] = request_reply_[is_request.type().name()];
            }
        }
    }
    else
    {
        request_entities_.data_mtx.unlock();

        logger_ << utils::Logger::Level::ERROR
                << "Failed to convert request from Integration Service to DDS for "
                << "service request topic '" << service_name_ << "_Request'" << std::endl;
    }
}

void Server::on_publication_matched(
        ::fastdds::dds::DataWriter* /*writer*/,
        const ::fastdds::dds::PublicationMatchedStatus& info)
{
    std::unique_lock<std::mutex> lock(matched_mtx_);
    if (1 == info.current_count_change)
    {
        pub_sub_matched_++;
        if (2 == pub_sub_matched_)
        {
            mtx_.unlock();
        }

        logger_ << utils::Logger::Level::INFO
                << "Publisher for topic '" << service_name_ << "_Request' matched" << std::endl;
    }
    else if (-1 == info.current_count_change)
    {
        pub_sub_matched_--;
        logger_ << utils::Logger::Level::INFO
                << "Publisher for topic '" << service_name_ << "_Request' unmatched" << std::endl;
    }
}

void Server::on_subscription_matched(
        ::fastdds::dds::DataReader* /*reader*/,
        const ::fastdds::dds::SubscriptionMatchedStatus& info)
{
    std::unique_lock<std::mutex> lock(matched_mtx_);
    if (1 == info.current_count_change)
    {
        pub_sub_matched_++;
        if (2 == pub_sub_matched_)
        {
            mtx_.unlock();
        }

        logger_ << utils::Logger::Level::INFO
                << "Subscriber for topic '" << service_name_ << "_Reply' matched" << std::endl;
    }
    else if (-1 == info.current_count_change)
    {
        pub_sub_matched_--;
        logger_ << utils::Logger::Level::INFO
                << "Subscriber for topic '" << service_name_ << "_Reply' unmatched" << std::endl;
    }
}

void Server::on_data_available(
        ::fastdds::dds::DataReader* /*reader*/)
{
    using namespace std::placeholders;

    ::fastdds::dds::SampleInfo info;

    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    reply_entities_.data_mtx.lock();

    if (!stop_cleaner_ && fastrtps::types::ReturnCode_t::RETCODE_OK
            == reply_entities_.dds_datareader->take_next_sample(reply_entities_.dynamic_data, &info))
    {
#if FASTRTPS_VERSION_MAJOR < 2 || FASTRTPS_VERSION_MINOR < 2
        if (::fastdds::dds::InstanceStateKind::ALIVE == info.instance_state)
#else
        if (::fastdds::dds::InstanceStateKind::ALIVE_INSTANCE_STATE == info.instance_state)
#endif //  if FASTRTPS_VERSION_MAJOR < 2 || FASTRTPS_VERSION_MINOR < 2
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Process incoming data available for service reply topic '"
                    << service_name_ << "_Reply'" << std::endl;

            std::thread* thread = new std::thread(&Server::receive, this, info.related_sample_identity);
            reception_threads_.emplace(thread->get_id(), thread);
        }
        else
        {
            reply_entities_.data_mtx.unlock();
        }
    }
    else
    {
        reply_entities_.data_mtx.unlock();
    }
}

void Server::receive(
        fastrtps::rtps::SampleIdentity sample_id)
{
    std::shared_ptr<void> call_handle;

    {
        std::unique_lock<std::mutex> lock(mtx_);
        if (sample_callhandle_.count(sample_id) > 0)
        {
            call_handle = sample_callhandle_[sample_id];
        }
        else
        {
            logger_ << utils::Logger::Level::WARN
                    << "Received reply from unasked request. Ignoring..." << std::endl;
            reply_entities_.data_mtx.unlock();
            return;
        }
    }

    ::xtypes::DynamicData received(reply_entities_.type);

    logger_ << utils::Logger::Level::INFO
            << "Receiving reply from DDS for service reply topic '"
            << service_name_ << "_Reply'" << std::endl;

    bool success = Conversion::fastdds_to_xtypes(reply_entities_.dynamic_data, received);
    reply_entities_.data_mtx.unlock();

    if (success)
    {
        std::unique_lock<std::mutex> lock(mtx_);
        std::string path = reply_entities_.type.name();
        if (reply_id_type_.count(sample_id) > 0)
        {
            path = type_to_discriminator_[reply_id_type_[sample_id]];
            reply_id_type_.erase(sample_id);
        }

        ::xtypes::WritableDynamicDataRef ref = Conversion::access_member_data(received, path);
        ::xtypes::DynamicData message(ref, ref.type());

        if (callhandle_client_.count(call_handle) > 0)
        {
            auto client = callhandle_client_.at(call_handle);
            callhandle_client_.erase(call_handle);
            sample_callhandle_.erase(sample_id);

            client->receive_response(
                call_handle,
                message);
        }
        else
        {
            logger_ << utils::Logger::Level::WARN
                    << "Received reply from unasked request. Ignoring..." << std::endl;
        }
    }
    else
    {
        logger_ << utils::Logger::Level::ERROR
                << "Failed to convert message from DDS to Integration Service "
                << "for service reply topic '" << service_name_ << "_Reply'" << std::endl;
    }

    // Notify that we have ended
    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    finished_threads_.push_back(std::this_thread::get_id());
    cleaner_cv_.notify_one();
}

void Server::cleaner_function()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    std::vector<std::thread::id> stopped;

    while (!stop_cleaner_)
    {
        cleaner_cv_.wait(
            lock,
            [this]()
            {
                return stop_cleaner_ || !finished_threads_.empty();
            });

        for (std::thread::id id : finished_threads_)
        {
            // Some threads may end too quickly, wait until the next iteration
            if (reception_threads_.count(id) > 0)
            {
                std::thread* thread = reception_threads_.at(id);
                reception_threads_.erase(id);

                if (thread->joinable())
                {
                    thread->join();
                }
                delete thread;
                stopped.push_back(id);
            }
        }

        for (std::thread::id id : stopped)
        {
            auto it = std::find(finished_threads_.begin(), finished_threads_.end(), id);
            finished_threads_.erase(it);
        }
        stopped.clear();

        // Free the CPU just a moment
        lock.unlock();
        std::this_thread::sleep_for(10ms);
        lock.lock();
    }

    // Wait for the rest of threads
    for (auto& pair : reception_threads_)
    {
        std::thread* thread = pair.second;
        if (thread->joinable())
        {
            thread->join();
        }
        delete thread;
    }
}

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

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

#include "Client.hpp"
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

#include <iostream>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

Client::Client(
        Participant* participant,
        const std::string& service_name,
        const xtypes::DynamicType& request_type,
        const xtypes::DynamicType& reply_type,
        ServiceClientSystem::RequestCallback* callback,
        const YAML::Node& config)
    : participant_(participant)
    , service_name_(service_name)
    , request_entities_(request_type)
    , reply_entities_(reply_type)
    , matched_mtx_()
    , pub_sub_matched_(0)
    , stop_cleaner_{false}
    , cleaner_thread_{&Client::cleaner_function, this}
    , logger_("is::sh::FastDDS::Client")
{
    mtx_.lock(); // Lock mtx_ until datareader and datawriter have matched

    add_config(config, callback);

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
        participant->register_dynamic_type(service_name + "_Reply", reply_type.name(), builder_reply);
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
        // Create DDS subscriber
        request_entities_.dds_subscriber = dds_participant->create_subscriber(
            ::fastdds::dds::SUBSCRIBER_QOS_DEFAULT);
        if (request_entities_.dds_subscriber)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS subscriber for service '"
                    << service_name << "' request" << std::endl;
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS subscriber for service '"
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

        // Create DDS datareader
        ::fastdds::dds::DataReaderQos datareader_qos = ::fastdds::dds::DATAREADER_QOS_DEFAULT;
        ::fastdds::dds::ReliabilityQosPolicy rel_policy;
        rel_policy.kind = ::fastdds::dds::RELIABLE_RELIABILITY_QOS;
        datareader_qos.reliability(rel_policy);

        request_entities_.dds_datareader = request_entities_.dds_subscriber->create_datareader(
            request_entities_.dds_topic, datareader_qos, this);

        if (request_entities_.dds_datareader)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS datareader for service '" << service_name
                    << "' request" << std::endl;

            participant_->associate_topic_to_dds_entity(
                request_entities_.dds_topic, request_entities_.dds_datareader);
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS datareader for service '" << service_name << "'request was not created";

            throw (DDSMiddlewareException(logger_, err.str()));
        }
    }

    // Create reply entities
    {
        // Create DDS publisher
        reply_entities_.dds_publisher = dds_participant->create_publisher(
            ::fastdds::dds::PUBLISHER_QOS_DEFAULT);
        if (reply_entities_.dds_publisher)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS publisher for service '"
                    << service_name << "' reply" << std::endl;
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS publisher for service '"
                << service_name << "' reply was not created";

            throw DDSMiddlewareException(logger_, err.str());
        }

        // Create DDS topic
        // reply_entities_.dds_topic = participant_->find_topic(service_name + "_Reply");
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

                // participant_->register_topic(service_name + "_Reply", reply_entities_.dds_topic);
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

        // Create DDS datawriter
        ::fastdds::dds::DataWriterQos datawriter_qos = ::fastdds::dds::DATAWRITER_QOS_DEFAULT;

        if (config["service_instance_name"])
        {
            fastrtps::rtps::Property instance_property;
            instance_property.name("dds.rpc.service_instance_name");
            instance_property.value(config["service_instance_name"].as<std::string>());
            datawriter_qos.properties().properties().emplace_back(std::move(instance_property));
        }

        reply_entities_.dds_datawriter = reply_entities_.dds_publisher->create_datawriter(
            reply_entities_.dds_topic, datawriter_qos, this);

        if (reply_entities_.dds_datawriter)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS datawriter for service '" << service_name
                    << "' reply" << std::endl;

            participant_->associate_topic_to_dds_entity(
                reply_entities_.dds_topic, reply_entities_.dds_datawriter);
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS datawriter for service '" << service_name << "' reply was not created";

            throw(DDSMiddlewareException(logger_, err.str()));
        }
    }
}

Client::~Client()
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
            request_entities_.dds_topic, request_entities_.dds_datareader);

        request_entities_.dds_subscriber->delete_datareader(request_entities_.dds_datareader);
        dds_participant->delete_subscriber(request_entities_.dds_subscriber);

        if (delete_topic)
        {
            dds_participant->delete_topic(request_entities_.dds_topic);
        }
    }

    {
        std::unique_lock<std::mutex> reply_lock(reply_entities_.data_mtx);
        participant_->delete_dynamic_data(reply_entities_.dynamic_data);

        bool delete_topic = participant_->dissociate_topic_from_dds_entity(
            reply_entities_.dds_topic, reply_entities_.dds_datawriter);

        reply_entities_.dds_publisher->delete_datawriter(reply_entities_.dds_datawriter);
        dds_participant->delete_publisher(reply_entities_.dds_publisher);

        if (delete_topic)
        {
            dds_participant->delete_topic(reply_entities_.dds_topic);
        }
    }
}

void Client::add_member(
        const ::xtypes::DynamicType& type,
        const std::string& path)
{
    std::shared_ptr<NavigationNode> root;
    if (member_tree_.count(type.name()) > 0)
    {
        root = member_tree_[type.name()];
    }
    else
    {
        root = std::make_shared<NavigationNode>();
        member_tree_[type.name()] = root;
    }
    NavigationNode::fill_root_node(root, type, path);
}

bool Client::add_config(
        const YAML::Node& config,
        ServiceClientSystem::RequestCallback* callback)
{
    bool callback_set = false;
    // Map discriminator to type from config

    auto add_member_config = [&](
        const YAML::Node& request_config,
        std::string& req,
        bool is_request) -> bool
            {
                const std::string disc = request_config.as<std::string>();

                const ::xtypes::DynamicType& member_type = Conversion::resolve_discriminator_type(
                    is_request ? request_entities_.type : reply_entities_.type, disc);

                std::string type_name;
                if (member_type.name().find("::") == 0)
                {
                    type_name = member_type.name().substr(2);
                }
                else
                {
                    type_name = member_type.name();
                }

                type_to_discriminator_[type_name] = disc;
                member_types_.push_back(type_name);

                if (is_request)
                {
                    callbacks_[type_name] = callback;
                    callback_set = true;
                    req = type_name;
                }
                else
                {
                    if (req.empty())
                    {
                        logger_ << utils::Logger::Level::ERROR
                                << "Adding config for service '" << service_name_
                                << "': request type cannot be empty" << std::endl;
                        return false;
                    }
                    request_reply_[req] = type_name;
                }

                logger_ << utils::Logger::Level::DEBUG
                        << "Member '" << disc << "' has "
                        << (is_request ? "request" : "reply") << " type '"
                        << req << "'" << std::endl;

                add_member(is_request ? request_entities_.type : reply_entities_.type, disc);

                return true;
            };

    if (config["remap"])
    {
        if (config["remap"]["dds"]) // Or name...
        {
            if (config["remap"]["dds"]["type"])
            {
                std::string unused;
                add_member_config(config["remap"]["dds"]["type"], unused, true);
            }
            else
            {
                std::string req;
                if (config["remap"]["dds"]["request_type"])
                {
                    add_member_config(config["remap"]["dds"]["request_type"], req, true);
                }
                if (config["remap"]["dds"]["reply_type"])
                {
                    if (!add_member_config(config["remap"]["dds"]["reply_type"], req, false))
                    {
                        return false;
                    }
                }
            }
        }
    }

    if (!callback_set)
    {
        if (config["type"])
        {
            std::string unused;
            add_member_config(config["type"], unused, true);
        }
        else
        {
            std::string req;
            if (config["request_type"])
            {
                add_member_config(config["request_type"], req, true);
            }

            if (config["reply_type"])
            {
                if (!add_member_config(config["reply_type"], req, false))
                {
                    return false;
                }
            }
        }
    }

    return callback_set;
}

void Client::receive_response(
        std::shared_ptr<void> call_handle,
        const ::xtypes::DynamicData& response)
{
    fastrtps::rtps::WriteParams params;
    fastrtps::rtps::SampleIdentity sample_id =
            *static_cast<fastrtps::rtps::SampleIdentity*>(call_handle.get());
    params.related_sample_identity(sample_id);

    std::string path = reply_entities_.type.name();

    {
        std::unique_lock<std::mutex> lock(mtx_);
        if (reply_id_type_.count(sample_id) > 0)
        {
            path = type_to_discriminator_[reply_id_type_[sample_id]];
            reply_id_type_.erase(sample_id);
        }
    }

    ::xtypes::DynamicData reply(reply_entities_.type);

    Conversion::access_member_data(reply, path) = response;

    logger_ << utils::Logger::Level::INFO
            << "Translating reply from Integration Service to DDS for service reply topic '"
            << service_name_ << "_Reply': [[ " << response << " ]]" << std::endl;

    std::unique_lock<std::mutex> reply_lock(reply_entities_.data_mtx);
    bool success = Conversion::xtypes_to_fastdds(reply, reply_entities_.dynamic_data);

    if (success)
    {
        success = reply_entities_.dds_datawriter->write(reply_entities_.dynamic_data, params);

        if (!success)
        {
            logger_ << utils::Logger::Level::WARN
                    << "Failed to publish to DDS service reply topic '"
                    << service_name_ << "_Reply'" << std::endl;
        }
    }
    else
    {
        logger_ << utils::Logger::Level::ERROR
                << "Failed to convert reply from Integration Service to DDS for "
                << "service reply topic '" << service_name_ << "_Reply'" << std::endl;
    }
}

void Client::on_publication_matched(
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
                << "Publisher for topic '" << service_name_ << "_Reply' matched" << std::endl;
    }
    else if (-1 == info.current_count_change)
    {
        pub_sub_matched_--;
        logger_ << utils::Logger::Level::INFO
                << "Publisher for topic '" << service_name_ << "_Reply' unmatched" << std::endl;
    }
}

void Client::on_subscription_matched(
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
                << "Subscriber for topic '" << service_name_ << "_Request' matched" << std::endl;
    }
    else if (-1 == info.current_count_change)
    {
        pub_sub_matched_--;
        logger_ << utils::Logger::Level::INFO
                << "Subscriber for topic '" << service_name_ << "_Request' unmatched" << std::endl;
    }
}

void Client::on_data_available(
        ::fastdds::dds::DataReader* /*reader*/)
{
    using namespace std::placeholders;

    ::fastdds::dds::SampleInfo info;

    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    request_entities_.data_mtx.lock();

    if (!stop_cleaner_ && fastrtps::types::ReturnCode_t::RETCODE_OK
            == request_entities_.dds_datareader->take_next_sample(request_entities_.dynamic_data, &info))
    {
#if FASTRTPS_VERSION_MINOR < 2
        if (::fastdds::dds::InstanceStateKind::ALIVE == info.instance_state)
#else
        if (::fastdds::dds::InstanceStateKind::ALIVE_INSTANCE_STATE == info.instance_state)
#endif //  if FASTRTPS_VERSION_MINOR < 2
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Process incoming data available for service request topic '"
                    << service_name_ << "_Request'" << std::endl;

            std::thread* thread = new std::thread(&Client::receive, this, info.sample_identity);
            reception_threads_.emplace(thread->get_id(), thread);
        }
        else
        {
            request_entities_.data_mtx.unlock();
        }
    }
    else
    {
        request_entities_.data_mtx.unlock();
    }
}

void Client::receive(
        fastrtps::rtps::SampleIdentity sample_id)
{
    {
        ::xtypes::DynamicData received(request_entities_.type);

        logger_ << utils::Logger::Level::INFO
                << "Receiving request from DDS for service request topic '"
                << service_name_ << "_Request'" << std::endl;

        bool success = Conversion::fastdds_to_xtypes(request_entities_.dynamic_data, received);
        request_entities_.data_mtx.unlock();

        if (success)
        {
            std::shared_ptr<NavigationNode> member =
                    NavigationNode::get_discriminator(member_tree_, received, member_types_);

            {
                std::unique_lock<std::mutex> lock(mtx_);
                if (request_reply_.count(member->type_name) > 0)
                {
                    reply_id_type_[sample_id] = request_reply_[member->type_name];
                }
            }

            ::xtypes::WritableDynamicDataRef ref =
                    Conversion::access_member_data(received, member->get_path());
            ::xtypes::DynamicData message(ref, ref.type());

            if (callbacks_.count(message.type().name()))
            {
                (*callbacks_[message.type().name()])(
                    message,
                    *this, std::make_shared<fastrtps::rtps::SampleIdentity>(sample_id));
            }
        }
        else
        {
            logger_ << utils::Logger::Level::ERROR
                    << "Failed to convert message from DDS to Integration Service "
                    << "for service request topic '" << service_name_ << "_Request'" << std::endl;
        }
    }

    // Notify that we have ended
    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    finished_threads_.push_back(std::this_thread::get_id());
    cleaner_cv_.notify_one();
}

void Client::cleaner_function()
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

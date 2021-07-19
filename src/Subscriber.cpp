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

#include "Subscriber.hpp"
#include "Conversion.hpp"

#include <is/core/Message.hpp>

#include <fastdds/dds/subscriber/SubscriberListener.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#if FASTRTPS_VERSION_MAJOR >= 2 && FASTRTPS_VERSION_MINOR >= 2
#include <fastdds/dds/subscriber/InstanceState.hpp>
#endif //  if FASTRTPS_VERSION_MAJOR >= 2 && FASTRTPS_VERSION_MINOR >= 2

#include <functional>
#include <iostream>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

Subscriber::Subscriber(
        Participant* participant,
        const std::string& topic_name,
        const xtypes::DynamicType& message_type,
        TopicSubscriberSystem::SubscriptionCallback* is_callback)
    : participant_(participant)
    , dds_subscriber_(nullptr)
    , dynamic_data_(nullptr)
    , topic_name_(topic_name)
    , message_type_(message_type)
    , is_callback_(is_callback)
    , reception_threads_()
    , stop_cleaner_(false)
    , cleaner_thread_(&Subscriber::cleaner_function, this)
    , logger_("is::sh::FastDDS::Subscriber")
{
    DynamicTypeBuilder* builder = Conversion::create_builder(message_type);
    if (builder != nullptr)
    {
        participant->register_dynamic_type(topic_name, message_type.name(), builder);
    }
    else
    {
        throw DDSMiddlewareException(
                  logger_, "Cannot create builder for type " + message_type.name());
    }

    dynamic_data_ = participant->create_dynamic_data(topic_name);

    // Retrieve DDS participant
    ::fastdds::dds::DomainParticipant* dds_participant = participant->get_dds_participant();
    if (!dds_participant)
    {
        throw DDSMiddlewareException(
                  logger_, "Trying to create a subscriber without a DDS participant!");
    }

    // Create DDS subscriber with default subscriber QoS
    dds_subscriber_ = dds_participant->create_subscriber(::fastdds::dds::SUBSCRIBER_QOS_DEFAULT);

    if (dds_subscriber_)
    {
        logger_ << utils::Logger::Level::DEBUG
                << "Created Fast DDS subscriber for topic '" << topic_name << "'" << std::endl;
    }
    else
    {
        std::ostringstream err;
        err << "Fast DDS subscriber for topic '" << topic_name << "' was not created";

        throw DDSMiddlewareException(logger_, err.str());
    }

    // Create DDS topic
    auto topic_description = dds_participant->lookup_topicdescription(topic_name);
    if (!topic_description)
    {
        dds_topic_ = dds_participant->create_topic(
            topic_name, message_type.name(), ::fastdds::dds::TOPIC_QOS_DEFAULT);
        if (dds_topic_)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Created Fast DDS topic '" << topic_name << "' with type '"
                    << message_type.name() << "'" << std::endl;
        }
        else
        {
            std::ostringstream err;
            err << "Fast DDS topic '" << topic_name << "' with type '"
                << message_type.name() << "' was not created";

            throw DDSMiddlewareException(logger_, err.str());
        }
    }
    else
    {
        dds_topic_ = static_cast<::fastdds::dds::Topic*>(topic_description);
    }

    // Create DDS datareader
    ::fastdds::dds::DataReaderQos datareader_qos = ::fastdds::dds::DATAREADER_QOS_DEFAULT;
    ::fastdds::dds::ReliabilityQosPolicy rel_policy;
    rel_policy.kind = ::fastdds::dds::RELIABLE_RELIABILITY_QOS;
    datareader_qos.reliability(rel_policy);

    dds_datareader_ = dds_subscriber_->create_datareader(dds_topic_, datareader_qos, this);
    if (dds_datareader_)
    {
        logger_ << utils::Logger::Level::DEBUG
                << "Created Fast DDS datareader for topic '" << topic_name << "'" << std::endl;

        participant_->associate_topic_to_dds_entity(dds_topic_, dds_datareader_);
    }
    else
    {
        std::ostringstream err;
        err << "Fast DDS datareader for topic '" << topic_name << "' was not created";

        throw DDSMiddlewareException(logger_, err.str());
    }
}

Subscriber::~Subscriber()
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

    std::unique_lock<std::mutex> lock(data_mtx_);
    participant_->delete_dynamic_data(dynamic_data_);

    bool delete_topic = participant_->dissociate_topic_from_dds_entity(dds_topic_, dds_datareader_);

    dds_datareader_->set_listener(nullptr);
    dds_subscriber_->delete_datareader(dds_datareader_);
    participant_->get_dds_participant()->delete_subscriber(dds_subscriber_);

    if (delete_topic)
    {
        participant_->get_dds_participant()->delete_topic(dds_topic_);
    }
}

void Subscriber::receive(
        const fastrtps::types::DynamicData* dds_message,
        ::fastdds::dds::SampleInfo sample_info)
{
    logger_ << utils::Logger::Level::INFO
            << "Receiving message from DDS for topic '" << topic_name_ << "'" << std::endl;

    ::xtypes::DynamicData is_message(message_type_);
    bool success = Conversion::fastdds_to_xtypes(dds_message, is_message);
    data_mtx_.unlock();

    if (success)
    {
        logger_ << utils::Logger::Level::INFO
                << "Received message: [[ " << is_message << " ]]" << std::endl;

        (*is_callback_)(is_message, static_cast<void*>(&sample_info));
    }
    else
    {
        logger_ << utils::Logger::Level::ERROR
                << "Failed to convert message from DDS to Integration Service for topic '"
                << topic_name_ << "'" << std::endl;
    }

    // Notify that we have ended
    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    finished_threads_.push_back(std::this_thread::get_id());
    cleaner_cv_.notify_one();
}

void Subscriber::on_data_available(
        ::fastdds::dds::DataReader* /*reader*/)
{
    using namespace std::placeholders;

    ::fastdds::dds::SampleInfo info;
    std::unique_lock<std::mutex> lock(cleaner_mtx_);
    data_mtx_.lock();

    if (!stop_cleaner_ && fastrtps::types::ReturnCode_t::RETCODE_OK
            == dds_datareader_->take_next_sample(dynamic_data_, &info))
    {
#if FASTRTPS_VERSION_MAJOR < 2 || FASTRTPS_VERSION_MINOR < 2
        if (::fastdds::dds::InstanceStateKind::ALIVE == info.instance_state)
#else
        if (::fastdds::dds::InstanceStateKind::ALIVE_INSTANCE_STATE == info.instance_state)
#endif //  if FASTRTPS_VERSION_MAJOR < 2 || FASTRTPS_VERSION_MINOR < 2
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Processing incoming data available for topic '"
                    << topic_name_ << "'" << std::endl;

            std::thread* thread = new std::thread(&Subscriber::receive, this, dynamic_data_, info);
            reception_threads_.emplace(thread->get_id(), thread);
        }
        else
        {
            data_mtx_.unlock();
        }
    }
    else
    {
        data_mtx_.unlock();
    }
}

void Subscriber::on_subscription_matched(
        ::fastdds::dds::DataReader* /*reader*/,
        const ::fastdds::dds::SubscriptionMatchedStatus& info)
{
    if (1 == info.current_count_change)
    {
        logger_ << utils::Logger::Level::INFO
                << "Subscriber for topic '" << topic_name_ << "' matched" << std::endl;
    }
    else if (-1 == info.current_count_change)
    {
        logger_ << utils::Logger::Level::INFO
                << "Subscriber for topic '" << topic_name_ << "' unmatched" << std::endl;
    }
}

void Subscriber::cleaner_function()
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

} // namespace fastdds
} // namespace sh
} // namespace is
} // namespace eprosima

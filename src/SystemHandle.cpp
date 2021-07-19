/*
 * Copyright 2021 Proyectos y Sistemas de Mantenimiento SL (eProsima).
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

#include <is/systemhandle/SystemHandle.hpp>
#include <is/utils/Log.hpp>

#include "Participant.hpp"
#include "Publisher.hpp"
#include "Subscriber.hpp"
#include "Server.hpp"
#include "Client.hpp"
#include "Conversion.hpp"

#include <iostream>
#include <thread>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

/**
 * @class SystemHandle
 *        This class represents a full *Integration Service* system handle or plugin for the *DDS*
 *        middleware, using eProsima's implementation, <a href="https://fast-dds.docs.eprosima.com/en/latest/">
 *        Fast DDS</a>.
 *
 *        This class inherits from is::FullSystem, so to implement publisher, subscriber, and server/
 *        client operations for the *Integration Service*.
 *
 */
class SystemHandle : public virtual FullSystem
{
public:

    SystemHandle()
        : FullSystem()
        , logger_("is::sh::FastDDS")
    {
    }

    ~SystemHandle()
    {
    }

    bool configure(
            const core::RequiredTypes& /*types*/,
            const YAML::Node& configuration,
            TypeRegistry& /*type_registry*/) override
    {
        /*
         * The Fast-DDS sh doesn't define new types.
         * Needed types will be defined in the 'types' section of the YAML file, and hence,
         * already registered in the 'TypeRegistry' by the *Integration Service core*.
         */
        try
        {
            // TODO Add a warning that was here in Participant build_participant creation
            participant_ = std::make_unique<Participant>(configuration);
        }
        catch (DDSMiddlewareException& e)
        {
            logger_ << utils::Logger::Level::ERROR << "Participant creation failed." << std::endl;
            e.from_logger << utils::Logger::Level::ERROR << e.what() << std::endl;
            return false;
        }

        logger_ << utils::Logger::Level::INFO << "Configured!" << std::endl;

        return true;
    }

    bool okay() const override
    {
        return (nullptr != participant_->get_dds_participant());
    }

    bool spin_once() override
    {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
        return okay();
    }

    bool subscribe(
            const std::string& topic_name,
            const xtypes::DynamicType& message_type,
            SubscriptionCallback* callback,
            const YAML::Node& /* configuration */) override
    {
        try
        {
            auto subscriber = std::make_shared<Subscriber>(
                participant_.get(), topic_name, message_type, callback);

            subscribers_.emplace_back(std::move(subscriber));

            logger_ << utils::Logger::Level::INFO
                    << "Subscriber created for topic '" << topic_name << "', with type '"
                    << message_type.name() << "'" << std::endl;

            return true;
        }
        catch (DDSMiddlewareException& e)
        {
            e.from_logger << utils::Logger::Level::ERROR << e.what() << std::endl;
            return false;
        }
    }

    bool is_internal_message(
            void* filter_handle)
    {
        ::fastdds::dds::SampleInfo* sample_info = static_cast<::fastdds::dds::SampleInfo*>(filter_handle);

        auto sample_writer_guid = fastrtps::rtps::iHandle2GUID(sample_info->publication_handle);

        if (sample_writer_guid.guidPrefix == participant_->get_dds_participant()->guid().guidPrefix)
        {
            if (utils::Logger::Level::DEBUG == logger_.get_level())
            {
                for (const auto& publisher : publishers_)
                {
                    if (sample_writer_guid == fastrtps::rtps::iHandle2GUID(publisher->get_dds_instance_handle()))
                    {
                        logger_ << utils::Logger::Level::DEBUG
                                << "Received internal message from publisher '"
                                << publisher->topic_name() << "', ignoring it..." << std::endl;

                        break;
                    }
                }
            }
            // This is a message published FROM Integration Service. Discard it.
            return true;
        }

        return false;
    }

    std::shared_ptr<TopicPublisher> advertise(
            const std::string& topic_name,
            const xtypes::DynamicType& message_type,
            const YAML::Node& configuration) override
    {
        try
        {
            auto publisher = std::make_shared<Publisher>(
                participant_.get(), topic_name, message_type, configuration);
            publishers_.emplace_back(std::move(publisher));

            logger_ << utils::Logger::Level::INFO
                    << "Publisher created for topic '" << topic_name << "', with type '"
                    << message_type.name() << "'" << std::endl;

            return publishers_.back();
        }
        catch (DDSMiddlewareException& e)
        {
            e.from_logger << utils::Logger::Level::ERROR << e.what() << std::endl;
            return std::shared_ptr<TopicPublisher>();
        }
    }

    bool create_client_proxy(
            const std::string& service_name,
            const xtypes::DynamicType& type,
            RequestCallback* callback,
            const YAML::Node& configuration) override
    {
        return create_client_proxy(service_name, type, type, callback, configuration);
    }

    bool create_client_proxy(
            const std::string& service_name,
            const xtypes::DynamicType& request_type,
            const xtypes::DynamicType& reply_type,
            RequestCallback* callback,
            const YAML::Node& configuration) override
    {
        if (clients_.count(service_name) == 0)
        {
            try
            {
                auto client = std::make_shared<Client>(
                    participant_.get(),
                    service_name,
                    request_type,
                    reply_type,
                    callback,
                    configuration);

                clients_[service_name] = std::move(client);

                logger_ << utils::Logger::Level::INFO
                        << "Client created for service '" << service_name
                        << "', with request_type '" << request_type.name()
                        << "' and reply_type '" << reply_type.name() << "'" << std::endl;

                return true;

            }
            catch (DDSMiddlewareException& e)
            {
                e.from_logger << utils::Logger::Level::ERROR << e.what() << std::endl;
                return false;
            }
        }

        return clients_[service_name]->add_config(configuration, callback);
    }

    std::shared_ptr<ServiceProvider> create_service_proxy(
            const std::string& service_name,
            const xtypes::DynamicType& type,
            const YAML::Node& configuration) override
    {
        return create_service_proxy(service_name, type, type, configuration);
    }

    std::shared_ptr<ServiceProvider> create_service_proxy(
            const std::string& service_name,
            const xtypes::DynamicType& request_type,
            const xtypes::DynamicType& reply_type,
            const YAML::Node& configuration) override
    {
        if (servers_.count(service_name) == 0)
        {
            try
            {
                auto server = std::make_shared<Server>(
                    participant_.get(),
                    service_name,
                    request_type,
                    reply_type,
                    configuration);

                servers_[service_name] = std::move(server);

                logger_ << utils::Logger::Level::INFO
                        << "Server created for service '" << service_name
                        << "', with request_type '" << request_type.name()
                        << "' and reply_type '" << reply_type.name() << "'" << std::endl;

                return servers_[service_name];
            }
            catch (DDSMiddlewareException& e)
            {
                e.from_logger << utils::Logger::Level::ERROR << e.what() << std::endl;
                return nullptr;
            }
        }

        if (servers_[service_name]->add_config(configuration))
        {
            return servers_[service_name];
        }
        else
        {
            return nullptr;
        }
    }

private:

    std::unique_ptr<Participant> participant_;
    std::vector<std::shared_ptr<Publisher> > publishers_;
    std::vector<std::shared_ptr<Subscriber> > subscribers_;
    std::map<std::string, std::shared_ptr<Client> > clients_;
    std::map<std::string, std::shared_ptr<Server> > servers_;

    utils::Logger logger_;
};

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

// TODO aliases must come from CMAKE
IS_REGISTER_SYSTEM("fastdds", eprosima::is::sh::fastdds::SystemHandle)
IS_REGISTER_SYSTEM("databroker", eprosima::is::sh::fastdds::SystemHandle)

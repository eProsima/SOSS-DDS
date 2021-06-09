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

#include "Participant.hpp"
#include "DDSMiddlewareException.hpp"
#include "Conversion.hpp"
#include "utils.hpp"

#include <fastdds/rtps/transport/TCPv4TransportDescriptor.h>
#include <fastdds/rtps/transport/UDPv4TransportDescriptor.h>

#include <fastrtps/types/DynamicDataFactory.h>
#include <fastrtps/xmlparser/XMLProfileManager.h>

#include <sstream>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

Participant::Participant()
    : dds_participant_(nullptr)
    , logger_("is::sh::FastDDS::Participant")
{
    build_participant();
}

Participant::Participant(
        const YAML::Node& config)
    : dds_participant_(nullptr)
    , logger_("is::sh::FastDDS::Participant")
{
    build_participant(config);
}

Participant::~Participant()
{
    if (!dds_participant_->has_active_entities())
    {
        dds_participant_->set_listener(nullptr);

        if (fastrtps::types::ReturnCode_t::RETCODE_OK !=
                ::fastdds::dds::DomainParticipantFactory::get_instance()->delete_participant(dds_participant_))
        {
            logger_ << utils::Logger::Level::ERROR
                    << "Cannot delete Fast DDS participant yet: it has active entities" << std::endl;
        }
    }
}

void Participant::build_participant()
{
    // Call build participant with empty configuration
    build_participant(YAML::Node());
}

void Participant::build_participant(
        const YAML::Node& config)
{
    logger_ << utils::Logger::Level::DEBUG << "Creating new fastdds Participant" << std::endl;

    // Check if domain_id exists in config
    eprosima::fastdds::dds::DomainId_t domain_id(0);

    // Check if domain_id tag is under other tag
    if (config["domain_id"])
    {
        logger_ << utils::Logger::Level::DEBUG << "#! Get Domain from plain config" << std::endl;
        domain_id = config["domain_id"].as<uint32_t>();
    }
    else if (config["participant"] && config["participant"]["domain_id"])
    {
        logger_ << utils::Logger::Level::DEBUG << "#! Get Domain from participant" << std::endl;
        domain_id = config["participant"]["domain_id"].as<uint32_t>();
    }
    else if (config["databroker"] && config["databroker"]["domain_id"])
    {
        logger_ << utils::Logger::Level::DEBUG << "#! Get Domain from databroker" << std::endl;
        domain_id = config["databroker"]["domain_id"].as<uint32_t>();
    }

    logger_ << utils::Logger::Level::DEBUG << "Creating Participant QoS" << std::endl;

    ::fastdds::dds::DomainParticipantQos participant_qos;

    // Depending the SH type, use participant std qos or databroker qos
    if (config["participant"])
    {
        participant_qos = get_participant_qos(config["participant"]);
    }
    else if (config["databroker"])
    {
        participant_qos = get_databroker_qos(config["databroker"]);
    }

    dds_participant_ = ::fastdds::dds::DomainParticipantFactory::get_instance()->create_participant(
        domain_id,
        participant_qos);

    if (dds_participant_)
    {
        logger_ << utils::Logger::Level::INFO
                << "Created Fast DDS participant '" << participant_qos.name()
                << "' with Domain ID: " << domain_id << std::endl;
    }
    else
    {
        std::ostringstream err;
        err << "Error while creating Fast DDS participant '" << participant_qos.name()
            << "' with Domain ID: " << domain_id;

        throw DDSMiddlewareException(logger_, err.str());
    }
}

::fastdds::dds::DomainParticipant* Participant::get_dds_participant() const
{
    return dds_participant_;
}

void Participant::register_dynamic_type(
        const std::string& topic_name,
        const std::string& type_name,
        fastrtps::types::DynamicTypeBuilder* builder)
{
    auto topic_to_type_it = topic_to_type_.find(topic_name);
    if (topic_to_type_it != topic_to_type_.end())
    {
        return; // Already registered.
    }

    auto types_it = types_.find(type_name);
    if (types_.end() != types_it)
    {
        // Type known, add the entry in the map topic->type
        topic_to_type_.emplace(topic_name, type_name);

        logger_ << utils::Logger::Level::DEBUG
                << "Adding type '" << type_name << "' to topic '"
                << topic_name << "'" << std::endl;

        return;
    }

    fastrtps::types::DynamicType_ptr dtptr = builder->build();

    if (dtptr != nullptr)
    {
        auto pair = types_.emplace(type_name, fastrtps::types::DynamicPubSubType(dtptr));
        fastrtps::types::DynamicPubSubType& dynamic_type_support = pair.first->second;

        topic_to_type_.emplace(topic_name, type_name);

        // Check if already registered
        ::fastdds::dds::TypeSupport p_type = dds_participant_->find_type(type_name);

        if (nullptr == p_type)
        {
            dynamic_type_support.setName(type_name.c_str());

            /**
             * The following lines are added here so that a bug with UnionType in
             * Fast DDS Dynamic Types is bypassed. This is a workaround and SHOULD
             * be removed once this bug is solved.
             * Until that moment, the Fast DDS SystemHandle will not be compatible with
             * Fast DDS Dynamic Type Discovery mechanism.
             *
             * More information here: https://eprosima.easyredmine.com/issues/11349
             */
            // WORKAROUND START
            dynamic_type_support.auto_fill_type_information(false);
            dynamic_type_support.auto_fill_type_object(false);
            // WORKAROUND END

            // Register it within the DomainParticipant
            if (pair.second && !dds_participant_->register_type(dynamic_type_support))
            {
                std::ostringstream err;
                err << "Dynamic type '" << type_name << "' registration failed";

                throw DDSMiddlewareException(logger_, err.str());
            }
        }

        if (pair.second)
        {
            logger_ << utils::Logger::Level::DEBUG
                    << "Registered type '" << type_name << "' in topic '"
                    << topic_name << "'" << std::endl;

            Conversion::register_type(topic_name, &dynamic_type_support);
        }
        else
        {
            logger_ << utils::Logger::Level::WARN
                    << "Failed registering type '" << type_name << "' in topic '"
                    << topic_name << "'" << std::endl;
        }
    }
    else
    {
        std::ostringstream err;
        err << "Dynamic type '" << type_name << "' for topic '"
            << topic_name << "' was not correctly built";

        throw DDSMiddlewareException(logger_, err.str());
    }
}

fastrtps::types::DynamicData* Participant::create_dynamic_data(
        const std::string& topic_name) const
{
    auto topic_to_type_it = topic_to_type_.find(topic_name);
    if (topic_to_type_.end() == topic_to_type_it)
    {
        std::ostringstream err;
        err << "Creating dynamic data for topic '" << topic_name
            << "' failed because the topic was not registered";

        throw DDSMiddlewareException(logger_, err.str());
    }

    auto types_it = types_.find(topic_to_type_it->second);
    if (types_.end() == types_it)
    {
        std::ostringstream err;
        err << "Creating dynamic data: dynamic type '" << types_it->first << "' not defined";

        throw DDSMiddlewareException(logger_, err.str());
    }

    const fastrtps::types::DynamicType_ptr& dynamic_type_ = types_it->second.GetDynamicType();
    return fastrtps::types::DynamicDataFactory::get_instance()->create_data(dynamic_type_);
}

void Participant::delete_dynamic_data(
        fastrtps::types::DynamicData* data) const
{
    DynamicDataFactory::get_instance()->delete_data(data);
}

const fastrtps::types::DynamicType* Participant::get_dynamic_type(
        const std::string& name) const
{
    auto it = types_.find(name);
    if (it == types_.end())
    {
        return nullptr;
    }

    return static_cast<const DynamicType*>(it->second.GetDynamicType().get());
}

const std::string& Participant::get_topic_type(
        const std::string& topic) const
{
    return topic_to_type_.at(topic);
}

void Participant::associate_topic_to_dds_entity(
        ::fastdds::dds::Topic* topic,
        ::fastdds::dds::DomainEntity* entity)
{
    std::unique_lock<std::mutex> lock(topic_to_entities_mtx_);

    if (topic_to_entities_.end() == topic_to_entities_.find(topic))
    {
        std::set<::fastdds::dds::DomainEntity*> entities;
        entities.emplace(entity);

        topic_to_entities_[topic] = std::move(entities);
    }
    else
    {
        topic_to_entities_.at(topic).emplace(entity);
    }
}

bool Participant::dissociate_topic_from_dds_entity(
        ::fastdds::dds::Topic* topic,
        ::fastdds::dds::DomainEntity* entity)
{
    // std::unique_lock<std::mutex> lock(topic_to_entities_mtx_);

    if (1 == topic_to_entities_.at(topic).size())
    {
        // Only one entity remains in the map
        topic_to_entities_.erase(topic);
        return true;
    }
    else // Size should not be 0 at this point
    {
        topic_to_entities_.at(topic).erase(entity);
        return false;
    }
}

eprosima::fastdds::dds::DomainParticipantQos Participant::get_default_participant_qos()
{
    logger_ << utils::Logger::Level::DEBUG << "#! get_default_participant_qos" << std::endl;

    eprosima::fastdds::dds::DomainParticipantQos df_pqos;
    df_pqos.name("default_IS-FastDDS-SH_participant");

    // By default use UDPv4 due to communication failures between dockers sharing the network with the host
    // When it is solved in Fast-DDS delete the following lines and use the default builtin transport.
    df_pqos.transport().use_builtin_transports = false;
    auto udp_transport = std::make_shared<::fastdds::rtps::UDPv4TransportDescriptor>();
    df_pqos.transport().user_transports.push_back(udp_transport);

    return df_pqos;
}

eprosima::fastdds::dds::DomainParticipantQos Participant::get_participant_qos(
        const YAML::Node& config)
{
    logger_ << utils::Logger::Level::DEBUG << "#! get_participant_qos" << std::endl;

    // Load XML if set in config yaml
    if (config["file_path"])
    {
        const std::string file_path = config["file_path"].as<std::string>();
        if (fastrtps::xmlparser::XMLP_ret::XML_OK !=
            fastrtps::xmlparser::XMLProfileManager::loadXMLFile(file_path))
        {
            std::ostringstream err;
            err << "Loading provided XML file in 'file_path': " << file_path << " incorrect or unexisted file";
            throw DDSMiddlewareException(
                logger_, err.str());
        }
    }

    // Variable to return
    eprosima::fastdds::dds::DomainParticipantQos pqos = get_default_participant_qos();

    // Load XML if set in config yaml
    if (config["profile_name"])
    {

        const std::string profile_name = config["profile_name"].as<std::string>();
        if (eprosima::fastrtps::types::ReturnCode_t::RETCODE_OK !=
                eprosima::fastdds::dds::DomainParticipantFactory::get_instance()->get_participant_qos_from_profile(
                    profile_name,
                    pqos))
        {
            std::ostringstream err;
            err << "Failed to fetch Fast DDS participant qos from XML "
                << "for profile named '" << profile_name << "'";

            throw DDSMiddlewareException(logger_, err.str());
        }
    }

    return pqos;
}

eprosima::fastdds::dds::DomainParticipantQos Participant::get_databroker_qos(
        const YAML::Node& config)
{
    logger_ << utils::Logger::Level::DEBUG << "#! get_databroker_qos" << std::endl;

    // Call first std participant qos to reuse std flags for fastdds SH
    eprosima::fastdds::dds::DomainParticipantQos pqos = get_participant_qos(config);

    uint32_t server_id = 0;
    std::string listening_addresses = "";
    std::string connection_addresses = "";

    if (config["server_id"])
    {
        // Conversion to int is needed so it is not treated as a char
        server_id = config["server_id"].as<uint32_t>() % std::numeric_limits<uint8_t>::max();
    }
    else
    {
        logger_ << utils::Logger::Level::INFO
                << "Not Server ID set in configuration, use 0 as default" << std::endl;
    }

    if (config["listening_addresses"])
    {
        listening_addresses = config["listening_addresses"].as<std::string>();
    }
    else
    {
        logger_ << utils::Logger::Level::WARN
                << "Server has no listening address."
                << "It will not discover or connect to other servers."
                << std::endl;
    }

    if (config["connection_addresses"])
    {
        connection_addresses = config["connection_addresses"].as<std::string>();
    }
    else
    {
        logger_ << utils::Logger::Level::INFO
                << "Server has no connection addresses, it will not try to connect to remote servers"
                << std::endl;
    }

    logger_ << utils::Logger::Level::DEBUG
            << "Server id set by configuration to " << server_id << std::endl;
    logger_ << utils::Logger::Level::DEBUG
            << "Listening addresses set by configuration to " << listening_addresses << std::endl;
    logger_ << utils::Logger::Level::DEBUG
            << "Connection addresses set by configuration to " << connection_addresses << std::endl;

    pqos.name("DataBroker_IS-FastDDS-SH_participant");

    pqos.wire_protocol().builtin.discovery_config.leaseDuration = fastrtps::c_TimeInfinite;
    pqos.wire_protocol().builtin.discovery_config.leaseDuration_announcementperiod =
            fastrtps::Duration_t(2, 0);

    pqos.wire_protocol().builtin.discovery_config.discoveryProtocol =
        fastrtps::rtps::DiscoveryProtocol::SERVER;

    // Set guid manually depending on the id
    pqos.wire_protocol().prefix = guid_server(server_id);

    // Listening configuration
    for (auto address : split_locator(listening_addresses))
    {
        // Create TCPv4 transport
        std::shared_ptr<eprosima::fastdds::rtps::TCPv4TransportDescriptor> descriptor =
            std::make_shared<eprosima::fastdds::rtps::TCPv4TransportDescriptor>();

        descriptor->add_listener_port(std::get<1>(address));
        descriptor->set_WAN_address(std::get<0>(address));

        descriptor->sendBufferSize = 0;
        descriptor->receiveBufferSize = 0;

        pqos.transport().user_transports.push_back(descriptor);

        // Create Locator
        eprosima::fastrtps::rtps::Locator_t tcp_locator;
        tcp_locator.kind = LOCATOR_KIND_TCPv4;

        eprosima::fastrtps::rtps::IPLocator::setIPv4(tcp_locator, std::get<0>(address));
        eprosima::fastrtps::rtps::IPLocator::setWan(tcp_locator, std::get<0>(address));
        eprosima::fastrtps::rtps::IPLocator::setLogicalPort(tcp_locator, std::get<1>(address));
        eprosima::fastrtps::rtps::IPLocator::setPhysicalPort(tcp_locator, std::get<1>(address));

        pqos.wire_protocol().builtin.metatrafficUnicastLocatorList.push_back(tcp_locator);
    }

    // Add every connection address as server
    for (auto address : split_ds_locator(connection_addresses))
    {
        // Set Server guid manually
        eprosima::fastrtps::rtps::RemoteServerAttributes server_attr;
        server_attr.guidPrefix = guid_server(std::get<2>(address));

        // Discovery server locator configuration TCP
        eprosima::fastrtps::rtps::Locator_t tcp_locator;
        tcp_locator.kind = LOCATOR_KIND_TCPv4;
        eprosima::fastrtps::rtps::IPLocator::setIPv4(tcp_locator, std::get<0>(address));
        eprosima::fastrtps::rtps::IPLocator::setLogicalPort(tcp_locator, std::get<1>(address));
        eprosima::fastrtps::rtps::IPLocator::setPhysicalPort(tcp_locator, std::get<1>(address));
        server_attr.metatrafficUnicastLocatorList.push_back(tcp_locator);

        pqos.wire_protocol().builtin.discovery_config.m_DiscoveryServers.push_back(server_attr);
    }

    logger_ << utils::Logger::Level::DEBUG
            << "Databroker initialized with GUID " << pqos.wire_protocol().prefix << std::endl;

    return pqos;
}

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

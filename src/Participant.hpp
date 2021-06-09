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
#ifndef _IS_SH_FASTDDS__INTERNAL__PARTICIPANT_HPP_
#define _IS_SH_FASTDDS__INTERNAL__PARTICIPANT_HPP_

#include "DDSMiddlewareException.hpp"

#include <fastdds/dds/core/Entity.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/qos/DomainParticipantQos.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastrtps/types/DynamicType.h>

#include <is/utils/Log.hpp>

#include <yaml-cpp/yaml.h>

#include <map>

namespace fastdds = eprosima::fastdds;

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

/**
 * @class Participant
 *        This class represents a <a href="https://fast-dds.docs.eprosima.com/en/latest/fastdds/dds_layer/domain/domainParticipant/domainParticipant.html">
 *        FastDDS DomainParticipant</a> within the *Integration Service* framework.
 *
 * @details It includes a mapping of the topic names to their corresponding
 *          <a href="https://fast-dds.docs.eprosima.com/en/latest/fastdds/dynamic_types/dynamic_types.html">
 *          Dynamic Type</a> representation, and also mappings to identify each topic with its type.
 *
 *          This class inherits from
 *          <a href="https://fast-dds.docs.eprosima.com/en/latest/fastdds/dds_layer/domain/domainParticipantListener/domainParticipantListener.html">
 *          Fast DDS DomainParticipantListener</a> class to scan for state changes on the DDS
 *          participant created by this *Integration Service* is::SystemHandle.
 */
class Participant
{
public:

    /**
     * @brief Construct a new Participant, with default values.
     *
     * @throws DDSMiddlewareException If the *DomainParticipant* could not be created.
     */
    Participant();

    /**
     * @brief Construct a new Participant object with the user-provided parameters
     *        in the *YAML* configuration file.
     *
     * @param[in] config The configuration provided by the user.
     *            It must contain two keys in the *YAML* map:
     *
     *            - `file_path`: Specifies the path to the XML profile that will be used to configure the
     *              *DomainParticipant*. More information on how to write these XML profiles can be found
     *              <a href="https://fast-dds.docs.eprosima.com/en/latest/fastdds/xml_configuration/xml_configuration.html">
     *              here</a>.
     *
     *            - `profile_name`: Provide a name to search for within the profiles defined in the XML
     *              that corresponds to the configuration profile that we want this Participant
     *              to be configured with.
     *
     * @throws DDSMiddlewareException If the XML profile was incorrect and, thus, the
     *         *DomainParticipant* could not be created.
     */
    Participant(
            const YAML::Node& config);

    /**
     * @brief Destroy the Participant object.
     */
    virtual ~Participant();

    /**
     * @brief Construct a *Fast DDS DomainParticipant*, given its DDS domain ID.
     *
     * @param[in] domain_id The DDS domain ID for this participant.
     *
     * @throws DDSMiddlewareException If the *DomainParticipant* could not be created.
     */
    void build_participant(
            const YAML::Node& config);

    //! Build participant with empty configuration
    void build_participant();

    /**
     * @brief Get the associate *FastDDS DomainParticipant* attribute.
     *
     * @returns The DDS participant.
     */
    ::fastdds::dds::DomainParticipant* get_dds_participant() const;

    /**
     * @brief Register a *Dynamic Type* within the types map. Also, register the
     *        associated DDS topic.
     *
     * @param[in] topic_name The topic name to be associated to the *Dynamic Type*.
     *
     * @param[in] type_name The type name to be registered in the factory.
     *
     * @param[in] builder A class that represents a builder for the desired *Dynamic Type*.
     *
     * @throws DDSMiddlewareException If the type could not be registered.
     */
    void register_dynamic_type(
            const std::string& topic_name,
            const std::string& type_name,
            fastrtps::types::DynamicTypeBuilder* builder);

    /**
     * @brief Create an empty dynamic data object for the specified topic.
     *
     * @param[in] topic_name The topic name.
     *
     * @returns The empty DynamicData for the required topic.
     *
     * @throws DDSMiddlewareException if the topic was not found
     *         or the type was not registered previously.
     */
    fastrtps::types::DynamicData* create_dynamic_data(
            const std::string& topic_name) const;

    /**
     * @brief Delete a certain dynamic data from the *DomainParticipant* database.
     *
     * @param[in] data The dynamic data to be deleted.
     */
    void delete_dynamic_data(
            fastrtps::types::DynamicData* data) const;

    /**
     * @brief Get the dynamic type pointer associated to a certain key.
     *
     * @param[in] name The key to find within the types map.
     *
     * @returns The pointer to the dynamic type if found, or `nullptr` otherwise.
     */
    const fastrtps::types::DynamicType* get_dynamic_type(
            const std::string& name) const;

    /**
     * @brief Get the type name associated to a certain topic.
     *
     * @param[in] topic The topic whose type is wanted to be retrieved.
     *
     * @returns A const reference to the topic type's name.
     */
    const std::string& get_topic_type(
            const std::string& topic) const;

    /**
     * @brief Register a topic into the topics map.
     *
     * @note This method is a workaround until `fastdds::dds::DomainParticipant::find_topic` gets implemented.
     *
     * @param[in] topic The name of the topic to register.
     *
     * @param[in] entity A pointer to the entity to be registered.
     */

    void associate_topic_to_dds_entity(
            ::fastdds::dds::Topic* topic,
            ::fastdds::dds::DomainEntity* entity);

    /**
     * @brief Unregister a topic from the topics map.
     *
     * @note This method is a workaround until `fastdds::dds::DomainParticipant::find_topic` gets implemented.
     *
     * @param[in] topic The name of the topic to unregister.
     *
     * @param[in] entity A pointer to the entity to be unregistered.
     */

    bool dissociate_topic_from_dds_entity(
            ::fastdds::dds::Topic* topic,
            ::fastdds::dds::DomainEntity* entity);

protected:

    //! Get Participant QoS using config file
    eprosima::fastdds::dds::DomainParticipantQos get_participant_qos(
            const YAML::Node& config);

    //! Get Integration Service Participant default Qos
    eprosima::fastdds::dds::DomainParticipantQos get_default_participant_qos();

    //! Get Databroker DomainParticipantQos with TCP enable in WAN
    //! It uses \c get_participant_qos to reuse std participant tags
    eprosima::fastdds::dds::DomainParticipantQos get_databroker_qos(
            const YAML::Node& config);

private:

    /**
     * Class members.
     */
    ::fastdds::dds::DomainParticipant* dds_participant_;

    std::map<std::string, fastrtps::types::DynamicPubSubType> types_;
    std::map<std::string, std::string> topic_to_type_;
    std::map<::fastdds::dds::Topic*, std::set<::fastdds::dds::DomainEntity*> > topic_to_entities_;
    std::mutex topic_to_entities_mtx_;

    is::utils::Logger logger_;
};

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

#endif //  _IS_SH_FASTDDS__INTERNAL__PARTICIPANT_HPP_

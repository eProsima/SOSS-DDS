// Copyright 2021 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file utils.hpp
 *
 */

#ifndef _IS_SH_FASTDDS__INTERNAL__UTILS_HPP_
#define _IS_SH_FASTDDS__INTERNAL__UTILS_HPP_

#include <sstream>
#include <vector>

#include <fastdds/rtps/common/GuidPrefix_t.hpp>

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

#define SERVER_DEFAULT_GUID "01.0f.00.44.41.54.95.42.52.4f.4b.45.52"
#define SERVER_DEFAULT_GUID_ID_INDEX 2

inline eprosima::fastrtps::rtps::GuidPrefix_t guid_server(
        uint8_t id)
{
    eprosima::fastrtps::rtps::GuidPrefix_t guid;
    std::istringstream(SERVER_DEFAULT_GUID) >> guid;
    guid.value[SERVER_DEFAULT_GUID_ID_INDEX] = static_cast<unsigned char>(id);
    return guid;
}

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

#endif // _IS_SH_FASTDDS__INTERNAL__UTILS_HPP_

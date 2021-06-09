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
 * @file utils.cpp
 *
 */

#include <sstream>
#include <vector>

#include "utils.hpp"

namespace eprosima {
namespace is {
namespace sh {
namespace fastdds {

std::vector<std::pair<std::string, uint16_t>> split_locator(std::string addresses, std::string value_delimiter, std::string address_delimiter)
{
    std::vector<std::pair<std::string, uint16_t>> result;

    size_t pos_ini = 0;
    size_t pos = 0;
    std::string token;

    if (addresses == "")
    {
        return result;
    }

    if (addresses.find(address_delimiter) == std::string::npos)
    {
        // get address ip
        std::string ip = addresses.substr(0, addresses.find(value_delimiter));

        // get port
        uint16_t port = std::stol(addresses.substr(ip.length()+1));

        result.push_back(std::make_pair(ip, port));

        return result;
    }

    do
    {
        pos = addresses.find(address_delimiter, pos_ini);

        token = addresses.substr(pos_ini, pos - pos_ini);

        // get address ip
        std::string ip = addresses.substr(pos_ini, token.find(value_delimiter));

        // get port
        uint16_t port = std::stol(token.substr(ip.length()+1));

        result.push_back(std::make_pair(ip, port));
        pos_ini = pos + 1;

    }while (pos != std::string::npos);

    return result;
}

std::vector<std::tuple<std::string, uint16_t, uint16_t>> split_ds_locator(std::string addresses, std::string value_delimiter, std::string address_delimiter)
{
    std::vector<std::tuple<std::string, uint16_t, uint16_t>> result;

    size_t pos_ini = 0;
    size_t pos = 0;
    std::string token;

    size_t first_delimiter_pos = 0;
    size_t second_delimiter_pos = 0;

    if (addresses == "")
    {
        return result;
    }

    if (addresses.find(address_delimiter) == std::string::npos)
    {
        first_delimiter_pos = addresses.find(value_delimiter);
        second_delimiter_pos = addresses.find(value_delimiter, first_delimiter_pos+1);

        // get address ip
        std::string ip = addresses.substr(0, first_delimiter_pos);

        // get port
        uint16_t port = std::stol(addresses.substr(first_delimiter_pos+1, second_delimiter_pos));

        // get id
        uint16_t id = std::stol(addresses.substr(second_delimiter_pos+1));

        result.push_back(std::make_tuple(ip, port, id));

        return result;
    }

    do
    {

        pos = addresses.find(address_delimiter, pos_ini);

        token = addresses.substr(pos_ini, pos - pos_ini);

        first_delimiter_pos = token.find(value_delimiter);
        second_delimiter_pos = token.find(value_delimiter, first_delimiter_pos+1);

        // get address ip
        std::string ip = token.substr(0, first_delimiter_pos);

        // get port
        uint16_t port = std::stol(token.substr(first_delimiter_pos+1, second_delimiter_pos));

        // get id
        uint16_t id = std::stol(token.substr(second_delimiter_pos+1));

        result.push_back(std::make_tuple(ip, port, id));
        pos_ini = pos + 1;

    } while (pos != std::string::npos);

    return result;
}

std::string print_locator(std::string addresses, std::string value_delimiter, std::string address_delimiter)
{
    std::stringstream result;
    for (auto locator : split_locator(addresses, value_delimiter, address_delimiter))
    {
        result << "IP: " << locator.first << "  port: " << locator.second << std::endl;
    }
    return result.str();
}

std::string print_ds_locator(std::string addresses, std::string value_delimiter, std::string address_delimiter)
{
    std::stringstream result;
    for (auto locator : split_ds_locator(addresses, value_delimiter, address_delimiter))
    {
        result << "IP: " << std::get<0>(locator) << "  port: " << std::get<1>(locator)
            << "  id: " << std::get<2>(locator) << std::endl;
    }
    return result.str();
}

} //  namespace fastdds
} //  namespace sh
} //  namespace is
} //  namespace eprosima

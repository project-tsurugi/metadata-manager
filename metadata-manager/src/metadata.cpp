/*
 * Copyright 2020 tsurugi project.
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
 */
#include <iostream>
#include <string>
#include <string_view>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

using namespace boost::property_tree;

namespace manager::metadata {
/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)   [in]  database name.
 *  @param  (tablename)  [in]  metadata-table name.
 *  @param  (pt)         [out] property_tree object to populating metadata.
 *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(
    __attribute__((unused)) std::string_view database, std::string_view tablename,
    boost::property_tree::ptree& pt, __attribute__((unused)) const uint64_t generation)
{
    std::string filename = std::string(storage_dir_path) + std::string{tablename} + ".json";

    try {
        read_json(filename, pt);
    } catch (boost::property_tree::json_parser_error& e) {
        std::wcout << "read_json() error. " << e.what() << std::endl;
        return ErrorCode::UNKNOWN;
    } catch (...) {
        std::cout << "read_json() error." << std::endl;
        return ErrorCode::UNKNOWN;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (tablename)  [in]  metadata-table name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 */
ErrorCode Metadata::save(
    __attribute__((unused)) std::string_view database, std::string_view tablename,
    boost::property_tree::ptree& pt, __attribute__((unused)) uint64_t* generation)
{
    std::string filename = std::string(storage_dir_path) + std::string{tablename} + ".json";

    try {
        write_json(filename, pt);
    }
    catch (...) {
        std::cout << "write_json() error." << std::endl;
        return ErrorCode::UNKNOWN;
    }

    if (generation != nullptr) {
        *generation = 1;
    }

    return ErrorCode::OK;
}

/*
 *  @biref  initialization of Metadata.
 */
void Metadata::init(ptree& root)
{
    root.put(Metadata::FORMAT_VERSION, 1);
    root.put(Metadata::GENERATION, 1);
}

/**
 *  @brief  Read latest table-metadata from metadata-table.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load()
{
    return load(Metadata::LATEST_VERSION);
}

/**
 *  @brief  Read table-metadata which specific generation from metadata-table.
 *  @param  (generation) [in]  metadata generation to read.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(const uint64_t generation)
{
    return Metadata::load(database(), table_name(), metadata_, generation);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (object) [in]  metadata-object to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree& object)
{
    return add(object, nullptr);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (object)    [in]  metadata-object to add.
 *  @param  (object_id) [out] ID of the added metadata-object.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree& object, uint64_t* object_id)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    // generate the object ID of the added metadata-object.
    uint64_t new_id = generate_object_id();
    object.put(ID, new_id);
    if (object_id != nullptr) {
        *object_id = new_id;
    }

    error = fill_parameters(object);
    if (error != ErrorCode::OK) {
        return error;
    }

    // add new element.
    ptree node = metadata_.get_child(root_node());
    node.push_back(std::make_pair("", object));
    metadata_.put_child(root_node(), node);

    error = Metadata::save(database(), table_name(), metadata_);

    return error;
}

/**
 *  @brief  Get metadata-object.
 *  @param  (object_id) [in]  metadata-object ID.
 *  @param  (object)    [out] metadata-object with the specified ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get(const ObjectIdType object_id, boost::property_tree::ptree& object) const
{
    assert(object_id > 0);

    ErrorCode error = ErrorCode::UNKNOWN;

    object.clear();

    error = ErrorCode::ID_NOT_FOUND;
    BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
        const ptree& temp_obj = node.second;

        boost::optional<ObjectIdType> id = temp_obj.get_optional<ObjectIdType>(ID);
        if (!id) {
            return ErrorCode::NOT_FOUND;
        }
        if (id == object_id) {
            object = temp_obj;
            error = ErrorCode::OK;
            break;
        }
    }

    return error;
}

/**
 *  @brief  Get metadata-object.
 *  @param  (object_name)   [in]  metadata-object name. (Value of "name" key.)
 *  @param  (object)        [out] metadata-object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get(
    std::string_view object_name, boost::property_tree::ptree& object) const
{
    assert(!object_name.empty());

    ErrorCode error = ErrorCode::UNKNOWN;

    error = ErrorCode::NAME_NOT_FOUND;
    BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
        const ptree& temp_obj = node.second;

        boost::optional<std::string> name = temp_obj.get_optional<std::string>(NAME);
        if (!name) {
            return ErrorCode::NOT_FOUND;
        }
        if (!name.get().compare(object_name)) {
            object = temp_obj;
            error = ErrorCode::OK;
            break;
        }
    }

    return error;
}

/**
 *  @brief  Get metadata-object.
 *  @param  (key)           [in]  metadata-object key.
 *  @param  (value)         [in]  metadata-object value.
 *  @param  (object)        [out] metadata-object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get(
    const char *object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const
{
    assert(object_key != NULL);
    assert(!object_value.empty());

    ErrorCode error = ErrorCode::UNKNOWN;

    error = ErrorCode::NAME_NOT_FOUND;
    BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
        const ptree& temp_obj = node.second;

        boost::optional<std::string> value = temp_obj.get_optional<std::string>(object_key);
        if (!value) {
            return ErrorCode::NOT_FOUND;
        }
        if (!value.get().compare(object_value)) {
            object = temp_obj;
            error = ErrorCode::OK;
            break;
        }
    }

    return error;
}

/**
 *  @brief  Remove metadata-object from metadata-table.
 *  @param  (object_name)   [in] name of metadata-object. (Value of "name" key.)
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::remove(const char *object_name)
{
    assert(object_name != nullptr);

    ErrorCode error = ErrorCode::NAME_NOT_FOUND;

    ptree node = metadata_.get_child(root_node());

    for (ptree::iterator it = node.begin(); it != node.end(); ++it)
    //BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
    {
        const ptree &temp_obj = it->second;
        boost::optional<std::string> name = temp_obj.get_optional<std::string>(NAME);
        if (!name)
        {
            return ErrorCode::NOT_FOUND;
        }
        if (!name.get().compare(object_name))
        {
            std::cout << "erase:" << object_name << std::endl;
            node.erase(it);
            //temp_obj.clear();
            error = ErrorCode::OK;
            break;
        }
    }

    std::cout << "print node start" << std::endl;

    BOOST_FOREACH (const ptree::value_type &node, metadata_.get_child(root_node()))
    {
        const ptree &temp_obj = node.second;

        boost::optional<std::string> name = temp_obj.get_optional<std::string>(NAME);
        if (name)
        {
            std::cout << name << std::endl;
        }

    }
    std::cout << "print node end" << std::endl;

    error = Metadata::save(database(), table_name(), metadata_);

    return error;
}

/**
 *  @brief  Get next metadata-object.
 *  @param  (object) [out] property_tree object to populating metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
 */
ErrorCode Metadata::next(boost::property_tree::ptree& object)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    if (!object_queue_.empty()) {
        object_queue_.pop_front();
    } else{
        object_queue_ = metadata_.get_child(root_node());
    }

    if (!object_queue_.empty()) {
        object = object_queue_.front().second;
        error = ErrorCode::OK;
    } else {
        error = ErrorCode::END_OF_ROW;
    }

    return error;
}

} // namespace manager::metadata

/* ============================================================================================= */
namespace manager::metadata_manager {

const char * Metadata::FORMAT_VERSION   = "formatVersion";
const char * Metadata::GENERATION       = "generation";
const char * Metadata::ID               = "id";
const char * Metadata::NAME             = "name";

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)   [in]  database name.
 *  @param  (tablename)  [in]  metadata-table name.
 *  @param  (pt)         [out] property_tree object to populating metadata.
 *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(
    __attribute__((unused)) std::string_view database, std::string_view tablename,
    boost::property_tree::ptree& pt, __attribute__((unused)) const uint64_t generation)
{
    std::string filename = std::string{tablename} + ".json";

    try {
        read_json(filename, pt);
    } catch (boost::property_tree::json_parser_error& e) {
        std::wcout << "read_json() error. " << e.what() << std::endl;
        return ErrorCode::UNKNOWN;
    } catch (...) {
        std::cout << "read_json() error." << std::endl;
        return ErrorCode::UNKNOWN;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (tablename)  [in]  metadata-table name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 */
ErrorCode Metadata::save(
    __attribute__((unused)) std::string_view database, std::string_view tablename,
    boost::property_tree::ptree& pt, __attribute__((unused)) uint64_t* generation)
{
    std::string filename = std::string{tablename} + ".json";

    try {
        write_json(filename, pt);
    }
    catch (...) {
        std::cout << "write_json() error." << std::endl;
        return ErrorCode::UNKNOWN;
    }

    if (generation != nullptr) {
        *generation = 1;
    }

    return ErrorCode::OK;
}

/*
 *  @biref  initialization of Metadata.
 */
void Metadata::init(ptree& root)
{
    root.put(Metadata::FORMAT_VERSION, 1);
    root.put(Metadata::GENERATION, 1);

}

/**
 *  @brief  Read latest table-metadata from metadata-table.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load()
{
    return load(Metadata::LATEST_GENERATION);
}

/**
 *  @brief  Read table-metadata which specific generation from metadata-table.
 *  @param  (generation) [in]  metadata generation to read.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(const uint64_t generation)
{
    return Metadata::load(database(), table_name(), metadata_, generation);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (object) [in]  metadata-object to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree& object)
{
    return add(object, nullptr);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (object)    [in]  metadata-object to add.
 *  @param  (object_id) [out] ID of the added metadata-object.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree& object, uint64_t* object_id)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    // generate the object ID of the added metadata-object.
    uint64_t new_id = generate_object_id();
    object.put(ID, new_id);
    if (object_id != nullptr) {
        *object_id = new_id;
    }

    error = fill_parameters(object);
    if (error != ErrorCode::OK) {
        return error;
    }

    // add new element.
    ptree node = metadata_.get_child(root_node());
    node.push_back(std::make_pair("", object));
    metadata_.put_child(root_node(), node);

    Metadata::save(database(), table_name(), metadata_);

    error = ErrorCode::OK;

    return error;
}

/**
 *  @brief  Get metadata-object.
 *  @param  (object_id) [in]  metadata-object ID.
 *  @param  (object)    [out] metadata-object with the specified ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get(const ObjectIdType object_id, boost::property_tree::ptree& object) const
{
    assert(object_id > 0);

    ErrorCode error = ErrorCode::UNKNOWN;

    object.clear();

    error = ErrorCode::ID_NOT_FOUND;
    BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
        const ptree& temp_obj = node.second;

        boost::optional<ObjectIdType> id = temp_obj.get_optional<ObjectIdType>(ID);
        if (!id) {
            return ErrorCode::NOT_FOUND;
        }
        if (id == object_id) {
            object = temp_obj;
            error = ErrorCode::OK;
            break;
        }
    }

    return error;
}

/**
 *  @brief  Get metadata-object.
 *  @param  (object_name)   [in]  metadata-object name. (Value of "name" key.)
 *  @param  (object)        [out] metadata-object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get(
    std::string_view object_name, boost::property_tree::ptree& object) const
{
    assert(!object_name.empty());

    ErrorCode error = ErrorCode::UNKNOWN;

    error = ErrorCode::NAME_NOT_FOUND;
    BOOST_FOREACH (const ptree::value_type& node, metadata_.get_child(root_node())) {
        const ptree& temp_obj = node.second;

        boost::optional<std::string> name = temp_obj.get_optional<std::string>(NAME);
        if (!name) {
            return ErrorCode::NOT_FOUND;
        }
        if (!name.get().compare(object_name)) {
            object = temp_obj;
            error = ErrorCode::OK;
            break;
        }
    }

    return error;
}

/**
 *  @brief  Get next metadata-object.
 *  @param  (object) [out] property_tree object to populating metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
 */
ErrorCode Metadata::next(boost::property_tree::ptree& object)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    if (!object_queue_.empty()) {
        object_queue_.pop_front();
    } else{
        object_queue_ = metadata_.get_child(root_node());
    }

    if (!object_queue_.empty()) {
        object = object_queue_.front().second;
        error = ErrorCode::OK;
    } else {
        error = ErrorCode::END_OF_ROW;
    }

    return error;
}

} // namespace manager::metadata_manager

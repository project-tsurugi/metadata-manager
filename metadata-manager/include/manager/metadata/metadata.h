/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
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
#ifndef MANAGER_METADATA_H_
#define MANAGER_METADATA_H_

#include <string>
#include <string_view>
#include <sys/stat.h>
#include <iostream>
#include <boost/filesystem.hpp>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"

namespace manager::metadata {

using GenerationType = uint64_t;
using ObjectIdType = uint64_t;

class Metadata {
    public:
        /**
         * @brief key of metadata.
         */
        static constexpr const char* const FORMAT_VERSION   = "formatVersion";
        static constexpr const char* const GENERATION       = "generation";
        static constexpr const char* const ID               = "id";
        static constexpr const char* const NAME             = "name";

        /**
         * @brief storage path of metadata
         */
        static constexpr const char* const HOME_DIR = "HOME";
        static constexpr const char* const TSURUGI_METADATA_DIR = "/.local/tsurugi/metadata/";

        /**
         *  @brief  Constructor
         *  @param  (database)  [in]  database name.
         *  @param  (component) [in]  your component name.
         *  @return none.
         */
        Metadata(std::string_view& database, std::string_view& component) {
            namespace fs = boost::filesystem;
            fs::create_directories(storage_dir_path);
        }

        std::string_view database() const { return database_; }
        std::string_view component() const { return component_; }
        GenerationType generation() const { return generation_; }
        uint64_t format_version() const { return format_version_; }

        /**
         *  @brief  Load the latest metadata from metadata-table.
         *  @param  (database)  [in]  database name.
         *  @param  (component) [in]  component name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode
            load();

        /**
         *  @brief  Loads the metadata which specific generation from metadata-table.
         *  @param  (database)  [in]  database name.
         *  @param  (component) [in]  component name.
         *  @param  (generation) [in]  metadata generation to load.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load(uint64_t generation);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (object) [in]  metadata-object to add.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree& object);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (object)      [in]  metadata-object to add.
         *  @param  (object_id)   [out] ID of the added metadata-object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode add(boost::property_tree::ptree& object, ObjectIdType* object_id);

        /**
         *  @brief  Get metadata-object.
         *  @param  (object_id) [in]  metadata-object ID.
         *  @param  (object)    [out] metadata-object with the specified ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(const ObjectIdType object_id, boost::property_tree::ptree& object) const;

        /**
         *  @brief  Get metadata-object.
         *  @param  (object_name)   [in]  metadata-object name. (Value of "name" key.)
         *  @param  (object)        [out] metadata-object with the specified name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(
            std::string_view object_name, boost::property_tree::ptree& object) const;

        /**
         *  @brief  Get metadata-object.
         *  @param  (key)           [in]  metadata-object key.
         *  @param  (value)         [in]  metadata-object value.
         *  @param  (object)        [out] metadata-object with the specified name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode get(
            const char *object_key, std::string_view object_value, boost::property_tree::ptree& object) const;

#if 0
        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (object_id)   [in]  metadata-object ID.
         *  @param  (object)      [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if table_id NOT found.
         */
        virtual ErrorCode set(const uint64_t object_id, boost::property_tree::ptree& object) = 0;

        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (name)   [in]  name of metadata-object. (Value of "name" key.)
         *  @param  (object) [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if name NOT found.
         */
        virtual ErrorCode set(const std::string_view name, boost::property_tree::ptree& object) = 0;

        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  [in] metadata-object ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(const uint64_t object_id) = 0;
#endif
        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  (object_name) [in] name of metadata-object. (Value of "name" key.)
         *  @param  (object_id)   [out] ID of the added metadata-object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(const char *object_name, uint64_t* object_id);

        /**
         *  @brief  Get next metadata-object.
         *  @param  (object) [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
         */
        ErrorCode next(boost::property_tree::ptree& object);

        Metadata(const Metadata&) = delete;
        Metadata& operator=(const Metadata&) = delete;

    protected:
        static const uint64_t LATEST_VERSION = 0;
        static std::string storage_dir_path;

        boost::property_tree::ptree metadata_;

        static void init(boost::property_tree::ptree& root);

        /**
         *  @brief  Load metadata from metadata-table.
         *  @param  (database)   [in]  database name.
         *  @param  (tablename)  [in]  metadata-table name.
         *  @param  (pt)         [out] property_tree object to populating metadata.
         *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode load(
            std::string_view database, std::string_view tablename,
            boost::property_tree::ptree& pt, const GenerationType generation = LATEST_VERSION);

        /**
         *  @brief  Save the metadata to metadata-table.
         *  @param  (database)   [in]  database name.
         *  @param  (tablename)  [in]  metadata-table name.
         *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
         *  @param  (generation) [out] the generation of saved metadata.
         */
        static ErrorCode save(
            std::string_view database, std::string_view tablename, boost::property_tree::ptree& pt,
            GenerationType* generation = nullptr);

        // functions for template-method.
        virtual std::string_view table_name() const = 0;
        virtual const std::string root_node() const = 0;
        virtual ObjectIdType generate_object_id() const = 0;
        virtual ErrorCode fill_parameters(boost::property_tree::ptree& object) = 0;

    private:
        std::string database_;
        std::string component_;
        GenerationType generation_ = 1;
        static constexpr uint64_t format_version_ = 1;
        boost::property_tree::ptree object_queue_;
};

} // namespace manager::metadata

/* ============================================================================================= */
namespace manager::metadata_manager {

using GenerationType = uint64_t;
using ObjectIdType = uint64_t;

class Metadata {
    public:
        static const char * FORMAT_VERSION;
        static const char * GENERATION;
        static const char * ID;
        static const char * NAME;

        /**
         *  @brief  Constructor
         *  @param  (database)  [in]  database name.
         *  @param  (component) [in]  your component name.
         *  @return none.
         */
        Metadata(std::string_view database, std::string_view component)
            : database_(database), component_(component) {}

        std::string_view database() const { return database_; }
        std::string_view component() const { return component_; }
        GenerationType generation() const { return generation_; }
        uint64_t format_version() const { return format_version_; }

        /**
         *  @brief  Load the latest metadata from metadata-table.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load();

        /**
         *  @brief  Loads the metadata which specific generation from metadata-table.
         *  @param  (generation) [in]  metadata generation to load.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load(uint64_t generation);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (object) [in]  metadata-object to add.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree& object);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (object)      [in]  metadata-object to add.
         *  @param  (object_id)   [out] ID of the added metadata-object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree& object, ObjectIdType* object_id);

        /**
         *  @brief  Get metadata-object.
         *  @param  (object_id) [in]  metadata-object ID.
         *  @param  (object)    [out] metadata-object with the specified ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(const ObjectIdType object_id, boost::property_tree::ptree& object) const;

        /**
         *  @brief  Get metadata-object.
         *  @param  (object_name)   [in]  metadata-object name. (Value of "name" key.)
         *  @param  (object)        [out] metadata-object with the specified name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(
            std::string_view object_name, boost::property_tree::ptree& object) const;
#if 0
        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (object_id)   [in]  metadata-object ID.
         *  @param  (object)      [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if table_id NOT found.
         */
        virtual ErrorCode set(const uint64_t object_id, boost::property_tree::ptree& object) = 0;

        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (name)   [in]  name of metadata-object. (Value of "name" key.)
         *  @param  (object) [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if name NOT found.
         */
        virtual ErrorCode set(const std::string_view name, boost::property_tree::ptree& object) = 0;

        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  [in] metadata-object ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(const uint64_t object_id) = 0;

        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  [in] name of metadata-object. (Value of "name" key.)
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(std::string_view name) = 0;
#endif
        /**
         *  @brief  Get next metadata-object.
         *  @param  (object) [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
         */
        ErrorCode next(boost::property_tree::ptree& object);

        Metadata(const Metadata&) = delete;
        Metadata& operator=(const Metadata&) = delete;

    protected:
        static const uint64_t LATEST_GENERATION = 0;

        static void init(boost::property_tree::ptree& root);

        /**
         *  @brief  Load metadata from metadata-table.
         *  @param  (database)   [in]  database name.
         *  @param  (tablename)  [in]  metadata-table name.
         *  @param  (pt)         [out] property_tree object to populating metadata.
         *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode load(
            std::string_view database, std::string_view tablename,
            boost::property_tree::ptree& pt, const GenerationType generation = LATEST_GENERATION);

        /**
         *  @brief  Save the metadata to metadata-table.
         *  @param  (database)   [in]  database name.
         *  @param  (tablename)  [in]  metadata-table name.
         *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
         *  @param  (generation) [out] the generation of saved metadata.
         */
        static ErrorCode save(
            std::string_view database, std::string_view tablename, boost::property_tree::ptree& pt,
            GenerationType* generation = nullptr);

        // functions for template-method.
        virtual std::string_view table_name() const = 0;
        virtual const std::string root_node() const = 0;
        virtual ObjectIdType generate_object_id() const = 0;
        virtual ErrorCode fill_parameters(boost::property_tree::ptree& object) = 0;

    private:
        boost::property_tree::ptree metadata_;
        std::string database_;
        std::string component_;
        GenerationType generation_ = 1;
        static constexpr uint64_t format_version_ = 1;
        boost::property_tree::ptree object_queue_;
};

} // namespace manager::metadata_manager

#endif // MANAGER_METADATA_H_

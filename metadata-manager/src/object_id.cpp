#include <iostream>
#include <fstream>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/optional.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/object_id.h"
#include "manager/metadata/object_id.h"

using namespace boost::property_tree;

namespace manager::metadata {

static ObjectIdType INVALID_OID = 0;

/**
 *  @brief  initialize object-ID metadata-table.
 */
ErrorCode ObjectId::init()
{
    std::ifstream file(ObjectId::TABLE_NAME);
    
    try {
        if (!file.is_open()) {
            // create oid-metadata-table.
            ptree root;
            write_ini(ObjectId::TABLE_NAME, root);
        }
    } catch (...) {
        return ErrorCode::UNKNOWN;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  current object-ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::current(const std::string table_name)
{
    boost::property_tree::ptree pt;

    init();

    try {
        read_ini(ObjectId::TABLE_NAME, pt);
    } catch (boost::property_tree::ini_parser_error& e) {
        std::wcout << "read_ini() error. " << e.what() << std::endl;
        return INVALID_OID;
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
    if (!oid) {
        // create OID key for specified metadata.
        pt.put(table_name, 0);
        oid = pt.get_optional<ObjectIdType>(table_name);
    }

    return oid.get();
}

/**
 *  @brief  generate new object-ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::generate(const std::string table_name) 
{
    boost::property_tree::ptree pt;

    init();

    try {
        read_ini(ObjectId::TABLE_NAME, pt);
    } catch (boost::property_tree::ini_parser_error& e) {
        std::wcout << "read_ini() error. " << e.what() << std::endl;
        return INVALID_OID;
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
    if (!oid) {
        // create OID key for specified metadata.
        pt.put(table_name, 0);
        oid = pt.get_optional<ObjectIdType>(table_name);
    }

    // generate new OID
    pt.put(table_name, ++oid.get());

    try {
        write_ini(ObjectId::TABLE_NAME, pt);
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    return oid.get();
}

} // namespace manager::metadata

/* ============================================================================================= */
namespace manager::metadata_manager {

static ObjectIdType INVALID_OID = 0;

const char * ObjectId::TABLE_NAME = "object_id.ini";

/**
 *  @brief  initialize object-ID metadata-table.
 */
ErrorCode ObjectId::init()
{
    std::ifstream file(ObjectId::TABLE_NAME);
    
    try {
        if (!file.is_open()) {
            // create oid-metadata-table.
            ptree root;
            write_ini(ObjectId::TABLE_NAME, root);
        }
    } catch (...) {
        return ErrorCode::UNKNOWN;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  current object-ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::current(const std::string table_name)
{
    boost::property_tree::ptree pt;

    init();

    try {
        read_ini(ObjectId::TABLE_NAME, pt);
    } catch (boost::property_tree::ini_parser_error& e) {
        std::wcout << "read_ini() error. " << e.what() << std::endl;
        return INVALID_OID;
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
    if (!oid) {
        // create OID key for specified metadata.
        pt.put(table_name, 0);
        oid = pt.get_optional<ObjectIdType>(table_name);
    }

    return oid.get();
}

/**
 *  @brief  generate new object-ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::generate(const std::string table_name) 
{
    boost::property_tree::ptree pt;

    init();

    try {
        read_ini(ObjectId::TABLE_NAME, pt);
    } catch (boost::property_tree::ini_parser_error& e) {
        std::wcout << "read_ini() error. " << e.what() << std::endl;
        return INVALID_OID;
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
    if (!oid) {
        // create OID key for specified metadata.
        pt.put(table_name, 0);
        oid = pt.get_optional<ObjectIdType>(table_name);
    }

    // generate new OID
    pt.put(table_name, ++oid.get());

    try {
        write_ini(ObjectId::TABLE_NAME, pt);
    } catch (...) {
        std::cout << "read_ini() error." << std::endl;
        return INVALID_OID;
    }

    return oid.get();
}

} // namespace manager::metadata_manager

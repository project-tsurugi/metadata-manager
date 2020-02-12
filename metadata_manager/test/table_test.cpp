#include <iostream>
#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/optional.hpp>

#include "error_code.h"
#include "metadata.h"
#include "table_metadata.h"

using namespace manager::metadata_manager;
using namespace boost::property_tree;

const char * const TEST_DB = "test_DB";

void print_error(ErrorCode error, uint64_t line)
{
    std::cout << "error occurred at line " << line << ", errorno: " << (uint64_t) error << std::endl;
}

/*
 *  @biref  test for Metadata class static functions.
 */
ErrorCode static_functions_test()
{
    ptree pt;
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    // create table-metadata
    //
    ptree tables;
    {
        ptree table;
        table.put("name", "table1");
        table.put("namespace", "public");

        ptree columns;
        ptree column;
        column.put("name", "column11");
        column.put("ordinal_position", 1);
        column.put("data_type_id", 1);
        column.put("nullable", false);
        columns.push_back(std::make_pair("", column));

        column.put("name", "column12");
        column.put("ordinal_position", 2);
        column.put("data_type_id", 2);
        column.put("nullable", true);
        columns.push_back(std::make_pair("", column));

        ptree constraints;
        ptree constraint;
        ptree column_keys;
        ptree column_key[2];
        column_key[0].put("", 1);
        column_key[1].put("", 2);
        column_keys.push_back(std::make_pair("", column_key[0]));
        column_keys.push_back(std::make_pair("", column_key[1]));
        constraint.add_child("column_key", column_keys);
        constraint.put("type", "p");
        constraints.push_back(std::make_pair("", constraint));

        table.add_child("columns", columns);
        table.add_child("constraints", constraints);
        tables.push_back(std::make_pair("", table));
    }
    pt.add_child("tables", tables);

    //  save table-metadata
    error = TableMetadata::save(TEST_DB, pt);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    //  load table-metadata
    //
    error = TableMetadata::load(TEST_DB, pt);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    std::cout << "--- tables ---" << std::endl;
    tables = pt.get_child("tables");
    BOOST_FOREACH (const ptree::value_type& child, pt.get_child("tables")) {
        const ptree& table = child.second;

        boost::optional<std::string> name = table.get_optional<std::string>("name");
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table name : " << name.get() << std::endl;

        boost::optional<std::string> table_namespace = table.get_optional<std::string>("namespace");
        if (!table_namespace) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "namespace : " << table_namespace.get() << std::endl;

        std::cout << "--- columns ---" << std::endl;
        ptree columns = table.get_child("columns");
        BOOST_FOREACH (const ptree::value_type& child, table.get_child("columns")) {
            const ptree& column = child.second;
            boost::optional<std::string> name = column.get_optional<std::string>("name");
            if (!name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << name.get() << std::endl;

            boost::optional<uint64_t> ordinal_position 
                = column.get_optional<std::uint64_t>("ordinal_position");
            if (!ordinal_position) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "ordinal_position : " << ordinal_position.get() << std::endl;

            boost::optional<uint64_t> data_type_id = column.get_optional<uint64_t>("data_type_id");
            if (!data_type_id) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "data_type_id : " << data_type_id.get() << std::endl;

            boost::optional<bool> nullable = column.get_optional<bool>("nullable");
            if (!nullable) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "nullable : " << nullable.get() << std::endl;
        }

        std::cout << "--- constraints ---" << std::endl;
        ptree constraints = table.get_child("constraints");
        BOOST_FOREACH (const ptree::value_type& child, table.get_child("constraints")) {
            const ptree& constraint = child.second;

            ptree column_keys = constraint.get_child("column_key");
            BOOST_FOREACH (const ptree::value_type& child, column_keys) {
                std::cout << "column_key : " << child.second.data() << std::endl;
            }
            if (boost::optional<std::string> type = constraint.get_optional<std::string>("type")) {
                std::cout << "constraint type : " << type.get() << std::endl;
            } else {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
        }
        
    }

    return ErrorCode::OK;
}

/*
 *  @biref  test for TableMetadata class object.
 */
ErrorCode class_object_test()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    //  add table-metadata object and save
    //
    Metadata* tables = new TableMetadata(TEST_DB);
    tables->load();

    // table-metadata
    ptree new_table;
    new_table.put("name", "new_table");
    new_table.put("namespace", "public");
    
    // column-metadata
    ptree columns;
    {
        ptree column;
        column.put("name", "new_column21");
        column.put<uint64_t>("ordinal_position", 1);
        column.put("data_type_id", 1);
        column.put<bool>("nullable", false);
        columns.push_back(std::make_pair("", column));

        column.put("name", "new_column22");
        column.put<uint64_t>("ordinal_position", 2);
        column.put("data_type_id", 2);
        column.put<bool>("nullable", true);
        columns.push_back(std::make_pair("", column));
    }
    new_table.add_child("columns", columns);

    // constraint-metadata
    ptree constraints;
    {
        ptree constraint;
        ptree column_keys;
        {
            ptree column_key[2];
            column_key[0].put("", 1);
            column_key[1].put("", 2);
            column_keys.push_back(std::make_pair("", column_key[0]));
            column_keys.push_back(std::make_pair("", column_key[1]));
        }
        constraint.add_child("column_key", column_keys);
        constraint.put("type", "p");
        constraints.push_back(std::make_pair("", constraint));
    }
    new_table.add_child("constraints", constraints);

    // add metadata-object
    if (tables->add(new_table) != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    //  load table-metadata
    //
    ptree pt;
    while (tables->next(pt) == ErrorCode::OK) {
        // table metadata
        std::cout << "--- tables ---" << std::endl;
        boost::optional<std::string> name = pt.get_optional<std::string>("name");
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table name : " << name << std::endl;

        // column metadata
        std::cout << "--- columns ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& e, pt.get_child("columns")) {
            const ptree& column = e.second;

            boost::optional<std::string> name = column.get_optional<std::string>("name");
            if (!name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << name << std::endl;

            boost::optional<uint64_t> ordinal_position 
                = column.get_optional<uint64_t>("ordinal_position");
            if (!ordinal_position) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "ordinal_position : " << ordinal_position << std::endl;

            boost::optional<std::string> data_type_id 
                = column.get_optional<std::string>("data_type_id");
            if (!data_type_id) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "data_type : " << data_type_id << std::endl;

            boost::optional<bool> nullable = column.get_optional<bool>("nullable");
            if (!nullable) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "nullable : " << nullable << std::endl;
        }
    }

    return ErrorCode::OK;
}

/*
 *  @biref  main function.
 */
int main() 
{
    std::cout << "*** TableMetadta test start. ***" << std::endl << std::endl;

    std::cout << "=== static functions test start. ===" << std::endl;;
    if (static_functions_test() == ErrorCode::OK) {
        std::cout << "=== static functions test has succeeded. ===" << std::endl;
    } else {
        std::cout << "=== static functions test has failed. ===" << std::endl;
    }
    std::cout << std::endl;

    std::cout << "=== class object test start. ===" << std::endl;
    if (class_object_test() == ErrorCode::OK) {
        std::cout << "=== class object test has succeeded. ===" << std::endl;
    } else {
        std::cout << "=== class object test has failed. ===" << std::endl;
    }
    std::cout << std::endl;

    std::cout << "*** TableMetadta test has done. ***" << std::endl;;

    return 0;
}

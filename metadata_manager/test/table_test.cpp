#include <iostream>
#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/optional.hpp>

#include "error_code.h"
#include "metadata.h"
#include "datatype_metadata.h"
#include "table_metadata.h"

using namespace manager::metadata_manager;
using namespace boost::property_tree;

const char * const TEST_DB = "test_DB";

void print_error(ErrorCode error, uint64_t line)
{
    std::cout << "error occurred at line " << line << ", errorno: " << (uint64_t) error << std::endl;
}

/*
 *  @biref  create datatype-metadata.
 *  @note   this function for metadata-manager test.
 */
ErrorCode create_datatypes()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    ptree root;
    root.put("format_version", 1);
    root.put("generation", 1);
    
    ptree datatypes;
    {
        ptree datatype;
        // NULL_VALUE
        datatype.put("id", 1);
        datatype.put("name", "NULL_VALUE");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT16
        datatype.put("id", 1);
        datatype.put("name", "INT16");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT32
        datatype.put("id", 2);
        datatype.put("name", "INT32");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT64
        datatype.put("id", 3);
        datatype.put("name", "INT64");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT32
        datatype.put("id", 4);
        datatype.put("name", "FLOAT32");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT64
        datatype.put("id", 5);
        datatype.put("name", "FLOAT64");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));

        // TEXT
        datatype.put("id", 6);
        datatype.put("name", "TEXT");
        datatype.put("pg_datatype", 0);
        datatypes.push_back(std::make_pair("", datatype));
    }
    root.add_child(DatatypeMetadata::DATATYPES_NODE, datatypes);

    error = DatatypeMetadata::save(TEST_DB, root);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    return ErrorCode::OK;
}

/*
 *  @biref  test for Metadata class static functions.
 */
ErrorCode static_functions_test()
{
    ptree root;
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    // create table-metadata
    //
    ptree tables;

    // tables
    ptree table;
    table.put("name", "table1");
    table.put("namespace", "public");

    // columns
    ptree columns;
    ptree column;
    {
        column.put("name", "column11");
        column.put("ordinal_position", 1);
        column.put("datatype_name", "INT64");
        column.put("nullable", false);
    }
    columns.push_back(std::make_pair("", column));

    {
        column.put("name", "column12");
        column.put("ordinal_position", 2);
        column.put("datatype_name", "TEXT");
        column.put("nullable", true);
    }
    columns.push_back(std::make_pair("", column));

    // constraints
    ptree constraints;
    ptree constraint;
    {
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
    }
    constraints.push_back(std::make_pair("", constraint));

    // primary index
    ptree primary_index;
    ptree column_info;
    {
        primary_index.put("name", "primary_index1");
        column_info.put("name", "column11");
        column_info.put("direction", 0);
    }
    primary_index.push_back(std::make_pair("column", column_info));

    // secondary indices
    ptree secondary_indices;
    ptree index;
    {
        index.put("name", "secondary_index1");
        column_info.put("name", "column12" );
        column_info.put("direction", 1);
    }    
    index.push_back(std::make_pair("column", column_info));
    secondary_indices.push_back(std::make_pair("", index));

    // add child trees
    table.add_child("columns", columns);
    table.add_child("constraints", constraints);
    table.add_child("primary_index", primary_index);
    table.add_child("secondary_indices", secondary_indices);
    tables.push_back(std::make_pair("", table));

    root.add_child("tables", tables);

    //  save table-metadata
    error = TableMetadata::save(TEST_DB, root);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    //  load table-metadata
    //
    error = TableMetadata::load(TEST_DB, root);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    std::cout << "--- tables ---" << std::endl;
    tables = root.get_child("tables");
    BOOST_FOREACH (const ptree::value_type& child, root.get_child("tables")) {
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

            boost::optional<uint64_t> datatype_name 
                = column.get_optional<uint64_t>("datatype_name");
            if (!datatype_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "datatype_name : " << datatype_name.get() << std::endl;

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

            boost::optional<std::string> type = constraint.get_optional<std::string>("type");
            if (!type) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "constraint type : " << type.get() << std::endl;
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
    std::unique_ptr<Metadata> tables(new TableMetadata(TEST_DB));

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
        column.put("datatype_name", "FLOAT32");
        column.put<bool>("nullable", false);
        columns.push_back(std::make_pair("", column));

        column.put("name", "new_column22");
        column.put<uint64_t>("ordinal_position", 2);
        column.put("datatype_name", "TEXT");
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
            ptree column_key[1];
            column_key[0].put("", 1);
            column_keys.push_back(std::make_pair("", column_key[0]));
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
    ptree root;
    while (tables->next(root) == ErrorCode::OK) {
        // table metadata
        std::cout << "--- tables ---" << std::endl;
        boost::optional<std::string> name = root.get_optional<std::string>("name");
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table name : " << name << std::endl;

        // column metadata
        std::cout << "--- columns ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& e, root.get_child("columns")) {
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

            boost::optional<std::string> datatype_name 
                = column.get_optional<std::string>("datatype_name");
            if (!datatype_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "datatype : " << datatype_name << std::endl;

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
int main(void) 
{
    std::cout << "*** TableMetadta test start. ***" << std::endl << std::endl;

    if (create_datatypes() != ErrorCode::OK) {
        std::cout << "create data-types has failed." << std::endl;
        std::cout << "*** TableMetadta test has done. ***" << std::endl;;
    }

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

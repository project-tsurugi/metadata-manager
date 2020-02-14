#include <iostream>
#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/optional.hpp>

#include "error_code.h"
#include "object_id.h"
#include "metadata.h"
#include "datatype_metadata.h"
#include "table_metadata.h"

using namespace manager::metadata_manager;
using namespace boost::property_tree;

const char * const TEST_DB = "test_DB";

/*
 *  @brief  print error code and line number.
 */
void print_error(ErrorCode error, uint64_t line)
{
    std::cout << "error occurred at line " << line << ", errorno: " << (uint64_t) error << std::endl;
}

/*
 *  @brief  generate new table name.
 */
const std::string get_tablename()
{
    static uint64_t sequential = 0;
    std::string name = "table_" + std::to_string(++sequential);

    return name;
}

/*
 *  @brief  initialize test environment.
 */
bool initialize()
{
    bool success = false;

    // create oid-metadata-table.
    if (ObjectId::init() != ErrorCode::OK) {
        std::cout << "initialization of TableMetadata class failed." << std::endl;
        return success;
    }
    
    // create table-metadata-table.
    if (TableMetadata::init() != ErrorCode::OK) {
        std::cout << "initialization of TableMetadata class failed." << std::endl;
        return success;
    }
       
    if (DatatypeMetadata::init() != ErrorCode::OK) {
        std::cout << "initialization of DatatypeMetadata failed." << std::endl;
        return success;
    }

    success = true;

    return success;
}

/*
 *  @biref  test for Metadata class static functions.
 */
ErrorCode static_functions_test()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    //  load table-metadata
    //
    ptree root;
    ptree tables;

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

            boost::optional<std::string> datatype_name 
                = column.get_optional<std::string>("datatype_name");
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

        std::cout << "--- primary index ---" << std::endl;
        const ptree& primary_index = table.get_child("primary_index");
        boost::optional<std::string> index_name = primary_index.get_optional<std::string>("name");
        if (!index_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "primary index name : " << index_name.get() << std::endl;      

        const ptree& column = primary_index.get_child("column");
        boost::optional<std::string> column_name = column.get_optional<std::string>("name");
        if (!column_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "column name : " << column_name.get() << std::endl;      

        boost::optional<uint16_t> direction = column.get_optional<uint16_t>("direction");
        if (!direction) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "direction : " << direction.get() << std::endl;

        std::cout << "--- secondary indices ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& child, table.get_child("secondary_indices")) {
            const ptree& secondary_index = child.second;

            boost::optional<std::string> index_name = secondary_index.get_optional<std::string>("name");
            if (!index_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "secondary index name : " << index_name.get() << std::endl;

            const ptree& column = secondary_index.get_child("column");
            boost::optional<std::string> column_name = column.get_optional<std::string>("name");
            if (!column_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << column_name.get() << std::endl;      

            boost::optional<uint16_t> direction = column.get_optional<uint16_t>("direction");
            if (!direction) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "direction : " << direction.get() << std::endl;
        }
        std::cout << std::endl;
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

    error = tables->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    // table-metadata
    ptree new_table;
    new_table.put("name", get_tablename());
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

    // primary index
    ptree primary_index;
    ptree column_info;
    {
        primary_index.put("name", "primary_index1");
        column_info.put("name", "column21");
        column_info.put("direction", 1);
    }
    primary_index.push_back(std::make_pair("column", column_info));
    new_table.add_child("primary_index", primary_index);

    // secondary indices
    ptree secondary_indices;
    ptree index;
    {
        index.put("name", "secondary_index1");
        column_info.put("name", "column22" );
        column_info.put("direction", 0);
    }    
    index.push_back(std::make_pair("column", column_info));
    secondary_indices.push_back(std::make_pair("", index));
    new_table.add_child("secondary_indices", secondary_indices);

    // add table-metadata-object
    error = tables->add(new_table);
    if ( error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    //  load table-metadata
    //
    std::cout << "--- table-metadata parameters to read. ---" << std::endl;

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

        // constraint metadata
        std::cout << "--- constraints ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& child, root.get_child("constraints")) {
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

        // primary index
        std::cout << "--- primary index ---" << std::endl;
        const ptree& primary_index = root.get_child("primary_index");
        boost::optional<std::string> index_name = primary_index.get_optional<std::string>("name");
        if (!index_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "primary index name : " << index_name.get() << std::endl;      

        const ptree& column = primary_index.get_child("column");
        boost::optional<std::string> column_name = column.get_optional<std::string>("name");
        if (!column_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "column name : " << column_name.get() << std::endl;      

        boost::optional<uint16_t> direction = column.get_optional<uint16_t>("direction");
        if (!direction) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "direction : " << direction.get() << std::endl;

        // secondary indices
        std::cout << "--- secondary indices ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& child, root.get_child("secondary_indices")) {
            const ptree& secondary_index = child.second;

            boost::optional<std::string> index_name = secondary_index.get_optional<std::string>("name");
            if (!index_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "secondary index name : " << index_name.get() << std::endl;

            const ptree& column = secondary_index.get_child("column");
            boost::optional<std::string> column_name = column.get_optional<std::string>("name");
            if (!column_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << column_name.get() << std::endl;      

            boost::optional<uint16_t> direction = column.get_optional<uint16_t>("direction");
            if (!direction) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "direction : " << direction.get() << std::endl;
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

    if (!initialize()) {
        std::cout << "initialization of test environment failed." << std::endl;
        std::cout << "*** TableMetadta test done. ***" << std::endl;      
    }

    std::cout << "=== class object test start. ===" << std::endl;
    if (class_object_test() == ErrorCode::OK) {
        std::cout << "=== class object test has succeeded. ===" << std::endl;
    } else {
        std::cout << "=== class object test has failed. ===" << std::endl;
    }
    std::cout << std::endl;

    std::cout << "=== static functions test start. ===" << std::endl;;
    if (static_functions_test() == ErrorCode::OK) {
        std::cout << "=== static functions test has succeeded. ===" << std::endl;
    } else {
        std::cout << "=== static functions test has failed. ===" << std::endl;
    }
    std::cout << std::endl;

    std::cout << "*** TableMetadta test completed. ***" << std::endl;;

    return 0;
}

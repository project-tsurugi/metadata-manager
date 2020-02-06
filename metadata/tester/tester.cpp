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

using namespace management::metadata;
using namespace boost::property_tree;

void init(ptree &pt)
{
    ptree child;
    {
        ptree info;
        info.put("id", 1);
        info.put("name", "table1");
        child.push_back(std::make_pair("", info));
    }
    {
        ptree info;
        info.put("id", 2);
        info.put("name", "table2");
        child.push_back(std::make_pair("", info));
    }
    pt.add_child("table", child);

}

void print_error(ErrorCode error)
{
    std::cout << "error : " << (const int) error << std::endl;
}

int main(void) 
{
    ptree pt;
//    ErrorCode error = ErrorCode::UNKNOWN;

#if 0
    ptree pt1;
    write_json(std::cout, pt);
    init(pt1);
    write_json(std::cout, pt);
    Metadata::save("test", MetadataClass::TABLE, pt);
#endif

#if 0
    //
    //  Metadata Class Test
    //

    //
    //  load raw metadatas
    //
    error = Metadata::load("test", MetadataClass::TABLE, pt);
    if (error != ErrorCode::OK) {
        print_error(error);
        return -1;
    }

    BOOST_FOREACH (const ptree::value_type& child, pt.get_child(TableMetadata::TABLE_NODE)) {
        const ptree& table = child.second;

        if (boost::optional<int> id = table.get_optional<int>("id")) {
            std::cout << "id : " << id.get() << std::endl;
        }

        if (boost::optional<std::string> name = table.get_optional<std::string>("name")) {
            std::cout << "name : " << name.get() << std::endl;
        }
    }
    //
    //  save raw metadatas
    //
    Metadata::save("test", MetadataClass::TABLE, pt);
#endif
 
    //
    // TableMetadata Class test
    //
    TableMetadata tables("test_database");
    tables.load();

    //
    //  load table-metadata
    //
    std::cout << "load table-metadata" << std::endl;
    while (tables.next(pt) == ErrorCode::OK) {
        // table metadata
        boost::optional<std::string> name = pt.get_optional<std::string>("name");
        // column metadata
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& e, pt.get_child("columns")) {
            const ptree& column = e.second;

            // column name
            boost::optional<std::string> name = column.get_optional<std::string>("name");
            if (!name) {
                print_error(ErrorCode::NOT_FOUND);
                return -1;
            }
            std::cout << name << std::endl;

            // column number
            boost::optional<uint64_t> column_number = column.get_optional<uint64_t>("column_number");
            if (!column_number) {
                print_error(ErrorCode::NOT_FOUND);
                return -1;
            }
            std::cout << column_number << std::endl;

            // column data type
            boost::optional<std::string> data_type = column.get_optional<std::string>("data_type");
            if (!data_type) {
                print_error(ErrorCode::NOT_FOUND);
                return -1;
            }
            std::cout << data_type << std::endl;

            // nullable
            boost::optional<bool> nullable = column.get_optional<bool>("nullable");
            if (!nullable) {
                print_error(ErrorCode::NOT_FOUND);
                return -1;
            }
            std::cout << nullable << std::endl;
        }
    }

    //
    //  add table-metadata and save
    //
    std::cout << "add table-metadata" << std::endl;
    ptree new_table;
    {
        // table-metadata
        new_table.put("name", "new_table");
    }
    
    ptree columns;
    {
        // column-metadata
        ptree column;
        column.put("name", "new_column1");
        column.put<uint64_t>("column_number", 1);
        column.put("data_type", "TEXT");
        column.put<bool>("nullable", false);
        columns.push_back(std::make_pair("", column));

        column.put("name", "new_column2");
        column.put<uint64_t>("column_number", 2);
        column.put("data_type", "INT32");
        column.put<bool>("nullable", true);
        columns.push_back(std::make_pair("", column));
    }
    new_table.add_child("columns", columns);

    tables.add(new_table);

    {
        // table-metadata
        new_table.put("name", "new_table2");
    }
    {
        // column-metadata
        ptree column;
        column.put("name", "column1");
        column.put<uint64_t>("column_number", 1);
        column.put("data_type", "TEXT");
        column.put<bool>("nullable", false);
        columns.push_back(std::make_pair("", column));

        column.put("name", "column2");
        column.put<uint64_t>("column_number", 2);
        column.put("data_type", "INT32");
        column.put<bool>("nullable", true);
        columns.push_back(std::make_pair("", column));
    }
    new_table.add_child("columns", columns);
    tables.add(new_table);

    return 0;
}

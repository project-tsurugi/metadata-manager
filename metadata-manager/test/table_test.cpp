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
    std::cout << std::endl << "error occurred at line " << line << ", errorno: " << (uint64_t) error << std::endl;
}

/*
 *  @brief  generate new table name.
 */
const std::string get_tablename()
{
    ObjectIdType number = ObjectId::current("tables") + 1;
    std::string name = "table_" + std::to_string(number);

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
       
    if (DataTypeMetadata::init() != ErrorCode::OK) {
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
    BOOST_FOREACH (const ptree::value_type& node, root.get_child(TableMetadata::TABLES_NODE)) {
        const ptree& table = node.second;

        boost::optional<std::string> name = 
            table.get_optional<std::string>(TableMetadata::NAME);
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table name : " << name.get() << std::endl;

        boost::optional<std::string> table_namespace 
            = table.get_optional<std::string>(TableMetadata::NAMESPACE);
        if (!table_namespace) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "namespace : " << table_namespace.get() << std::endl;

        std::cout << "--- columns ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::COLUMNS_NODE)) {
            const ptree& column = node.second;

            boost::optional<std::string> name = 
                column.get_optional<std::string>(TableMetadata::Column::NAME);
            if (!name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << name.get() << std::endl;

            boost::optional<uint64_t> ordinal_position 
                  = column.get_optional<std::uint64_t>(TableMetadata::Column::ORDINAL_POSITION);
            if (!ordinal_position) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "ordinal position : " << ordinal_position.get() << std::endl;

            boost::optional<std::string> data_type_id 
                = column.get_optional<std::string>(TableMetadata::Column::DATA_TYPE_ID);
            if (!data_type_id) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "data type id : " << data_type_id.get() << std::endl;

            boost::optional<bool> nullable 
                = column.get_optional<bool>(TableMetadata::Column::NULLABLE);
            if (!nullable) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "nullable : " << nullable.get() << std::endl;
        }

        std::cout << "--- constraints ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::CONSTRAINTS_NODE)) {
            const ptree& constraint = node.second;

            ptree column_keys = 
                constraint.get_child(TableMetadata::Constraint::COLUMN_KEY_NODE);
            BOOST_FOREACH (const ptree::value_type& child, column_keys) {
                std::cout << "column key : " << child.second.data() << std::endl;
            }

            boost::optional<std::string> type 
                = constraint.get_optional<std::string>(TableMetadata::Constraint::TYPE);
            if (!type) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "constraint type : " << type.get() << std::endl;
        }

        std::cout << "--- primary index ---" << std::endl;
        const ptree& primary_index = table.get_child(TableMetadata::PRIMARY_INDEX_OBJECT);
        boost::optional<std::string> index_name
            = primary_index.get_optional<std::string>(TableMetadata::Index::NAME);
        if (!index_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "primary index name : " << index_name.get() << std::endl;      

        const ptree& column = primary_index.get_child(TableMetadata::Index::COLUMN_OBJECT);
        boost::optional<std::string> column_name 
            = column.get_optional<std::string>(TableMetadata::Index::Column::NAME);
        if (!column_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "column name : " << column_name.get() << std::endl;      

        boost::optional<uint16_t> 
            direction = column.get_optional<uint16_t>(TableMetadata::Index::Column::DIRECTION);
        if (!direction) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "direction : " << direction.get() << std::endl;

        std::cout << "--- secondary indices ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::SECONDARY_INDICES_NODDE)) {
            const ptree& secondary_index = node.second;

            boost::optional<std::string> index_name
                = secondary_index.get_optional<std::string>(TableMetadata::Index::NAME);
            if (!index_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "secondary index name : " << index_name.get() << std::endl;

            const ptree& column = secondary_index.get_child("column");
            boost::optional<std::string> column_name 
                = column.get_optional<std::string>(TableMetadata::Index::Column::NAME);
            if (!column_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << column_name.get() << std::endl;      

            boost::optional<uint16_t> direction 
                = column.get_optional<uint16_t>(TableMetadata::Index::Column::DIRECTION);
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
    std::unique_ptr<Metadata> tables(new TableMetadata(TEST_DB));   // use Template-Method.
    std::unique_ptr<Metadata> datatypes(new DataTypeMetadata(TEST_DB));
    ptree datatype;

    error = tables->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    error = datatypes->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    // table-metadata
    ptree new_table;
    new_table.put(TableMetadata::NAME, get_tablename());
    new_table.put(TableMetadata::NAMESPACE, "public");
    
    // column-metadata
    ptree columns;
    {
        ptree column;
        // column #1
        column.put(TableMetadata::Column::NAME, "column1");
        column.put<uint64_t>(TableMetadata::Column::ORDINAL_POSITION, 1);
        datatypes->get("FLOAT32", datatype);
        ObjectIdType data_type_id = datatype.get<ObjectIdType>(DataTypeMetadata::ID);
        column.put<ObjectIdType>(TableMetadata::Column::DATA_TYPE_ID, data_type_id);
        column.put<bool>(TableMetadata::Column::NULLABLE, false);
        columns.push_back(std::make_pair("", column));

        // column #2
        column.put(TableMetadata::Column::NAME, "ncolumn2");
        column.put<uint64_t>(TableMetadata::Column::ORDINAL_POSITION, 2);
        datatypes->get("TEXT", datatype);
        data_type_id = datatype.get<ObjectIdType>(DataTypeMetadata::ID);
        column.put(TableMetadata::Column::DATA_TYPE_ID, data_type_id);
        column.put<bool>(TableMetadata::Column::NULLABLE, true);

        // column-constraints
        ptree constraint;
        {
            ptree column_keys;
            {
                ptree column_key;
                column_key.put("", 2);   // column ordinal_position
                column_keys.push_back(std::make_pair("", column_key));
            }
            constraint.add_child(TableMetadata::Constraint::COLUMN_KEY_NODE, column_keys);
            constraint.put(TableMetadata::Constraint::TYPE, 
                TableMetadata::Constraint::Type::PRIMARY_KEY);
            column.push_back(std::make_pair(TableMetadata::CONSTRAINTS_NODE, constraint));
        }
        columns.push_back(std::make_pair("", column));
    }
    new_table.add_child(TableMetadata::COLUMNS_NODE, columns);

    // constraint-metadata
    ptree constraints;
    {
        ptree constraint;
        ptree column_keys;
        {
            ptree column_key[2];
            column_key[0].put("", 1);   // column ordinal_position
            column_keys.push_back(std::make_pair("", column_key[0]));
            column_key[1].put("", 2);   // column ordinal_position
            column_keys.push_back(std::make_pair("", column_key[1]));
        }
        constraint.add_child(TableMetadata::Constraint::COLUMN_KEY_NODE, column_keys);
        constraint.put(TableMetadata::Constraint::TYPE, 
            TableMetadata::Constraint::Type::PRIMARY_KEY);
        constraints.push_back(std::make_pair("", constraint));
    }
    new_table.add_child(TableMetadata::CONSTRAINTS_NODE, constraints);

    // primary index
    ptree primary_index;
    ptree column_info;
    {
        primary_index.put(TableMetadata::Index::NAME, "primary_index1");
        column_info.put(TableMetadata::Index::Column::NAME, "column1");
        column_info.put(TableMetadata::Index::Column::DIRECTION, 1);
    }
    primary_index.push_back(std::make_pair(TableMetadata::Index::COLUMN_OBJECT, column_info));
    new_table.add_child(TableMetadata::PRIMARY_INDEX_OBJECT, primary_index);

    // secondary indices
    ptree secondary_indices;
    ptree index;
    {
        index.put(TableMetadata::Index::NAME, "secondary_index1");
        column_info.put(TableMetadata::Index::Column::NAME, "column2" );
        column_info.put(TableMetadata::Index::Column::DIRECTION, 0);
    }    
    index.push_back(std::make_pair(TableMetadata::Index::COLUMN_OBJECT, column_info));
    secondary_indices.push_back(std::make_pair("", index));
    new_table.add_child(TableMetadata::SECONDARY_INDICES_NODDE, secondary_indices);

    // add table-metadata-object
    error = tables->add(new_table);
    if ( error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    //  load table-metadata
    //
    std::cout << "--- table-metadata to read. ---" << std::endl;

    ptree table;
    while (tables->next(table) == ErrorCode::OK) {
        // table metadata
        std::cout << "--- tables ---" << std::endl;
        boost::optional<ObjectIdType> table_id 
            = table.get_optional<ObjectIdType>(TableMetadata::ID);
        if (!table_id) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table ID : " << table_id.get() << std::endl;

        boost::optional<std::string> name 
            = table.get_optional<std::string>(TableMetadata::NAME);
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table name : " << name << std::endl;

        // column metadata
        std::cout << "--- columns ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::COLUMNS_NODE)) {
            const ptree& column = node.second;

            boost::optional<std::string> name 
                = column.get_optional<std::string>(TableMetadata::Column::NAME);
            if (!name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << name << std::endl;

            boost::optional<uint64_t> ordinal_position 
                = column.get_optional<uint64_t>(TableMetadata::Column::ORDINAL_POSITION);
            if (!ordinal_position) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "ordinal_position : " << ordinal_position << std::endl;

            boost::optional<ObjectIdType> data_type_id 
                = column.get_optional<ObjectIdType>(TableMetadata::Column::DATA_TYPE_ID);
            if (!data_type_id) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "datatype id : " << data_type_id << std::endl;
            datatypes->get(data_type_id.get(), datatype);
            std::cout << "datatype name : " 
                << datatype.get<std::string>(DataTypeMetadata::NAME) << std::endl;

            boost::optional<bool> nullable 
                = column.get_optional<bool>(TableMetadata::Column::NULLABLE);
            if (!nullable) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "nullable : " << nullable << std::endl;
        }

        // constraint metadata
        std::cout << "--- constraints ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::CONSTRAINTS_NODE)) {
            const ptree& constraint = node.second;

            ptree column_keys = constraint.get_child(TableMetadata::Constraint::COLUMN_KEY_NODE);
            BOOST_FOREACH (const ptree::value_type& e, column_keys) {
                std::cout << "column_key : " << e.second.data() << std::endl;
            }

            boost::optional<std::string> type 
                = constraint.get_optional<std::string>(TableMetadata::Constraint::TYPE);
            if (!type) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "constraint type : " << type.get() << std::endl;
        }

        // primary index
        std::cout << "--- primary index ---" << std::endl;
        const ptree& primary_index = table.get_child(TableMetadata::PRIMARY_INDEX_OBJECT);
        boost::optional<std::string> index_name
            = primary_index.get_optional<std::string>(TableMetadata::Index::NAME);
        if (!index_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "primary index name : " << index_name.get() << std::endl;      

        const ptree& column = primary_index.get_child(TableMetadata::Index::COLUMN_OBJECT);
        boost::optional<std::string> column_name 
            = column.get_optional<std::string>(TableMetadata::Index::Column::NAME);
        if (!column_name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "column name : " << column_name.get() << std::endl;      

        boost::optional<uint16_t> direction 
            = column.get_optional<uint16_t>(TableMetadata::Index::Column::DIRECTION);
        if (!direction) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "direction : " << direction.get() << std::endl;

        // secondary indices
        std::cout << "--- secondary indices ---" << std::endl;
        BOOST_FOREACH (const ptree::value_type& node, table.get_child(TableMetadata::SECONDARY_INDICES_NODDE)) {
            const ptree& secondary_index = node.second;

            boost::optional<std::string> index_name
                = secondary_index.get_optional<std::string>(TableMetadata::Index::NAME);
            if (!index_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "secondary index name : " << index_name.get() << std::endl;

            const ptree& column = secondary_index.get_child(TableMetadata::Index::COLUMN_OBJECT);
            boost::optional<std::string> column_name 
                = column.get_optional<std::string>(TableMetadata::Index::Column::NAME);
            if (!column_name) {
                error = ErrorCode::NOT_FOUND;
                print_error(error, __LINE__);
                return error;
            }
            std::cout << "column name : " << column_name.get() << std::endl;      

            boost::optional<uint16_t> direction 
                = column.get_optional<uint16_t>(TableMetadata::Index::Column::DIRECTION);
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
 *  @biref  main function.
 */
int main(void) 
{
    std::cout << "*** TableMetadta test start. ***" << std::endl << std::endl;

    if (!initialize()) {
        std::cout << "initialization of test environment failed." << std::endl;
        std::cout << "*** TableMetadta test interrupted. ***" << std::endl;
        return 1; 
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

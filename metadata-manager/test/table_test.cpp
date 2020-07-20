#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/exceptions.hpp>
#include <boost/foreach.hpp>
#include <boost/optional.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/object_id.h"
#include "manager/metadata/metadata.h"
//#include "manager/metadata/datatype_metadata.h"
//#include "manager/metadata/table_metadata.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/tables.h"

using namespace manager::metadata;
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
#if 0
/*
 *  @brief  initialize test environment.
 */
bool initialize()
{
    bool success = false;

    // create oid-metadata-table.
    if (ObjectId::init() != ErrorCode::OK) {
        std::cout << "initialization of Tables class failed." << std::endl;
        return success;
    }

    // create table-metadata-table.
    if (Tables::init() != ErrorCode::OK) {
        std::cout << "initialization of Tables class failed." << std::endl;
        return success;
    }

    if (DataTypes::init() != ErrorCode::OK) {
        std::cout << "initialization of DatatypeMetadata failed." << std::endl;
        return success;
    }

    success = true;

    return success;
}
#endif
/*
 *  @biref  display talbe-metadata-object.
 */
ErrorCode display_table_metadata_object(const ptree& table)
{
    ErrorCode error = ErrorCode::OK;

    std::unique_ptr<Metadata> datatypes(new DataTypes(TEST_DB));
    error = datatypes->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    ptree datatype;

    // table metadata
    std::cout << "--- table ---" << std::endl;
    boost::optional<ObjectIdType> id =
        table.get_optional<ObjectIdType>(Tables::ID);
    if (!id) {
        error = ErrorCode::NOT_FOUND;
        print_error(error, __LINE__);
        return error;
    }
    std::cout << "id : " << id.get() << std::endl;

    boost::optional<std::string> name =
        table.get_optional<std::string>(Tables::NAME);
    if (!name) {
        error = ErrorCode::NOT_FOUND;
        print_error(error, __LINE__);
        return error;
    }
    std::cout << "name : " << name << std::endl;

    boost::optional<std::string> table_namespace =
        table.get_optional<std::string>(Tables::NAMESPACE);
    if (!table_namespace) {
        error = ErrorCode::NOT_FOUND;
        print_error(error, __LINE__);
        return error;
    }
    std::cout << "namespace : " << table_namespace.get() << std::endl;

    ptree primary_keys = table.get_child(Tables::PRIMARY_KEY_NODE);
    BOOST_FOREACH (const ptree::value_type& node, primary_keys) {
        std::cout << "primary_key : " << node.second.data() << std::endl;
    }

    // column metadata
    std::cout << "--- columns ---" << std::endl;
    BOOST_FOREACH (const ptree::value_type& node, table.get_child(Tables::COLUMNS_NODE)) {
        const ptree& column = node.second;

        boost::optional<ObjectIdType> id =
            column.get_optional<ObjectIdType>(Tables::Column::ID);
        if (!id) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "id : " << id << std::endl;

        boost::optional<ObjectIdType> table_id =
            column.get_optional<ObjectIdType>(Tables::Column::TABLE_ID);
        if (!table_id) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "table id : " << table_id << std::endl;

        boost::optional<std::string> name =
            column.get_optional<std::string>(Tables::Column::NAME);
        if (!name) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "name : " << name << std::endl;

        boost::optional<uint64_t> ordinal_position =
            column.get_optional<uint64_t>(Tables::Column::ORDINAL_POSITION);
        if (!ordinal_position) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "ordinal position : " << ordinal_position << std::endl;

        boost::optional<ObjectIdType> data_type_id =
            column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
        if (!data_type_id) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "datatype id : " << data_type_id << std::endl;
        datatypes->get(data_type_id.get(), datatype);
        std::cout << "datatype name : "
            << datatype.get<std::string>(DataTypes::NAME) << std::endl;

        boost::optional<uint64_t> data_length =
            column.get_optional<uint64_t>(Tables::Column::DATA_LENGTH);
        if (!data_length) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "data length : " << data_length << std::endl;

        boost::optional<bool> varying =
            column.get_optional<bool>(Tables::Column::VARYING);
        if (!varying) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "varying : " << varying << std::endl;

        boost::optional<bool> nullable =
            column.get_optional<bool>(Tables::Column::NULLABLE);
        if (!nullable) {
            error = ErrorCode::NOT_FOUND;
            print_error(error, __LINE__);
            return error;
        }
        std::cout << "nullable : " << nullable << std::endl;

        boost::optional<std::string> default_expr =
            column.get_optional<std::string>(Tables::Column::DEFAULT);
        if (default_expr) {
            std::cout << "default : " << default_expr << std::endl;
        }

        boost::optional<uint64_t> direction =
            column.get_optional<uint64_t>(Tables::Column::DIRECTION);
        if (direction) {
            std::cout << "direction : " << direction << std::endl;
        }

        std::cout << "---------------" << std::endl;
    }

    return ErrorCode::OK;;
}

/*
 *  @biref  add a table-metadata to metadata-table.
 */
ErrorCode add_table_metadata()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    std::unique_ptr<Metadata> tables(new Tables(TEST_DB));   // use Template-Method.
    error = tables->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    std::unique_ptr<Metadata> datatypes(new DataTypes(TEST_DB));
    error = datatypes->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    ptree datatype;
    ptree new_table;

    //
    // table-metadata
    //
    new_table.put(Tables::NAME, get_tablename());
    new_table.put(Tables::NAMESPACE, "public");

    ptree primary_key;
    ptree primary_keys;
    primary_key.put<uint64_t>("", 1);
    primary_keys.push_back(std::make_pair("", primary_key));
    primary_key.put<uint64_t>("", 2);
    primary_keys.push_back(std::make_pair("", primary_key));
    new_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
    //
    // column-metadata
    //
    ptree columns;
    {
        ptree column;
        // column #1
        column.clear();
        column.put(Tables::Column::NAME, "column_1");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 1);
        datatypes->get(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float4", datatype);
        ObjectIdType data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
        if (!data_type_id) return ErrorCode::NOT_FOUND;
        column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, data_type_id);
        column.put<uint64_t>(Tables::Column::DATA_LENGTH, 1);
        column.put<bool>(Tables::Column::VARYING, false);
        column.put<bool>(Tables::Column::NULLABLE, false);
        column.put(Tables::Column::DEFAULT, "default_expr1");
        column.put<uint64_t>(Tables::Column::DIRECTION, 1);
        columns.push_back(std::make_pair("", column));

        // column #2
        column.clear();
        column.put(Tables::Column::NAME, "column_2");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 2);
        datatypes->get("VARCHAR", datatype);
        data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
        if (!data_type_id) return ErrorCode::NOT_FOUND;
        column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
        column.put<uint64_t>(Tables::Column::DATA_LENGTH, 8);
        column.put<bool>(Tables::Column::VARYING, true);
        column.put<bool>(Tables::Column::NULLABLE, true);
        column.put<uint64_t>(Tables::Column::DIRECTION, 2);
        columns.push_back(std::make_pair("", column));

        // column #3
        column.clear();
        column.put(Tables::Column::NAME, "column_3");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 3);
        datatypes->get("INT64", datatype);
        data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
        if (!data_type_id) return ErrorCode::NOT_FOUND;
        column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
        column.put<uint64_t>(Tables::Column::DATA_LENGTH, 1);
        column.put<bool>(Tables::Column::VARYING, false);
        column.put<bool>(Tables::Column::NULLABLE, true);
        column.put(Tables::Column::DEFAULT, "default_expr2");
        columns.push_back(std::make_pair("", column));
    }
    new_table.add_child(Tables::COLUMNS_NODE, columns);

    //
    // add table-metadata object
    //
    error = tables->add(new_table);
    if ( error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    error = ErrorCode::OK;

    return error;
}

/*
 *  @biref  remove a table-metadata to metadata-table.
 */
ErrorCode remove_table_metadata()
{
    ErrorCode error = ErrorCode::UNKNOWN;
    int TABLE_NUM_ADDED = 4;

    for (int num = 0; num < TABLE_NUM_ADDED; num++) {
        error = add_table_metadata();
        if (error != ErrorCode::OK) {
            print_error(error, __LINE__);
            return error;
        }
    }

    std::unique_ptr<Metadata> tables(new Tables(TEST_DB)); // use Template-Method.
    error = tables->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    //
    // remove table-metadata object
    //
    std::vector<std::string> table_names = {"table_2","table_4","table_1","table_5","table_3"};

    for (std::string name : table_names) {
        uint64_t object_id = 0;
        error = tables->remove(name.c_str(), &object_id);
        if (error != ErrorCode::OK) {
            print_error(error, __LINE__);
            return error;
        } else {
            std::cout << "remove table name :" << name << ", id:" << object_id << std::endl;
        }
    }

    error = add_table_metadata();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    return error;
}

/*
 *  @biref  read table-metadata from metadata-table.
 */
ErrorCode read_table_metadata()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    std::unique_ptr<Metadata> tables(new Tables(TEST_DB));   // use Template-Method.
    error = tables->load();
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    std::cout << "--- table-metadata to read. ---" << std::endl;

    ptree table;
    while ((error = tables->next(table)) == ErrorCode::OK) {
        error = display_table_metadata_object(table);
        if (error != ErrorCode::OK) {
            return error;
        }
        std::cout << std::endl;
    }

    if (error != ErrorCode::END_OF_ROW) {
        print_error(error, __LINE__);
        return error;
    }

    error = ErrorCode::OK;

    return error;
}

/*
 *  @biref  test for Tables class object.
 */
ErrorCode class_object_test()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    try {
        error = add_table_metadata();
    }
    catch (const ptree_error& e) {
        std::cerr << e.what() << std::endl;
        print_error(error, __LINE__);
        return error;
    }
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    try {
        error = read_table_metadata();
    } catch (const ptree_error& e) {
        std::cerr << e.what() << std::endl;
        print_error(error, __LINE__);
        return error;
    }
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    error = ErrorCode::OK;

    return error;
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

    error = Tables::load(TEST_DB, root);
    if (error != ErrorCode::OK) {
        print_error(error, __LINE__);
        return error;
    }

    try {
        BOOST_FOREACH (const ptree::value_type& node,
            root.get_child(Tables::TABLES_NODE)) {
                const ptree& table = node.second;
                error = display_table_metadata_object(table);
                if (error != ErrorCode::OK) {
                    return error;
                }
                std::cout << std::endl;
            }
    } catch(const ptree_error& e) {
        std::cerr << e.what() << std::endl;
        return error;
    }

    return ErrorCode::OK;
}

/*
 *  @biref  main function.
 */
int main(void)
{
    std::cout << "*** TableMetadta test start. ***" << std::endl << std::endl;
#if 0
    if (!initialize()) {
        std::cout << "initialization of test environment failed." << std::endl;
        std::cout << "*** TableMetadta test interrupted. ***" << std::endl;
        return 1;
    }
#endif
    std::cout << "=== class object test start. ===" << std::endl;
    ErrorCode class_object_test_error = class_object_test();
    std::cout << "=== class object test done. ===" << std::endl;
    std::cout << std::endl;

    std::cout << "=== static functions test start. ===" << std::endl;;
    ErrorCode static_functions_test_error = static_functions_test();
    std::cout << "=== static functions test done. ===" << std::endl;;
    std::cout << std::endl;

    std::cout << "=== remove table functions test start. ===" << std::endl;
    ErrorCode remove_table_test_error = remove_table_metadata();
    std::cout << "=== remove table functions test done. ===" << std::endl;
    std::cout << std::endl;

    std::cout << "class object test           : ";
    if (class_object_test_error == ErrorCode::OK) {
        std::cout << "Success" << std::endl;
    } else {
        std::cout << "*** Failure ***" << std::endl;
    }

    std::cout << "static functions test       : ";
    if (static_functions_test_error == ErrorCode::OK) {
        std::cout << "Success" << std::endl;
    } else {
        std::cout << "*** Failure ***" << std::endl;
    }

    std::cout << "remove table functions test : ";
    if (remove_table_test_error == ErrorCode::OK) {
        std::cout << "Success" << std::endl;
    } else {
        std::cout << "*** Failure ***" << std::endl;
    }
    std::cout << std::endl;

    std::cout << "*** TableMetadta test completed. ***" << std::endl;;

    return 0;
}

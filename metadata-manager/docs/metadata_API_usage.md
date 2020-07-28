
# クライアントにおけるメタデータ処理

2020.04.03 NEC

## テーブルメタデータを統合メタデータ管理基盤に新規作成する

1. tableメタデータオブジェクトを作成する
1. rootオブジェクトにtableメタデータオブジェクトを追加する
1. Tables::save()で統合メタデータ管理基盤にテーブルメタデータを保存する

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力して、tableメタデータオブジェクトを作成する
```c++
    boost::property_tree::ptree new_table;  // tableメタデータオブジェクト
    new_table.put(Tables::NAME, "sample table");
```

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する（配列・基本データ型）
```c++
boost::property_tree::ptree primary_keys;
{
    // 子ノードの作成
    boost::property_tree::ptree primary_key;
    // 配列要素１
    primary_key.put<uint64_t>("", 1);
    primary_keys.push_back(std::make_pair("", primary_key));    // 配列に追加
    // 配列要素２
    primary_key.put<uint64_t>("", 2);
    primary_keys.push_back(std::make_pair("", primary_key));    // 配列に追加
}
// 親ノードに子ノードを追加
new_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
```

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する（配列・オブジェクト）
```c++
    boost::property_tree::ptree columns;
    {
        // 子ノードの作成
        boost::property_tree::ptree column;
        // 配列要素１
        column.put(Tables::Column::NAME, "column_1");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 1);
        columns.push_back(std::make_pair("", column));  // 配列に追加
        // 配列要素２
        column.put(Tables::Column::NAME, "column_2");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 2);
        columns.push_back(std::make_pair("", column));  // 配列に追加
    }
    // 親ノードに子ノードを追加
     new_table.add_child(Tables::COLUMNS_NODE, columns);
```

dataTypeIdの取得方法
```c++
    // データ型メタデータオブジェクトの生成
    std::unique_ptr<Metadata> datatypes(new DataTypes("データベース名"));
    if (datatypes->load() != ErrorCode::OK) {
        エラー処理
    }
    // データ型名を指定してDataTypeメタデータオブジェクトを取得する
    boost::property_tree::ptree datatype;   // DataTypeメタデータオブジェクト
    if (datatypes->get("INT64", datatype) != ErrorCode::OK) {
        エラー処理
    }
    // データ型IDの取得
    boost::optional<ObjectIdType> data_type_id = 
        datatype.get_optional<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
        エラー処理
    }
    column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, data_type_id);
```

rootオブジェクトにtableメタデータオブジェクトを追加する
```c++
    boost::property_tree::ptree root;
    root.add_child(Tables::TABLES_NODE, new_table);
```

Tables::save()で統合メタデータ管理基盤にテーブルメタデータを保存する
```c++
    if (Tables::save("データベース名", root) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

---

## テーブルメタデータを統合メタデータ管理基盤に追加する

1. Tablesオブジェクトを作成する
1. Tablesオブジェクトに統合メタデータ管理基盤上のメタデータを読み込む
1. boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する
1. Tables::add()でテーブルメタデータをTsurugiに追加する（永続化もされる）
 
Tablesオブジェクトを作成する
```c++
    // Template-Methodパターンを使用しているため親クラス(Metadataクラス)のポインタを使用する
    // V1では、データベース名は任意の文字列で可
    std::unique_ptr<Metadata> tables(new Tables("データベース名"));
```

Tablesオブジェクトに統合メタデータ管理基盤上のメタデータを読み込む
```c++
    if ( tables->load() != metadata::ErrorCode::OK) {
        エラー処理
    }
```

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する
```c++
    boost::property_tree::ptree new_table;
    new_table.put(Tables::NAME, "sample table");
```

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する（配列・基本データ型）
```c++
    boost::property_tree::ptree primary_keys;
    {
        // 子ノードの作成
        boost::property_tree::ptree primary_key;
        // 配列要素１
        primary_key.put<uint64_t>("", 1);
        primary_keys.push_back(std::make_pair("", primary_key));    // 配列に追加
        // 配列要素２
        primary_key.put<uint64_t>("", 2);
        primary_keys.push_back(std::make_pair("", primary_key));    // 配列に追加
    }
    // 親ノードに子ノードを追加
    new_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
```

boost::property_tree::ptreeオブジェクトにテーブルメタデータを入力する（配列・オブジェクト）
```c++
    boost::property_tree::ptree columns;
    {
        // 子ノードの作成
        boost::property_tree::ptree column;
        // 配列要素１
        column.put(Tables::Column::NAME, "column_1");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 1);
        columns.push_back(std::make_pair("", column));  // 配列に追加
        // 配列要素２
        column.put(Tables::Column::NAME, "column_2");
        column.put<uint64_t>(Tables::Column::ORDINAL_POSITION, 2);
        columns.push_back(std::make_pair("", column));  // 配列に追加
    }
    // 親ノードに子ノードを追加
     new_table.add_child(Tables::COLUMNS_NODE, columns);
```

Tables::add()でテーブルメタデータをTsurugiに追加する（永続化もされる）
```c++
    if (tables.add(new_table) != metadata::ErrorCode::OK) {
        エラー処理
    }

```

---
## テーブルメタデータを統合メタデータ管理基盤から読み込む #1

1. Tables::load()で統合メタデータ管理基盤上のメタデータを読み込む
1. ptreeオブジェクトからテーブルメタデータを取得する

Tables::load()で統合メタデータ管理基盤上のメタデータを読み込む
```c++
boost::property_tree::ptree root;
metadata::ErrorCode error = Tables::load("データベース名", root);
if (error != metadata::ErrorCode::OK) {
    エラー処理
}
```

ptreeオブジェクトからテーブルメタデータを取得する
```c++
#include <boost/foreach.hpp>

boost::property_tree::ptree tables = root.get_child(Tables::TABLES_NODE);
BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, tables) {
    const boost::property_tree::ptree& table = node.second;
    // 値
    boost::optional<std::string> name = 
        table.get_optional<std::string>(Tables::NAME);
    if (!name) {
        エラー処理
    }

    // 配列（基本データ型）
    boost::property_tree::ptree primary_keys = table.get_child(Tables::PRIMARY_KEY_NODE);
    BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, primary_keys) {
        const boost::property_tree::ptree& value = node.second;
        boost::optional<uint64_t> primary_key = value.get_value_optional<uint64_t>();
    }

    // 配列（オブジェクト）
    boost::property_tree::ptree columns = table.get_child(Tables::COLUMNS_NODE);
    BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, columns) {
        const boost::property_tree::ptree& column = node.second;
        // columnメタデータオブジェクトから各メタデータを取得する
        boost::optional<std::string> name = 
            column.get_optional<std::string>(Tables::Column::NAME);
        if (!name) {
            エラー処理
        }
    }
}
```

---

### テーブルメタデータを統合メタデータ管理基盤から読み込む #2

1. Tablesオブジェクトを作成する
1. Tablesオブジェクトに統合メタデータ管理基盤上のメタデータを読み込む
1. Tablesオブジェクトからtableメタデータオブジェクトを読み込む
1. テーブルメタデータオブジェクトからテーブルメタデータを取得する

Tablesオブジェクトを作成する
```c++
    // Template-Methodパターンを使用しているため親クラス(Metadataクラス)のポインタを使用する
    // V1では、データベース名は任意の文字列で可
    std::unique_ptr<Metadata> tables(new Tables("データベース名"));
```

TablesオブジェクトにTsurugiのメタデータを読み込む
```c++
    if ( tables->load() != metadata::ErrorCode::OK) {
        エラー処理
    }
```

TablesオブジェクトからTableメタデータオブジェクトを読み込む
```c++
    boost::property_tree::ptree table
    metadata::ErrorCode error = tables->next(table);
    if (error != metadata::ErrorCode::OK) {
        if (error != metadata::ErrorCode::END_OF_ROW) {
            処理対象業が存在しない状態が期待通りであれば、処理を正常終了する。
        }
        エラー処理
    }
 
    boost::optional<std::string> name = 
        table.get_optional<std::string>(Tables::NAME);
    if (!name) {
        エラー処理
    }
    std::cout << name.get() << std::endl;
```
複数行の場合
```c++
    while ((error = tables->next(table)) == metadata::ErrorCode::OK) {
        boost::optional<std::string> name = 
            table.get_optional<std::string>(Tables::NAME);
        if (!name) {
            エラー処理
        }
    }  
    if (error_code != END_OF_ROW) {
        エラー処理
    }
```

以上

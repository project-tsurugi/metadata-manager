
# クライアントにおけるメタデータ処理

2020.04.03 NEC  
2022.10.28 NEC  

## 事前準備

ファイルの冒頭で次のコードを記載し、名前空間を省略した呼び出しができるようにする。

```c++
using namespace manager::metadata;
```

### テーブルメタデータを統合メタデータ管理基盤に新規追加する

1. テーブルメタデータオブジェクトを作成する
1. テーブルメタデータオブジェクトにカラムメタデータを追加する
1. テーブルメタデータオブジェクトを統合メタデータ管理基盤のテーブルメタデータに追加する

テーブルメタデータを入力して、テーブルメタデータオブジェクトを作成する

```c++
    boost::property_tree::ptree new_table;  // テーブルメタデータオブジェクト
    new_table.put(Tables::NAME, "sample table");
```

カラムメタデータをカラムメタデータオブジェクトに入力して、テーブルメタデータオブジェクトに追加する

```c++
    boost::property_tree::ptree columns;    // カラムメタデータオブジェクト
    {
        // 子ノードの作成
        boost::property_tree::ptree column;
        // 配列要素１
        column.put(Tables::Column::NAME, "column_1");
        column.put<uint64_t>(Tables::Column::COLUMN_NUMBER, 1);
        columns.push_back(std::make_pair("", column));  // 配列に追加
        // 配列要素２
        column.put(Tables::Column::NAME, "column_2");
        column.put<uint64_t>(Tables::Column::COLUMN_NUMBER, 2);
        columns.push_back(std::make_pair("", column));  // 配列に追加
    }
    // テーブルメタデータオブジェクトにカラムメタデータオブジェクトを追加
     new_table.add_child(Tables::COLUMNS_NODE, columns);
```

カラムメタデータに追加するデータ型IDの取得方法

```c++
    // データ型メタデータを読み込む
    auto datatypes = std::make_unique<metadata::DataTypes>("データベース名");
    boost::property_tree::ptree datatype;   // データ型メタデータオブジェクト
    // データ型名を指定して、DataTypeメタデータオブジェクトを取得する
    if (datatypes->get("INT64", datatype) != ErrorCode::OK) {
        エラー処理
    }
    // データ型IDの取得
    boost::optional<ObjectIdType> data_type_id = 
        datatype.get_optional<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
        エラー処理
    }
    // カラムメタデータオブジェクトの配列要素にデータ型IDを追加
    column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, data_type_id);
```

統合メタデータ管理基盤のテーブルメタデータにテーブルメタデータオブジェクトを追加する

```c++
    ObjectId object_id = metadata::INVALID_OBJECT_ID;
    auto tables = metadata::get_tables_ptr("データベース名");
    if (tables->add(new_table, &object_id) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

### 統合メタデータ管理基盤のテーブルメタデータからテーブルメタデータを読み込む

1. テーブルメタデータオブジェクトを読み込む
1. すべてのテーブルメタデータオブジェクトを読み込む

テーブル名を指定して、統合メタデータ管理基盤のテーブルメタデータからテーブルメタデータオブジェクトを読み込む

```c++
    boost::property_tree::ptree get_table;  // テーブルメタデータオブジェクト
    auto tables = metadata::get_tables_ptr("データベース名");
    if (tables->get("テーブル名", get_table) != metadata::ErrorCode::OK) {
        エラー処理
    }
    auto name = get_table.get_optional<std::string>(Tables::NAME);
```

統合メタデータ管理基盤のテーブルメタデータからすべてのテーブルメタデータオブジェクトを読み込む

```c++
    std::vector<boost::property_tree::ptree> table_elements;  // テーブルメタデータオブジェクトの配列
    auto tables = metadata::get_tables_ptr("データベース名");
    if (tables->get_all(table_elements) != metadata::ErrorCode::OK) {
        エラー処理
    }
    boost::property_tree::ptree get_table; テーブルメタデータオブジェクト
    for (get_table : table_elements) {
        auto name = get_table.get_optional<std::string>(Tables::NAME);
    }
```

### Primary Keyが設定されたカラム（メタデータオブジェクト）を特定する

1. Primary Keyを有するテーブルメタデータオブジェクトを読み込む
1. TypeにPRIMARY_KEYが設定された制約メタデータオブジェクトを検索する
1. 制約メタデータオブジェクトからカラムメタデータオブジェクトを読み込む

テーブル名を指定して、Primary Keyを有するテーブルメタデータオブジェクトを読み込む

```c++
    auto tables = metadata::get_tables_ptr("データベース名");
    boost::property_tree::ptree table_primary;  // テーブルメタデータオブジェクト
    if (tables->get("テーブル名", table_primary) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

読み込んだテーブルメタデータオブジェクトから、TypeにPRIMARY_KEYが設定された制約メタデータオブジェクトを検索する

```c++
    BOOST_FOREACH (const auto& constraint_node, table_primary.get_child(metadata::Tables::CONSTRAINTS_NODE)) {
        auto& constraint = constraint_node.second;
        auto constraint_type = constraint.get_optional<int64_t>(metadata::Constraint::TYPE);
        // TypeがPRIMARY_KEYか否か
        if (static_cast<manager::metadata::Constraint::ConstraintType>(constraint_type.value()) ==
                    metadata::Constraint::ConstraintType::PRIMARY_KEY) {
            // Primary Keyが設定されたカラムメタデータオブジェクトのIDを読み込む
            std::vector<ObjectId> pk_columns_id;
            BOOST_FOREACH (const auto& column_id_node, constraint.get_child(metadata::Constraint::COLUMNS_ID)) {
                auto& constraint_column_id = column_id_node.second;
                auto column_id = constraint_column_id.get_value_optional<ObjectId>();
                pk_columns_id.emplace_back(column_id.value());
            }
        }
    }
```

### 制約メタデータオブジェクトを追加（テーブルメタデータオブジェクトを更新）する

1. 制約メタデータオブジェクトを追加するテーブルメタデータオブジェクトを読み込む
1. 制約メタデータオブジェクトをテーブルメタデータオブジェクトに追加する
1. 統合メタデータ管理基盤のテーブルメタデータオブジェクトを更新する

テーブル名を指定して、制約メタデータオブジェクトが属するテーブルメタデータオブジェクトを読み込む

```c++
    auto tables = metadata::get_tables_ptr("データベース名");
    boost::property_tree::ptree update_table;  // テーブルメタデータオブジェクト
    if (tables->get("更新するテーブル名", update_table) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

制約メタデータを制約メタデータオブジェクトに入力して、テーブルメタデータオブジェクトに追加する

```c++
    boost::property_tree::ptree constraints;    // 制約メタデータオブジェクト
    {
        // 子ノードの作成
        boost::property_tree::ptree constraint;
        // 配列要素１
        constraint.put(Constraint::NAME, "constr_1");
        constraint.push_back(std::make_pair("", constraint));  // 配列に追加
        // 配列要素２
        constraint.put(Constraint::NAME, "constr_2");
        constraint.push_back(std::make_pair("", constraint));  // 配列に追加
    }
    // テーブルメタデータオブジェクトに制約メタデータオブジェクトを追加
     update_table.add_child(Tables::CONSTRAINTS_NODE, constraints);
```

制約メタデータオブジェクトを追加した統合メタデータ管理基盤のテーブルメタデータを更新する

```c++
    auto update_id = update_table.get_optional<ObjectIdType>(metadata::Table::ID);
    // テーブルメタデータオブジェクトを更新する
    if (tables->update(update_id.get(), update_table) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

### テーブルメタデータオブジェクトに属するインデックスメタデータを削除する

1. すべてのインデックスメタデータオブジェクトを読み込む
1. 削除対象のインデックスメタデータをテーブルIDから特定して削除する

統合メタデータ管理基盤のインデックスメタデータからすべてのインデックスメタデータオブジェクトを読み込む

```c++
    std::vector<boost::property_tree::ptree> index_elements;  // インデックスメタデータオブジェクトの配列
    auto indexes = metadata::get_indexes_ptr("データベース名");
    // すべてのインデックスメタデータオブジェクトを読み込む
    if (indexes->get_all(index_elements) != metadata::ErrorCode::OK) {
        エラー処理
    }
```

すべてのインデックスメタデータオブジェクトの中から削除対象のインデックスメタデータをテーブルIDから特定して削除する

```c++
    boost::property_tree::ptree remove_index; インデックスメタデータオブジェクト
    for (remove_index : index_elements) {
        auto remove_id = remove_index.get_optional<ObjectIdType>(Indexes::ID);
        // テーブルIDが一致するインデックスメタデータオブジェクトを特定する
        if (table_id.get() == remove_id.get()) {
            // インデックスメタデータオブジェクトを削除する
            if (indexes->remove(remove_id.get()) != metadata::ErrorCode::OK) {
                エラー処理
            }
        }        
    }
```

以上

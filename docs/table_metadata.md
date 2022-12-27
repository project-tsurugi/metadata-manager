
# Table Metadata

2020.04.03 NEC  
2022.10.28 NEC  
2022.12.28 NEC  

## Metadata::Tablesクラス

テーブル、カラム、制約に関するメタデータを管理する。

### includeファイル

* include/manager/metadata/tables.h
* include/manager/metadata/constraints.h
* include/manager/metadata/metadata.h
* include/manager/metadata/metadata_factory.h

### メソッド

* get_tables_ptr()
* add()
* get()
* update()
* remove()
* exists()
* get_all()

### Tableメタデータ(root)  

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
|"format_version" | int64_t               | データ形式フォーマットバージョン | "1"固定 |
|"generation"     | int64_t               | メタデータの世代 | "1"固定 |
|"tables"         | Tableの配列  | [Tableメタデータオブジェクト](#tableメタデータオブジェクト)  | - |

### Tableメタデータオブジェクト

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
| "id"                | ObjectId    | テーブルID | - |
| "name"              | std::string | テーブル名 | - |
| "namespace"         | std::string | スキーマ名 | - |
| "owner_id"          | ObjectId    | 所有者ID | - |
| "acl"               | std::string | アクセス権限 | - |
| "number_of_tuples"  | int64_t     | 統計情報で使用する行(live raw)数 | - |
| "columns"           | Columnの配列 | テーブルに属するカラム（Columnメタデータオブジェクト）のリスト | [Columnメタデータオブジェクト](#columnメタデータオブジェクト)を参照  |
| "indexes"           | Indexの配列 | テーブルに属するインデックス（Indexメタデータオブジェクト）のリスト | インデックスメタデータを参照(別途) |
| "constraints"       | Constraintの配列 | テーブルに属する制約（Constraintメタデータオブジェクト）のリスト | [Constraintメタデータオブジェクト](#constraintメタデータオブジェクト)を参照 |

### Columnメタデータオブジェクト

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
| "id"                  | ObjectId    | カラムID | - |
| "name"                | std::string | カラム名  | - |
| "table_id"            | ObjectId    | カラムが属するテーブルID | - |
| "column_number"       | int64_t     | カラム番号 | カラム番号は1から始まる。 |
| "data_type_id"        | ObjectId    | カラムのデータ型のID | DataType Metadata データ型ID一覧を参照(別途) |
| "data_length"         | int64_tの配列 | データ型のオプションを格納するリスト | DataType Metadata データ型オプションを参照(別途) |
| "varying"             | bool        | 文字列長の可変有無 | varchar [ (n) ](またはcharacter varying [ (n) ])の場合、true。<br>char [ (n) ](またはcharacter [ (n) ])の場合、false。<br>それ以外の場合、keyを作成しない。 |
| "is_not_null"         | bool        | 非NULL制約の有無 | このカラムが非NULL制約または主キー制約の場合、true。<br>それ以外の場合、false。|
| "default_expression"  | std::string | デフォルト制約のデフォルトデータ値 | デフォルト制約がある場合のみ存在。 |

## Constraintメタデータオブジェクト

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
| "id"                    | ObjectId    | 制約ID | - |
| "name"                  | std::string | 制約名 | 制約名がある場合のみ存在。 |
| "table_id"              | ObjectId    | 制約が属するテーブルID | - |
| "type"                  | int64_t     | 制約種別 | [ConstraintType](#constrainttype) を参照 |
| "columns"               | int64_tの配列 | 制約の対象となるカラム番号のリスト | - |
| "columns_id"            | ObjectIdの配列 | 制約の対象となるカラムIDのリスト | - |
| "index_id"              | ObjectId    | インデックスID | 主キー制約、一意性制約の場合のみ存在。 |
| "expression"            | std::string | 検査制約の検査式 | 検査制約の場合のみ存在。 |
| "pk_table"              | std::string | 外部キー制約の被参照テーブル名 | 外部キー制約の場合のみ存在。 |
| "pk_columns"            | int64_tの配列 | 外部キー制約の被参照カラム番号のリスト | 外部キー制約の場合のみ存在。 |
| "pk_columns_id"         | int64_tの配列 | 外部キー制約の被参照カラムIDのリスト | 外部キー制約の場合のみ存在。 |
| "fk_match_type"         | int64_t     | 外部キー制約における被参照行の照合タイプ | 外部キー制約の場合のみ存在。<br>[MatchType](#matchtype) を参照 |
| "fk_delete_action"      | int64_t     | 外部キー制約における被参照行の削除アクション | 外部キー制約の場合のみ存在。<br>[ActionType](#actiontype) を参照 |
| "fk_update_action"      | int64_t     | 外部キー制約における被参照行の更新アクション | 外部キー制約の場合のみ存在。<br>[ActionType](#actiontype) を参照 |

### ConstraintType

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0 | ConstraintType::PRIMARY_KEY | 主キー制約 | - |
  | 1 | ConstraintType::UNIQUE      | 一意性制約 | - |
  | 2 | ConstraintType::CHECK       | 検査制約 | - |
  | 3 | ConstraintType::FOREIGN_KEY | 外部キー制約 | - |
  | 4 | ConstraintType::TRIGGER     | 制約トリガ | Tsurugiでは非サポート |
  | 5 | ConstraintType::EXCLUDE     | 排他制約 | Tsurugiでは非サポート |

### MatchType

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0 | MatchType::SIMPLE   | MATCH SIMPLE | - |
  | 1 | MatchType::FULL     | MATCH FULL | - |
  | 2 | MatchType::PARTIAL  | MATCH PARTIAL | - |

### ActionType

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0 | ActionType::NO_ACTION   | NO ACTION | - |
  | 1 | ActionType::RESTRICT    | RESTRICT | - |
  | 2 | ActionType::CASCADE     | CASCADE | Tsurugiでは非サポート |
  | 3 | ActionType::SET_NULL    | SET NULL | - |
  | 4 | ActionType::SET_DEFAULT | SET DEFAULT | - |

以上

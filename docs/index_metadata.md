
# Index Metadata

2022.10.28 NEC  

## Metadata::Indexesクラス

インデックスに関するメタデータを管理する。

### includeファイル

* include/manager/metadata/index.h
* include/manager/metadata/indexes.h
* include/manager/metadata/metadata.h
* include/manager/metadata/metadata_factory.h

### メソッド

* get_indexes_ptr()
* add()
* get()
* update()
* remove()
* exists()
* get_all()

### Indexメタデータ(root)  

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
|"format_version" | int64_t               | データ形式フォーマットバージョン | "1"固定 |
|"generation"     | int64_t               | メタデータの世代 | "1"固定 |
|"indexes"         | std::vector\<Index\>  | [Indexメタデータオブジェクト](#indexメタデータオブジェクト)  | - |

### Indexメタデータオブジェクト

| 名前 | 型 | 説明 | 備考 |
|----|----|----|----|
| "id"                    | ObjectId    | インデックスID | - |
| "name"                  | std::string | インデックス名 | - |
| "namespace"             | std::string | スキーマ名 | - |
| "owner_id"              | ObjectId    | 所有者ID | - |
| "acl"                   | std::string | アクセス権限 | - |
| "table_id"              | ObjectId    | インデックスが属するテーブルID | - |
| "access_method"         | int64_t     | 使用するアクセスメソッド | [AccessMethod](#accessmethod) を参照 |
| "is_unique"             | bool        | 一意性制約の有無 | このインデックスが一意性制約または主キー制約の場合、true。<br>それ以外の場合、false。 |
| "is_primary"            | bool        | 主キー制約の有無 | このインデックスが主キー制約の場合、true。<br>それ以外の場合、false。 |
| "number_of_column"      | int64_t   | 非キー（INCLUDEカラム）を含むカラム数 | - |
| "number_of_key_column"  | int64_t   | 非キー（INCLUDEカラム）を除いたカラム数 | - |
| "columns"               | std::vector<int64_t> | インデックスの対象となるカラム番号のリスト | このリストには非キー（INCLUDEカラム）を含む。<br>リストへの登録順番は、キー＋非キーとなる。 |
| "columns_id"               | std::vector\<ObjectId\> | インデックスの対象となるカラムIDのリスト | このリストには非キー（INCLUDEカラム）を含む。<br>リストへの登録順番は、キー＋非キーとなる。 |
| "options"               | std::vector<int64_t> | カラム毎のオプションを格納するリスト | [ColumnOption](#columnoption) を参照 |

### AccessMethod

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0 | AccessMethod::DEFAULT    | デフォルト | - |
  | 1 | AccessMethod::MASS_TREE  | mass-tree | - |

### ColumnOption

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0 | Direction::ASC_NULLS_LAST  | ASC and NULLS LAST | - |
  | 1 | Direction::ASC_NULLS_FIRST  | ASC and NULLS FIRST | - |
  | 2 | Direction::DESC_NULLS_LAST | DESC and NULLS LAST | - |
  | 3 | Direction::DESC_NULLS_FIRST  | DESC and NULLS FIRST | - |

以上

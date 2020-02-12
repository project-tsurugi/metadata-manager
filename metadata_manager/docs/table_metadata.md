
# V1 テーブルメタデータ データ形式(rev.0.3)

2020.02.10 NEC

* '*'　:　メタデータ登録時に必須の項目
* '+'　:　メタデータ登録時に入力可能な項目
* '-'　:　統合メタデータ管理基盤が値を付与する項目

```
// Table情報メタデータ(root)
{
    "format_version" : number,          [-] // データ形式フォーマットバージョン ※V1は"1"固定
    "generation"     : number,          [-] // メタデータの世代 ※V1は"1"固定
    "tables"         : array[object]    [*] // Tableメタデータオブジェクト
}

// Tableメタデータオブジェクト
{
    "id"                : number,           [-] // テーブルID
    "name"              : string,           [*] // テーブル名
    "namespace"         : string,           [*] // 名前空間（テーブル名を除く）
    "columns"           : array[object],    [*] // カラムメタデータオブジェクト
    "primary_idnex"     : object,           [*] // Indexメタデータオブジェクト（Primary Index）
    "secondary_indices" : array[object],    [*] // Indexメタデータオブジェクト（Secondary Indices）
    "constraints"       : array[object]     [+] // Constraintメタデータオブジェクト（テーブル制約）
}

// Columnメタデータオブジェクト
{
    "id"                : number,           [-] // カラムID
    "table_id"          : number,           [-] // カラムが属するテーブルのID
    "name"              : string,           [*] // カラム名
    "ordinal_position"  : number,           [*] // カラム番号(1 origin)
    "datatype_id"       : number,           [*] // カラムのデータ型のID
                                                // データタイプメタデータを参照(別途)
    "datatype_name"     : string,           [*] // カラムのデータ型名
                                                // "datatype_id" と "datatype_name" は選択必須
    "data_length"       : array[number],    [+] // データ長(配列長)
                                                // varchar(20)など ※V1では未使用
    "nullable"          : bool,             [*] // NOT NULL制約の有無
    "constraints"       : array             [+] // Constraintメタデータオブジェクト（カラム制約）
}

// Constraintメタデータオブジェクト（カラム制約、テーブル制約共通）
{
    "id"            : number,           [-] // 制約ID
    "table_id"      : number,           [-] // 制約が属するテーブルのID
                                            // カラム制約の場合は"0"
    "column_key"    : array[number],    [*] // 制約が属するカラムの"ordinal_position"
    "name"          : string,           [+] // 制約名
    "type"          : string,           [*] // 制約の種類
                                            // PostgreSQLのメタデータに合わせる
                                            //  c = 検査制約, f = 外部キー制約,
                                            //  p = 主キー制約, u = 一意性制約,
                                            //  t = 制約トリガ, x = 排他制約
    "contents"      : string            [+] // 制約の補足情報（式など）
}

// Indexメタデータオブジェクト
{
    "name"          : string,       [*] // Index名
    "column" : {
        "name"      : string,       [*] // カラム名
        "direction" : number        [*] // 方向（0: ASCENDANT, 1: DESCENDANT）
    }
}
```

```JSON
// DataType情報メタデータ(root)   ※検討中
{
    "format_version"    : number,       // データ形式フォーマットバージョン
    "generation"        : number,       // メタデータの世代
    "datatypes"        : array[object] // DataTypeメタデータオブジェクト
}

// DataTypeメタデータオブジェクト
{
    "id"            : number,   // データ型ID
    "name"          : string,   // データ型名
    "pg_datatype"   : number    // 対応するPostgreSQLのデータ型のOID
}
```

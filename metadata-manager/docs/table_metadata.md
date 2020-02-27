
# Table Metadata (rev.0.5a)

2020.02.27 NEC

## TableMetadataクラス
テーブル、カラム、制約に関するメタデータを管理する。

### メソッド
* static load()
* load()
* add()
* get()
* set()
* remove()
* next()

### メタデータフォーマット

boost::property_tree::ptreeに格納されるメタデータのフォーマット。

※フォーマットはJSON Schemaに置換予定。

* '*'　:　メタデータ登録時に必須の項目
* '+'　:　メタデータ登録時に入力可能な項目
* '-'　:　統合メタデータ管理基盤が値を付与する項目

```
// Tableメタデータ(root)
{
    "formatVersion"  : number,          [-] // データ形式フォーマットバージョン ※V1は"1"固定
    "generation"     : number,          [-] // メタデータの世代 ※V1は"1"固定
    "tables"         : array[object]    [*] // Tableメタデータオブジェクト
}

// Tableメタデータオブジェクト
{
    "id"                : number,           [-] // テーブルID
    "name"              : string,           [*] // テーブル名
    "namespace"         : string,           [*] // 名前空間（テーブル名を除く）
    "columns"           : array[object],    [*] // カラムメタデータオブジェクト
    "primaryIndex"      : object,           [*] // Indexメタデータオブジェクト（Primary Index）
    "secondaryIndices"  : array[object],    [*] // Indexメタデータオブジェクト（Secondary Indices）
    "tableConstraints"  : array[object]     [+] // Constraintメタデータオブジェクト（テーブル制約）
}

// Columnメタデータオブジェクト
{
    "id"                : number,           [-] // カラムID
    "tableId"           : number,           [-] // カラムが属するテーブルのID
    "name"              : string,           [*] // カラム名
    "ordinalPosition"   : number,           [*] // カラム番号(1 origin)
    "dataTypeId"        : number,           [*] // カラムのデータ型のID
                                                // データタイプメタデータを参照(別途)
    "dataLength"        : array[number],    [+] // データ長(配列長)
                                                // varchar(20)など ※V1では未使用
                                                // NUMERIC(precision,scale)を考慮してarray[number] にしている。
                                                // array[number] か number かは継続して検討。
    "nullable"          : bool,             [*] // NOT NULL制約の有無
    "default"           : string            [+] // デフォルト式
}

// Constraintメタデータオブジェクト
{
    "id"            : number,           [-] // 制約ID
    "tableId"       : number,           [-] // 制約が属するテーブルのID
    "columnKey"     : array[number],    [*] // 制約が属するカラムの"ordinal_position"
    "name"          : string,           [+] // 制約名
    "type"          : string,           [*] // 制約の種類
                                                "PRIMARY KEY" : 主キー制約
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

以上

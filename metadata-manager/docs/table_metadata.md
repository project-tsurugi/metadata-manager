
# Table Metadata (rev.0.7)

2020.04.03 NEC

## Metadata::Tablesクラス
テーブル、カラム、制約に関するメタデータを管理する。

### includeファイル
* include/manager/metadata/Tables.h

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
    "namespace"         : string,           [+] // 名前空間（テーブル名を除く）
    "columns"           : array[object],    [*] // カラムメタデータオブジェクト
    "primaryKey"        : array[number]     [*] // primaryKeyカラムの"ordinal_position"
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
    "direction"         : number            [+] // 方向（0: DEFAULT, 1: ASCENDANT, 2: DESCENDANT）
}
```

以上

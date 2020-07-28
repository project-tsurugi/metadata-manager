
# DataType Metadata (rev.0.2)

2020.04.03 NEC

## Metadata::DataTypesクラス
Tsurugiで使用するデータ型に関するメタデータを管理する。

### includeファイル
* include/manager/metadata/Datatypes.h

### メソッド
* static load()
* load()
* add()
* get()
* set()
* remove()
* next()

### メタデータフォーマット

※フォーマットはJSON Schemaに置換予定。

* '*'　:　メタデータ登録時に必須の項目
* '+'　:　メタデータ登録時に入力可能な項目
* '-'　:　統合メタデータ管理基盤が値を付与する項目

```
// DataTypeメタデータ(root)
{
    "formatVersion"    : number,       // データ形式フォーマットバージョン
    "generation"       : number,       // メタデータの世代
    "dataTypes"        : array[object] // DataTypeメタデータオブジェクト
}

// DataTypeメタデータオブジェクト
{
    "id"                       : number, // データ型ID
    "name"                     : string, // データ型名
    "pg_dataType"              : number, // 対応するPostgreSQLのデータ型のOID
    "pg_dataTypeName"          : string, // ユーザーが入力するPostgreSQLの型名
    "pg_dataTypeQualifiedName" : string  // PostgreSQL内部の修飾型名
}
```

以上

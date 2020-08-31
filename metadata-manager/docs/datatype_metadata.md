
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

#### pg_dataTypeQualifiedNameを追加する理由
* PostgreSQLのCreateStmtクエリツリーから取得できる型名が次の表になっており、型変換を実施するため。

|SQLで指定したPostgreSQLの型|CreateStmtクエリツリーから取得できる型([...,...]は配列を示す)|ogawayamaの型|
|----|----|----|
|SMALLINT|[pg_catalog,int2] **xor** int2のoid|INT16 |
|INTEGER|[pg_catalog,int4] **xor** int4のoid|INT32 |
|BIGINT|[pg_catalog,int8] **xor** int8のoid|INT64 |
|REAL|[pg_catalog,float4] **xor** float4のoid|FLOAT32 |
|DOUBLE PRECISION|[pg_catalog,float8] **xor** float8のoid|FLOAT64 |
|TEXT|[text] **xor** textのoid|TEXT |
|CHAR[(n)],CHARACTER[(n)]|([pg_catalog,bpchar] **xor** bpcharのoid) |CHAR|
|VARCHAR[(n)],CHARACTER VARYING[(n)]|([pg_catalog,varchar] **xor** varcharのoid) |VARCHAR|

以上

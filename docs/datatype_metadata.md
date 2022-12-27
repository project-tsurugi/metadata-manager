
# DataType Metadata

2020.04.03 NEC  
2022.10.28 NEC  
2022.12.28 NEC  

## Metadata::DataTypesクラス

Tsurugiで使用するデータ型に関するメタデータを管理する。

### includeファイル

* include/manager/metadata/datatypes.h
* include/manager/metadata/metadata.h

### メソッド

* add()
* get()
* update()
* remove()
* exists()
* get_all()

### DataTypeメタデータ(root)

| 名前 | 型 | 説明 | 格納する値 |
|----|----|----|----|
|"formatVersion" | number       | データ形式フォーマットバージョン | "1" 固定 |
|"generation"    | number       | メタデータの世代 | "1" 固定 |
|"dataTypes"     | array[object] | DataTypeメタデータオブジェクト | [DataTypeメタデータオブジェクト](#datatypeメタデータオブジェクト) |

### DataTypeメタデータオブジェクト

| 名前 | 型 | 説明 | 格納する値 |
|----|----|----|----|
| "id"            | number   | データ型ID | [データ型ID一覧](#データ型id一覧) |
| "name"          | string   | データ型名 |同上|
| "pg_dataType"   | number    | 対応するPostgreSQLのデータ型のOID |同上|
| "pg_dataTypeName"      | string    | ユーザーが入力するPostgreSQLの型名 |同上|
| "pg_dataTypeQualifiedName"      | string    | PostgreSQL内部の修飾型名 |同上|

### データ型ID一覧

| id | name | pg_dataType | pg_dataTypeName | pg_dataTypeQualifiedName |
|----|----|----|----|----|
|4| INT32       | 23    | integer           | int4        |
|6| INT64       | 20    | bigint            | int8        |
|8| FLOAT32     | 700   | real              | float4      |
|9| FLOAT64     | 701   | double precision  | float8      |
|13| CHAR       | 1042  | char              | bpchar      |
|14| VARCHAR    | 1043  | varchar           | varchar     |
|16| NUMERIC    | 1700  | numeric           | numeric     |
|17| DATE       | 1082  | date              | date        |
|18| TIME       | 1083  | time              | time        |
|19| TIMETZ     | 1266  | timetz            | timetz      |
|20| TIMESTAMP  | 1114  | timestamp         | timestamp   |
|21| TIMESTAMPTZ| 1184  | timestamptz       | timestamptz |
|22| INTERVAL   | 1186  | interval          | interval    |

### pg_dataTypeQualifiedNameを追加する理由

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
|NUMERIC[(p [,s])]|([pg_catalog,numeric] **xor** numericのoid) |NUMERIC|
|DECIMAL[(p [,s])]|([pg_catalog,numeric] **xor** numericのoid) |DECIMAL|
|DATE|([pg_catalog,date] **xor** dateのoid) |DATE|
|TIME[(p)] [without time zone]|([pg_catalog,time] **xor** timeのoid) |TIME|
|TIME[(p)] with time zone|([pg_catalog,timetz] **xor** timeのoid) |TIMETZ|
|TIMESTAMP[(p)] [without time zone]|([pg_catalog,timestamp] **xor** timestampのoid) |TIMESTAMP|
|TIMESTAMP[(p)] with time zone|([pg_catalog,timestamptz] **xor** timestampのoid) |TIMESTAMPTZ|
|INTERVAL[fields] [(p)]|([pg_catalog,interval] **xor** intervalのoid) |INTERVAL|

以上

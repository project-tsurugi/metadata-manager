
# DataType Metadata

2020.04.03 NEC  
2022.10.28 NEC  
2022.12.28 NEC  
2023.01.30 NEC  

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
|~~16~~| ~~NUMERIC~~ <br>(将来拡張用)    | ~~1700~~  | ~~numeric~~           | ~~numeric~~     |
|17| DATE       | 1082  | date              | date        |
|18| TIME       | 1083  | time              | time        |
|~~19~~| ~~TIMETZ~~ <br>(将来拡張用)     | ~~1266~~  | ~~timetz~~            | ~~timetz~~      |
|20| TIMESTAMP  | 1114  | timestamp         | timestamp   |
|~~21~~| ~~TIMESTAMPTZ~~ <br>(将来拡張用)| ~~1184~~  | ~~timestamptz~~       | ~~timestamptz~~ |
|~~22~~| ~~INTERVAL~~ <br>(将来拡張用)   | ~~1186~~  | ~~interval~~          | ~~interval~~    |

### データ型オプション

SQLで指定されたデータ型のオプションは、Columnメタデータオブジェクトのdata_lengthsの配列に登録する。  

* char [ (n) ](またはcharacter [ (n) ])で(n)が指定された場合、配列の1番目にnを登録する。(n)が省略された場合、配列は空となる。  

* varchar [ (n) ](またはcharacter varying [ (n) ])で(n)が指定された場合、配列の1番目にnを登録する。(n)が省略された場合、配列は空となる。

* numeric [ (p [, s ]) ](またはdecimal [ (p [, s]) ])で(p, s)が指定された場合、配列の1番目にp、配列の2番目にsを登録する。, s が省略された場合、配列の1番目にpのみ登録する。(p, s)が省略された場合、配列は空となる。

* time [ (p) ](またはtimetz [ (p) ])で(p)が指定された場合、配列の1番目にpを登録する。(p)が省略された場合、配列は空となる。

* timestamp [ (p) ](またはtimestamptz [ (p) ])で(p)が指定された場合、配列の1番目にpを登録する。(p)が省略された場合、配列は空となる。

* interval [ fields ] [ (p) ]は、fieldsまたは(p)のいずれかが指定された場合、配列の1番目にfields、配列の2番目にpを登録する。(p)のみ省略された場合、配列の1番目にpのみ登録する。fieldsと(p)が省略された場合、配列は空となる。配列の1番目に登録するfields値は[IntervalFields](#intervalfields)を参照のこと。

#### IntervalFields

  | 値 | エイリアス | 説明 | 備考 |
  |---|---|---|---|
  | 0x00007FFF | IntervalFields::OMITTED           | (p)のみ省略された | - |
  | 0x00000004 | IntervalFields::YEAR              | YEAR | - |
  | 0x00000002 | IntervalFields::MONTH             | MONTH | - |
  | 0x00000008 | IntervalFields::DAY               | DAY | - |
  | 0x00000400 | IntervalFields::HOUR              | HOUR | - |
  | 0x00000800 | IntervalFields::MINUTE            | MINUTE | - |
  | 0x00001000 | IntervalFields::SECOND            | SECOND | - |
  | 0x00000006 | IntervalFields::YEAR_TO_MONTH     | YEAR TO MONTH | - |
  | 0x00000408 | IntervalFields::DAY_TO_HOUR       | DAY TO HOUR | - |
  | 0x00000C08 | IntervalFields::DAY_TO_MINUTE     | DAY TO MINUTE | - |
  | 0x00001C08 | IntervalFields::DAY_TO_SECOND     | DAY TO SECOND | - |
  | 0x00000C00 | IntervalFields::HOUR_TO_MINUTE    | HOUR TO MINUTE | - |
  | 0x00001C00 | IntervalFields::HOUR_TO_SECOND    | HOUR TO SECOND | - |
  | 0x00001800 | IntervalFields::MINUTE_TO_SECOND  | MINUTE TO SECOND | - |

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
|TIME[(p)] with time zone, TIMETZ[(p)]|([pg_catalog,timetz] **xor** timeのoid) |TIMETZ|
|TIMESTAMP[(p)] [without time zone]|([pg_catalog,timestamp] **xor** timestampのoid) |TIMESTAMP|
|TIMESTAMP[(p)] with time zone, TIMESTAMPTZ[(p)]|([pg_catalog,timestamptz] **xor** timestampのoid) |TIMESTAMPTZ|
|INTERVAL[fields] [(p)]|([pg_catalog,interval] **xor** intervalのoid) |INTERVAL|

### 日付・時刻データ型の内部構造

#### timestamp型の構造

* timestamp型のサイズは、int64（typedef int64 Timestamp;）である
* 値が0の場合、「2000-01-01 09:00:00.000000」を示す
* 0から999999の値は、1秒未満(精度：マイクロ秒)を示す（999999は「2000-01-01 09:00:00.999999」）
* 1000000以降の値は、2000/1/1 9:00:00以降の日時が1秒単位で昇順する（1000000は「2000-01-01 09:00:01.000000」）
* 2000/1/1 9:00:00以前の日時は、マイナス値となり1秒単位で降順する（-1000000は「2000-01-01 08:59:59.000000」）
* timestamp型の範囲（最遠の過去および最遠の未来）は、[PostgreSQLの仕様](https://www.postgresql.jp/document/12/html/datatype-datetime.html)に準じる

#### date型の構造

* date型のサイズは、int32（typedef int32 DateADT;）である
* 値が0の場合、「2000-01-01」を示す
* 値の精度は1日であり、-1が1999/12/31となる
* date型の範囲（最遠の過去および最遠の未来）は、[PostgreSQLの仕様](https://www.postgresql.jp/document/12/html/datatype-datetime.html)に準じる

#### time型の構造

* time型のサイズは、int64（typedef int64 TimeADT;）である
* 値が0の場合、「00:00:00.000000」を示す
* 0から999999の値は、1秒未満(精度：マイクロ秒)を示す（999999は「00:00:00.999999」）
* time型の範囲（最遠の過去および最遠の未来）は、[PostgreSQLの仕様](https://www.postgresql.jp/document/12/html/datatype-datetime.html)に準じる

#### 時間帯付きデータ型について

PostgreSQLは、日付と時刻をすべてUTCで保持し、クライアントに表示される前にTimeZone設定パラメータで指定された時間帯に変換を行っているため、時間帯付きデータ型（TimestampTz型、TimeTz型）は、Tsuirugiにおいて非サポート（将来拡張用）とする。  
　https://www.postgresql.jp/document/12/html/datatype-datetime.html#DATATYPE-TIMEZONES

#### OLTPがサポートしてないデータ型について

OLTPがサポートしてないInterval型（Numeric型も同様）は、Tsurugiにおいても非サポート（将来拡張用）とする。


以上


# DataType Metadata (rev.0.1)

2020.02.18 NEC

## DataTypeMetadataクラス
NEDO DBで使用するデータ型に関するメタデータを管理する。

### メソッド一覧
|メソッド名|説明|
|:---------|:---|
|load()|data_typeメタデータテーブルの内容をすべて読み込む。|
|add()|data_typeメタデータオブジェクトを追加する。|
|get()|指定したIDまたは名前を持つdata_typeメタデータオブジェクトを取得する。|
|set()|指定したIDまたは名前を持つdata_typeメタデータオブジェクトを更新する。|
|remove()|指定したIDまたは名前を持つdata_typeメタデータオブジェクトを削除する。|
|next()|次のdata_typeメタデータオブジェクトを取得する。|

### メタデータフォーマット

boost::property_tree::ptreeに格納されるメタデータのフォーマット。

```
// DataType情報メタデータ(root)
{
    "formatVersion"    : number,       // データ形式フォーマットバージョン
    "generation"       : number,       // メタデータの世代
    "dataTypes"        : array[object] // data_typeメタデータオブジェクト
}

// DataTypeメタデータオブジェクト
{
    "id"            : number,   // データ型ID
    "name"          : string,   // データ型名
    "pg_dataType"   : number    // 対応するPostgreSQLのデータ型のOID
    "baseType"      : string    // 基本型 ※draft
}
```

以上


# DataType Metadata (rev.0.1)

2020.02.18 NEC

## DataTypeMetadataクラス
NEDO DBで使用するデータ型に関するメタデータを管理する。

### メソッド一覧
|メソッド名|説明|
|:---------|:---|
|load()|DataTypeメタデータテーブルの内容をすべて読み込む。|
|add()|DataTypeメタデータオブジェクトを追加する。|
|get()|指定したIDまたは名前を持つDataTypeメタデータオブジェクトを取得する。|
|set()|指定したIDまたは名前を持つDataTypeメタデータオブジェクトを更新する。|
|remove()|指定したIDまたは名前を持つDataTypeメタデータオブジェクトを削除する。|
|next()|次のDataTypeメタデータオブジェクトを取得する。|

### メタデータフォーマット

boost::property_tree::ptreeに格納されるメタデータのフォーマット。

```
// DataType情報メタデータ(root)
{
    "formatVersion"    : number,       // データ形式フォーマットバージョン
    "generation"       : number,       // メタデータの世代
    "dataTypes"        : array[object] // DataTypeメタデータオブジェクト
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

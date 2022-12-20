.
# メタデータAPI基本仕様

2020.04.03 NEC  
2022.10.28 NEC  

## メタデータAPIアーキテクチャ

* メタデータAPIは、メタデータをboost::property_tree形式、または、manager::metadata::Object形式のオブジェクト(以降メタデータオブジェクトと称す)に格納して各コンポーネントに提供する。
* 各コンポーネントは、メタデータオブジェクトにメタデータを格納して、メタデータAPIに渡すことによってメタデータを追加/更新できる。
* メタデータAPIでは、メタデータテーブルの内容をそのまま渡すのではなく、コンポーネントがアクセスしやすいデータ構造に変換してメタデータを渡す。このデータ構造にはバージョン(フォーマットバージョン)が付与される。
* 統合メタデータ管理基盤は、性能向上を目的としたバッファリングや遅延書込み等は行わないため、メタデータ管理基盤を通じたメタデータ操作のコストは高くなる。

## manager::metadata::Metadataクラス

* メタデータを管理する基本的なクラス。
* 実際にはMetadataクラスから派生したクラス(Metadata::Tablesなど)を利用してメタデータにアクセスする。
* 派生クラスでは管理するメタデータの内容に合わせてメソッドが実装される（必ずしも基底クラスのメソッドがすべて派生クラスで実装されるわけではない）。

### メソッド一覧

|メソッド名|説明|
|:---------|:---|
|get_tables_ptr()|テーブルメタデータが格納された領域のポインタを取得する。|
|get_indexes_ptr()|インデックスメタデータが格納された領域のポインタを取得する。|
|add()|メタデータオブジェクト単位でメタデータを追加する（tableであれば１テーブル）。|
|get()|メタデータオブジェクト単位でメタデータを読み込む（tableであれば１テーブル）。|
|update()|メタデータオブジェクト単位でメタデータを更新する（tableであれば１テーブル）。|
|remove()|メタデータオブジェクト単位でメタデータを削除する（tableであれば１テーブル）。|
|exists()|メタデータオブジェクトの存在有無を確認する。|
|get_all()|メタデータに存在するのすべてメタデータオブジェクトを読み込む。|

## エラー処理

* metadata-managerは、テーブル定義要求を受け取った時、同一テーブル名が既に登録されていた場合、エラーとする。

## 想定する使用方法

各コンポーネントからの使用方法について説明する。

* テーブルメタデータのポインタを取得する。
   1. auto tables = **metadata::get_tables_ptr("tsurugi")**;

* インデックスメタデータのポインタを取得する。
   1. auto indexes = **metadata::get_indexes_ptr("tsurugi")**;

* メタデータオブジェクトを追加する(boost::property_tree形式)。
  1. property_tree::ptree table;
  1. table.put(metadata::Table::NAME, ”table1”);
  1. metadata::ErrorCode error = **tables->add(table, &object_id)**;

* メタデータオブジェクトを追加する(manager::metadata::Object形式)。
  1. metadata::Object object;
  1. auto& table = static_cast\<metadata::Table&\>(object);
  1. table.name = "table1";       // メタデータオブジェクトにデータを追加する
  1. metadata::ErrorCode error = **tables->add(table, &object_id)**;

* メタデータオブジェクトを(オブジェクトIDから)読み込む。
  1. metadata::Index index;
  1. auto error = **indexes->get(object_id, index)**;

* メタデータオブジェクトを(オブジェクト名から)読み込む。
  1. metadata::Index index;
  1. auto error = **indexes->get("index1", index)**;

* メタデータオブジェクト(テーブル)を更新する。
  1. auto error = tables->get("table1", table);   // 更新するメタデータオブジェクトを読み込む
  1. metadata::ObjectIdType object_id = table.id;
  1. table.name = "update_table";   // メタデータオブジェクトのデータを更新する
  1. error = **tables->update(object_id, table)**;

* メタデータオブジェクト(テーブル)を削除する。
  1. metadata::Table table;
  1. auto error = **tables->remove("table1")**;

* オブジェクト(テーブルメタデータ)の存在有無を確認する。
  1. bool is_exists = **tables->exists("table1")**;

* すべてのオブジェクト(インデックスメタデータ)を読み込む。
  1. std::vector\<boost::property_tree::ptree\> index_elements = {};
  1. metadata::ErrorCode error = **indexes->get_all(index_elements)**;

以上

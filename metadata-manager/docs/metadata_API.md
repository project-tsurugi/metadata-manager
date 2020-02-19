
# メタデータAPI基本仕様

2020.02.18 NEC

## メタデータAPIアーキテクチャ
* メタデータAPIは、メタデータを boost::property_tree オブジェクトに格納して各コンポーネントに提供する。
* 各コンポーネントは、property_treeオブジェクトにメタデータを格納して、メタデータAPIに渡すことによってメタデータを追加/更新できる。
* メタデータAPIでは、メタデータテーブルの内容をそのまま渡すのではなく、コンポーネントがアクセスしやすいデータ構造に変換してメタデータを渡す。このデータ構造にはバージョン(フォーマットバージョン)が付与される。
* 統合メタデータ管理基盤は、性能向上を目的としたバッファリングや遅延書込み等は行わないため、メタデータ管理基盤を通じたメタデータ操作のコストは高くなる。

## manager::metadta-manager::Metadataクラス

* メタデータを管理する基本的なクラス。
* 実際にはMetadataクラスから派生したクラス(TableMetadataなど)を利用してメタデータにアクセスする。
* 派生クラスでは管理するメタデータの内容に合わせてメソッドが実装される（必ずしも基底クラスのメソッドがすべて派生クラスで実装されるわけではない）。

### メソッド一覧

|メソッド名|説明|
|:---------|:---|
|static load()|メタデータテーブルのすべての内容をproperty_treeオブジェクトに読み込む。|
|generation()|Metadataクラスオブジェクトに読み込まれているメタデータの世代(メタデータバージョン)。|
|load()|メタデータテーブルからMetadataクラスオブジェクトにメタデータを読み込む。|
|add()|メタデータオブジェクト単位でメタデータを追加する（tableであれば１テーブル）。|
|get()|メタデータオブジェクト単位でメタデータを読み込む（tableであれば１テーブル）。|
|set()|メタデータオブジェクト単位でメタデータを書き込む（tableであれば１テーブル）。※V1スコープ外|
|remove()|メタデータオブジェクト単位でメタデータを削除する（tableであれば１テーブル）。※V1スコープ外|
|next()|次のメタデータオブジェクトを取得する。最後のオブジェクトを読み込んだ後はEND_OF_ROWを返す。|

## 想定する使用方法
各コンポーネントからの使用方法について説明する。
* メタデータをすべて読み込む。(方法1)
   1. static TableMetadata::load( database_name, ptree, generation );
   1. BOOST_FOREACH ( const ptree::value_type& child, ptree.get_child( node_name ) ) { childを使う処理 }

* メタデータ(オブジェクト)を順次読み込む。(方法2)
  1. Metadata* table = new TableMetadata( database_name );
  1. table->load( generation );
  1. while( table->next( ptree ) ) { ptreeを使う処理 }

* オブジェクト(テーブルメタデータ)を追加する。
  1. ptree.put( "name", "employee" );　　　　　　// メタデータをproperty_treeに詰め込む
  1. Metadata* table = new TableMetadata( database_name, generation );　// TableMetadataオブジェクトの生成と読み込み
  1. table->add( ptree, &id );　　　　　　　　　　// 追加したオブジェクトのIDを返す

* オブジェクト(テーブルメタデータ)を更新する。
  1. Metadata* table = new TableMetadata( database_name, generation );
  1. table->get( id, ptree );
  1. ptree->put( "name", "employee_temp" );
  1. table.->et( id, ptree );

* オブジェクト(テーブルメタデータ)を削除する。
  1. Metadata* table = new TableMetadata( database, generation );
  1. table->remove( "employee_temp" );

以上

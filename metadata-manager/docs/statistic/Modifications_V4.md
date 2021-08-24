# 統合メタデータ管理基盤<BR>統計情報メタデータ 改造仕様書

2021.08.20 初版 NEC

<!-- TOC -->

- [統合メタデータ管理基盤<BR>統計情報メタデータ 改造仕様書](#%E7%B5%B1%E5%90%88%E3%83%A1%E3%82%BF%E3%83%87%E3%83%BC%E3%82%BF%E7%AE%A1%E7%90%86%E5%9F%BA%E7%9B%A4br%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%83%A1%E3%82%BF%E3%83%87%E3%83%BC%E3%82%BF-%E6%94%B9%E9%80%A0%E4%BB%95%E6%A7%98%E6%9B%B8)
  - [現バージョンV3の問題点](#%E7%8F%BE%E3%83%90%E3%83%BC%E3%82%B8%E3%83%A7%E3%83%B3v3%E3%81%AE%E5%95%8F%E9%A1%8C%E7%82%B9)
  - [修正方針案](#%E4%BF%AE%E6%AD%A3%E6%96%B9%E9%87%9D%E6%A1%88)
  - [外部設計](#%E5%A4%96%E9%83%A8%E8%A8%AD%E8%A8%88)
    - [追加するメソッド](#%E8%BF%BD%E5%8A%A0%E3%81%99%E3%82%8B%E3%83%A1%E3%82%BD%E3%83%83%E3%83%89)
  - [内部設計](#%E5%86%85%E9%83%A8%E8%A8%AD%E8%A8%88)
    - [DBテーブル](#db%E3%83%86%E3%83%BC%E3%83%96%E3%83%AB)
      - [テーブル統計情報](#%E3%83%86%E3%83%BC%E3%83%96%E3%83%AB%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1)
      - [列統計情報](#%E5%88%97%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1)
  - [付録](#%E4%BB%98%E9%8C%B2)
    - [A. Boost Property TreeとJSONの対応](#a-boost-property-tree%E3%81%A8json%E3%81%AE%E5%AF%BE%E5%BF%9C)
      - [JSON形式とProperty Tree形式の変換例](#json%E5%BD%A2%E5%BC%8F%E3%81%A8property-tree%E5%BD%A2%E5%BC%8F%E3%81%AE%E5%A4%89%E6%8F%9B%E4%BE%8B)
      - [Property Treeのアクセス方法](#property-tree%E3%81%AE%E3%82%A2%E3%82%AF%E3%82%BB%E3%82%B9%E6%96%B9%E6%B3%95)
  - [B. PostgreSQLデータ型とC++データ型の対応関係](#b-postgresql%E3%83%87%E3%83%BC%E3%82%BF%E5%9E%8B%E3%81%A8c%E3%83%87%E3%83%BC%E3%82%BF%E5%9E%8B%E3%81%AE%E5%AF%BE%E5%BF%9C%E9%96%A2%E4%BF%82)
    - [数値データ型](#%E6%95%B0%E5%80%A4%E3%83%87%E3%83%BC%E3%82%BF%E5%9E%8B)
  - [C. PostgreSQLのStatisticsクラス](#c-postgresql%E3%81%AEstatistics%E3%82%AF%E3%83%A9%E3%82%B9)
    - [pg_statsview](#pg_statsview)
    - [pg_statistic](#pg_statistic)
  - [D. V1でMetadataクラスのメソッドを純粋仮想関数にしなかった理由](#d-v1%E3%81%A7metadata%E3%82%AF%E3%83%A9%E3%82%B9%E3%81%AE%E3%83%A1%E3%82%BD%E3%83%83%E3%83%89%E3%82%92%E7%B4%94%E7%B2%8B%E4%BB%AE%E6%83%B3%E9%96%A2%E6%95%B0%E3%81%AB%E3%81%97%E3%81%AA%E3%81%8B%E3%81%A3%E3%81%9F%E7%90%86%E7%94%B1)
  - [E.表統計情報オブジェクト（TableStatistics）](#e%E8%A1%A8%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88tablestatistics)
      - [表統計情報オブジェクト例（JSON形式でのイメージ）](#%E8%A1%A8%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88%E4%BE%8Bjson%E5%BD%A2%E5%BC%8F%E3%81%A7%E3%81%AE%E3%82%A4%E3%83%A1%E3%83%BC%E3%82%B8)
    - [列統計情報オブジェクト（ColumnStatistics）](#%E5%88%97%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88columnstatistics)
      - [列統計情報オブジェクト例（JSON形式でのイメージ）](#%E5%88%97%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88%E4%BE%8Bjson%E5%BD%A2%E5%BC%8F%E3%81%A7%E3%81%AE%E3%82%A4%E3%83%A1%E3%83%BC%E3%82%B8)
  - [F. 案２の統計情報オブジェクト](#f-%E6%A1%88%EF%BC%92%E3%81%AE%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88)
      - [統計情報オブジェクト例（JSON形式によるイメージ）](#%E7%B5%B1%E8%A8%88%E6%83%85%E5%A0%B1%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88%E4%BE%8Bjson%E5%BD%A2%E5%BC%8F%E3%81%AB%E3%82%88%E3%82%8B%E3%82%A4%E3%83%A1%E3%83%BC%E3%82%B8)

<!-- /TOC -->

## 現バージョン(V3)の問題点

1. Statisticsクラスのメソッド名が基底クラス(Metadata)の方針に沿っていない

    - 1つのクラスで複数のメタデータ（表統計情報、列統計情報）を扱っている。
    - 基本方針は1クラス、1メタデータ

1. Tablesクラスのすべてのテーブルメタデータを取得する手段が提供されていない

    - 以前はload()やnext()といった手段が提供されていた。

1. 一部メソッドのインタフェースが統計情報メタデータに最適化されている（汎用性がない）

    - 一部ptreeの代わりにunnamed_mapが使われているが、キーとして列番号を使用している。

## 修正方針案

おもに問題点１の修正案として以下の３つの案が考えられる。

- 案１：統計情報クラスを表統計情報クラスと列統計情報クラスに分ける
- 案２：表統計情報と列統計情報を1つの統計情報クラスにまとめる
- 案３：統計情報クラスでは列統計情報クラスのみ扱い、表統計情報はテーブルクラスで扱う

案１では、既存のStatisticsクラスを廃止し、表統計情報クラスと列統計情報クラスに分ける。この案では、1つの表に関する統計情報（表統計情報と列統計情報）を取得するために最低でも２回のメソッド呼び出し（DBアクセス）が必要となる。既存のStatisticsクラスのget_one_column_statisticなどのメソッドはgetメソッドなどのオーバーロードメソッド（引数違いのメソッド）に変更になる（メソッド名の変更）。

案２では、表統計情報と列統計情報を1つのクラスにまとめて返す。この案では１つの表に関する統計情報を取得するために１回のメソッド呼び出し（DBアクセス）で済む。ただし、クライアント側で統計情報クラスから表統計情報や列統計情報を別途取り出す（Property Treeのノード操作）処理が必要となる。そのため、特定の列の統計情報のみが必要な場合は既存のメソッド（get_one_column_statisticなど）を利用可能とする。

どちらの案であっても大きな違いはないと考えるが、設計をシンプルにするために今回は案１を選択する。

## 外部設計

### 表統計情報オブジェクト（TableStatistics）

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | format_version*  | int64_t | オブジェクトのフォーマットバージョン。  | ○ | -    | -  |
  | 2. | generation*      | int64_t | メタデータの世代。                      | ○ | -    | 現状"1"固定。 |
  | 3. | id*              | int64_t | 表統計情報オブジェクトのOID。           | ○ | -    | -  |
  | 4. | name*            | string  | 表統計情報オブジェクト名。              | -  | -    | 任意の名前を設定可能。 |
  | 5. | table_id         | int64_t | テーブルのOID。                         | -  | △*1 | -  |
  | 6. | table_name       | string  | テーブル名。                            | -  | △*1 | -  |
  | 7. | table_tuples     | float   | テーブルの推定行数。                    | -  | -    | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。
  - *1 table_id または table_nameの選択必須。

#### 表統計情報オブジェクト例（JSON形式でのイメージ）

- 例１－１）addメソッド実行時

  ```json
  {
    "name": "PUBLIC.foo", "table_id": 2, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

- 例１－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "PUBLIC.foo", 
    "table_id": 2, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```
- 例２－１）addメソッド実行時

  ```json
  {
    "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

- 例２－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "", 
    "table_id": 0, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

### 列統計情報オブジェクト（ColumnStatistics）

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | format_version*  | int64_t | オブジェクトのフォーマットバージョン。| ○ | -    |          |
  | 2. | generation*      | int64_t | メタデータの世代。                    | ○ | -    | 現状"1"固定。   |
  | 1. | id*              | int64_t | カラム統計情報オブジェクトのOID。     | ○ | -    | -  |
  | 2. | name*            | string  | カラム統計情報オブジェクト名。        | -  | -    | 任意の名前を設定可能。 |
  | 3. | table_id         | int64_t | カラムが属するテーブルのOID。         | -  | △*1 | -  |
  | 4. | table_name       | int64_t | カラムが属するテーブルのOID。         | -  | △*1 | -  |
  | 5. | column_id        | int64_t | カラムOID。                           | -  | △*2 | -  |
  | 6. | column_id        | int64_t | カラム名。                            | -  | △*2 | -  |
  | 7. | ordinal_position | int64_t | カラム番号。                          | -  | ○   | -  |
  | 8. | column_statistic | string  | カラム統計情報。                      | -  | -    | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。
  - *1 table_id または table_nameの選択必須。
  - *2 column_id または column_nameの選択必須。

#### 列統計情報オブジェクト例（JSON形式でのイメージ）

- 例１－１）addメソッド実行時

  ```json
  {
    "name": "PUBLIC.foo.col1.stat", "table_id": 2, "table_name": "PUBLIC.foo", 
    "column_id": 32, "column_name": "col1", "ordinal_position": 1, "column_statistic": {...}
  }
  ```

- 例１－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "PUBLIC.foo.col1.stat", 
    "table_id": 2, "table_name": "PUBLIC.foo", "column_id": 32, "column_name": "col1", 
    "ordinal_position": 1, "column_statistic": {...}
  }
  ```

- 例２－１）addメソッド実行時

  ```json
  {
    "table_id": 2, "column_name": "col1", "ordinal_position": 1, "column_statistic": {...}
  }
  ```
- 例２－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "", 
    "table_id": 2, "table_name": "", "column_id": 0, "column_name": "col1", 
    "ordinal_position": 1, "column_statistic": {...}
  }
  ```

### 追加するメソッド

getメソッドを例に今回の改造の要点を説明する。

  - 既存のgetメソッド
    - Statistics
      - get_table_statistic(table_id, table_statistic)
      - get_table_statistic(table_name, table_statistic)
      - get_one_column_statistic(table_id, column_number, column_statistic)
      - get_all_column_statistics(table_id, column_statistics)

  - 変更後のgetメソッド

    StatisticsクラスをColumnStatisticsとTableStatisticsの２つに分ける。

    - TableStatistics
      - get(id, ptree);
      - get(name, ptree);
      - get_all(objects) // すべての表統計情報を取得する
      - get_from_table_id(table_id, ptree);
      - get_from_table_name(table_name, ptree);
    
    - ColumnStatistics
      - get(id, ptree);
      - get(name, ptree);
      - get_all(objects);  // すべての列統計情報を取得する
      - get_from_column_id(column_id, ptree);
      - get_from_column_name(column_name, ptree);
      - get_all_from_table_id(table_id, objects)  // 特定のテーブルに属するすべての列統計情報を取得する

## 内部設計

### DBテーブル

#### テーブル統計情報

テーブル統計情報はTableメタデータテーブルに格納する。

| # | 名前 | 型 | 参照先 | 説明 |
|---|---|---|---|---|
| id          | | | | |
| name        | | | | |
| namespace   | | | | |
| primary_key | | | | |
| stat_id     |
| stat_name   |
| rel_tuples  | | | | |

#### 列統計情報

## 付録

### A. Boost Property TreeとJSONの対応

[How to Populate a Property Tree](https://www.boost.org/doc/libs/1_77_0/doc/html/property_tree/parsers.html#property_tree.parsers.json_parser)

プロパティツリーデータセットは型付けされておらず、配列をサポートしていません。そのため、以下のようなJSONとプロパティツリーのマッピングが使用されています。

- JSON オブジェクトはノードにマッピングされます。各プロパティは子ノードです。
- JSON 配列は、ノードにマッピングされます。各要素は、空の名前を持つ子ノードです。ノードに名前付きと名前なしの両方の子ノードがある場合は、JSON表現にマッピングできません。
- JSONの値は、その値を含むノードにマッピングされます。ただし、すべての型情報は失われます。数字やリテラルの「null」、「true」、「false」は、単に文字列形式にマッピングされます。
- 子ノードとデータの両方を含むプロパティ ツリー ノードは、マッピングできません。

型情報が失われていることを除けば、JSONはラウンドトリップします。

#### JSON形式とProperty Tree形式の変換例

  - JSON
    ```json
    {
      "menu":
      {
          "foo": true,
          "bar": "true",
          "value": 102.3E+06,
          "popup":
          [
            {"value": "New", "onclick": "CreateNewDoc()"},
            {"value": "Open", "onclick": "OpenDoc()"},
          ]
      }
    }
    ```

  - Property Tree
    ```text
    menu
    {
      foo true
      bar true
      value 102.3E+06
      popup
      {
          ""
          {
            value New
            onclick CreateNewDoc()
          }
          ""
          {
            value Open
            onclick OpenDoc()
          }
      }
    }
    ```

#### Property Treeのアクセス方法

[How to Access Data in a Property Tree](<https://www.boost.org/doc/libs/1_77_0/doc/html/property_tree/accessing.html>)

## B. PostgreSQLデータ型とC++データ型の対応関係

前提条件

- 64bit CPU 
- 64bit OS (Ubuntu 20.x LTS)

### 数値データ型

| # | PostgreSQLデータ型 | サイズ | C++データ型 | サイズ | 値域(PG) | 備考 |
|---|---|---|---|---|---|---|
|1. | smallint  | 2バイト   | std::int16_t | 2バイト  | -32,768 -> +32,767                                       | - |
|2. | integer   | 4バイト   | std::int32_t | 4バイト  | -21,47,483,648 -> +2,147,483,647                         | - |
|3. | bigint    | 8バイト   | std::int64_t | 8バイト  | -9,223,372,036,854,775,808 -> 9,223,372,036,854,775,807  | - |
|4. | decimal   | 可変長    | 
|5. | numeric   | 可変長    |
|6. | real              | 4バイト | std::float   | 4バイト  | 6桁精度   | 
|7. | double\nprecision | 8バイト | std::double  | 8バイト  | 15桁精度  |
|8. | smallserial | 2バイト | std::int16_t  | 2バイト | 1 -> 32,767                     | |
|9. | serial      | 4バイト | std::int32_t  | 4バイト | 1 -> 2,147,483,647              | |
|10.| bigserial   | 8バイト | std::int64_t  | 8バイト | 1 -> 9,223,372,036,854,775,807  | |

## C. PostgreSQLのStatisticsクラス

### pg_stats(view)

<https://www.postgresql.jp/document/12/html/view-pg-stats.html>

人間が見ることを意識しているためか、以下の3つの情報で一意の列を表している。
- スキーマ名
- テーブル名
- 列名

### pg_statistic

<https://www.postgresql.jp/document/12/html/catalog-pg-statistic-ext.html>

- 列が属するテーブルもしくはインデックスのoid
- 列番号

## D. V1でMetadataクラスのメソッドを純粋仮想関数にしなかった理由

理由：MetadataクラスのメソッドにTemplate Methodパターンを適用したため。

通常、今回のようなケースでは基底クラスであるMetdataクラスにget()などのメソッドを純粋仮想関数（インタフェース）として宣言し、各派生クラス（TableクラスやStatisticsクラス）でそれらのメソッドの処理を記述する。

```text
(基底クラス)
virtual Metadata::get() = 0;

(派生クラス)
Tables::get() {
  try {
    db.connect(db_name);
    query = "SELECT * FROM pg_table"; 
    db.dispatch_query(query);
    value = db.read();
    db.close();
  }
  catch(...) {
    ...
  }
  return value;  
}
Statistics::get() {
  try {
    db.connect(db_name);
    query = "SELECT * FROM pg_statistics";
    db.dispatch_query(query);
    value = db.read();
    db.close();
  }
  catch(...) {
    ...
  }
  return value;  
}
```

ただ、統合メタデータ管理基盤のような比較的単純な処理（データの読み書きなど）を行う場合は、各派生クラスでの処理が似たようなものになり、一部のパラメータ（DBの表名や列名など）の違いのみとなることが多い。そのようなケースでは、あらかじめ基底クラスで基本となる処理テンプレート（異常系を含む）を作成しておき、パラメータや処理が異なる部分のみを純粋仮想関数とすることによって派生クラスの作成が楽になる。

```text
(基底クラス)
virtual Metadata::get() {
  try {
    db.connect(db_name);
    query = create_query();
    db.dispatch_query(query);
    value = db.read();
    db.close();
  }
  catch(...) {
    ...
  }
  return value;
}
virtual std::string Metadata::create_query() = 0;

(派生クラス)
Tables::create_query() { query = "SELECT * FROM pg_table"; }
Statistics::create_query() { query = "SELECT * FROM pg_statistics"; }
```

## E.表統計情報オブジェクト（TableStatistics）

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | format_version*  | int64_t | オブジェクトのフォーマットバージョン。  | ○ | -    | -  |
  | 2. | generation*      | int64_t | メタデータの世代。                      | ○ | -    | 現状"1"固定。 |
  | 3. | id*              | int64_t | 表統計情報オブジェクトのOID。           | ○ | -    | -  |
  | 4. | name*            | string  | 表統計情報オブジェクト名。              | -  | -    | 任意の名前を設定可能。 |
  | 5. | table_id         | int64_t | テーブルのOID。                         | -  | △*1 | -  |
  | 6. | table_name       | string  | テーブル名。                            | -  | △*1 | -  |
  | 7. | table_tuples     | float   | テーブルの推定行数。                    | -  | -    | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。
  - *1 table_id または table_nameの選択必須。

#### 表統計情報オブジェクト例（JSON形式でのイメージ）

- 例１－１）addメソッド実行時

  ```json
  {
    "name": "PUBLIC.foo", "table_id": 2, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

- 例１－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "PUBLIC.foo", 
    "table_id": 2, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```
- 例２－１）addメソッド実行時

  ```json
  {
    "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

- 例２－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "", 
    "table_id": 0, "table_name": "PUBLIC.foo", "table_tuples": 231000
  }
  ```

### 列統計情報オブジェクト（ColumnStatistics）

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | format_version*  | int64_t | オブジェクトのフォーマットバージョン。| ○ | -    |          |
  | 2. | generation*      | int64_t | メタデータの世代。                    | ○ | -    | 現状"1"固定。   |
  | 1. | id*              | int64_t | カラム統計情報オブジェクトのOID。     | ○ | -    | -  |
  | 2. | name*            | string  | カラム統計情報オブジェクト名。        | -  | -    | 任意の名前を設定可能。 |
  | 3. | table_id         | int64_t | カラムが属するテーブルのOID。         | -  | △*1 | -  |
  | 4. | table_name       | int64_t | カラムが属するテーブルのOID。         | -  | △*1 | -  |
  | 5. | column_id        | int64_t | カラムOID。                           | -  | △*2 | -  |
  | 6. | column_id        | int64_t | カラム名。                            | -  | △*2 | -  |
  | 7. | ordinal_position | int64_t | カラム番号。                          | -  | ○   | -  |
  | 8. | column_statistic | string  | カラム統計情報。                      | -  | -    | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。
  - *1 table_id または table_nameの選択必須。
  - *2 column_id または column_nameの選択必須。

#### 列統計情報オブジェクト例（JSON形式でのイメージ）

- 例１－１）addメソッド実行時

  ```json
  {
    "name": "PUBLIC.foo.col1.stat", "table_id": 2, "table_name": "PUBLIC.foo", 
    "column_id": 32, "column_name": "col1", "ordinal_position": 1, "column_statistic": {...}
  }
  ```

- 例１－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "PUBLIC.foo.col1.stat", 
    "table_id": 2, "table_name": "PUBLIC.foo", "column_id": 32, "column_name": "col1", 
    "ordinal_position": 1, "column_statistic": {...}
  }
  ```

- 例２－１）addメソッド実行時

  ```json
  {
    "table_id": 2, "column_name": "col1", "ordinal_position": 1, "column_statistic": {...}
  }
  ```
- 例２－２）getメソッド実行時

  ```json
  {
    "format_version": 1, "generation": 1, "id": 3, "name": "", 
    "table_id": 2, "table_name": "", "column_id": 0, "column_name": "col1", 
    "ordinal_position": 1, "column_statistic": {...}
  }
  ```

## F. 案２の統計情報オブジェクト

統計情報オブジェクトはテーブルごとに作成され、そのテーブルが持つすべてのカラムのカラム統計情報オブジェクトを持つ。

なお、カラム統計情報のフォーマットは従来仕様から変更がない（フリーフォーマット）。

- 統計情報オブジェクト

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | format_version*| int64_t  | オブジェクトのフォーマットバージョン。      | ○ | -    |          |
  | 2. | generation*    | int64_t  | メタデータの世代。                          | ○ | -    | 予約。   |
  | 3. | id*            | int64_t  | 統計情報オブジェクトのOID。                 | ○ | -    |          |
  | 4. | name*          | string   | 統計情報オブジェクトの名前。                | -  | -    | 任意の名前を設定可能。|
  | 5. | table_id       | int64_t  | 統計情報を持つテーブルのOID。               | -  | △*1 |          |
  | 6. | table_name     | string   | 統計情報を持つテーブルの名前。              | -  | △*1 |          |
  | 7. | table_tuples   | float    | テーブルの推定行数。                        | -  | -    |          |
  | 8. | columns        | Node     | 列統計情報オブジェクト群（下記参照）。      | -  | -    |          |

  - *はすべてのメタデータオブジェクトに含まれる基本パラメータであることを示す。
  - "採番"の項目は統合メタデータ管理基盤で自動入力される値。
  - "add"はaddメソッド実行時に入力が必須の項目。

- 列統計情報オブジェクト

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | id*              | int64_t | カラム統計情報オブジェクトのOID。 | ○  | -  | -  |
  | 2. | name*            | string  | カラム名。                        | -   | -  | 任意の名前を設定可能。 |
  | 3. | column_id        | int64_t | カラムのOID。                     | -   | -  | -  |
  | 4. | column_name      | string  | カラム名。                        | -   | -  | -  |
  | 5. | ordinalPosition  | int64_t | カラム番号。                      | -   | ○ | -  |
  | 6. | columnStatistic  | string  | カラム統計情報。                  | -   | -  | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。

#### 統計情報オブジェクト例（JSON形式によるイメージ）

JSON形式での記述イメージ。

- 例１）すべてのパラメータが埋まっている場合

  ```json
  {
    "format_version": 1,
    "generation": 1,
    "id": 2,
    "name": PUBLIC."foo_table.stat",
    "table_id": 2,
    "table_name": "PUBLIC.foo_table",
    "table_tuples": 10000,
    "columns": [
      {"id": 33, "name": "col1.stat", "column_id": 3, "column_name": "col1", "ordinalPosition": 1,
        "columnStatistic": {...}  },
      {"id": 34, "name": "col2.stat", "column_id": 4, "column_name": "col2", "ordinalPosition": 2, 
        "columnStatistic": {...}  },
      {"id": 35, "name": "col3.stat", "column_id": 5, "column_name": "col3", "ordinalPosition": 3, 
        "columnStatistic": {...}  },
    ]
  }
  ```

以上

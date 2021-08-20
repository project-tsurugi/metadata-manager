# 統合メタデータ管理基盤<BR>統計情報メタデータ 改造仕様書

2021.08.05 初版 NEC

## 改造概要

### メソッドの追加

Statisticsクラスにテーブルごとの統計情報をまとめた統計情報オブジェクトを追加する。  
Statisticsクラスに以下のメソッドを追加する。

- add()
- get()
- remove()

メソッドの追加に合わせて各メソッドで扱う統計情報オブジェクトを新たに定義する。

## 外部設計

### 統計情報オブジェクト

統計情報オブジェクトはテーブルごとに作成され、そのテーブルが持つすべてのカラムのカラム統計情報オブジェクトを持つ。

なお、カラム統計情報のフォーマットは従来仕様から変更がない（フリーフォーマット）。

- 統計情報オブジェクト

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | formatVersion* | int64_t  | オブジェクトのフォーマットバージョン。      | ○ | -  |          |
  | 2. | generation*    | int64_t  | メタデータの世代。                          | ○ | -  | 予約。   |
  | 3. | id*            | int64_t  | 統計情報オブジェクトのOID。                 | ○ | -  |          |
  | 4. | name*          | string   | 統計情報オブジェクトの名前。                | -  | -  | 任意の名前を設定可能。|
  | 5. | tableId        | int64_t  | 統計情報を持つテーブルのOID。               | -  | ○ |          |
  | 6. | tableName      | string   | 統計情報を持つテーブルの名前。              | -  | -  |          |
  | 7. | relTuples      | float    | テーブルの推計行数。                        | -  | -  |          |
  | 8. | columns        | Node     | カラム統計情報オブジェクト群（下記参照）。  | -  | -  |          |

  - *はすべてのメタデータオブジェクトに含まれる基本パラメータであることを示す。
  - "採番"の項目は統合メタデータ管理基盤で自動入力される値。
  - "add"はaddメソッド実行時に入力が必須の項目。

- カラム統計情報オブジェクト

  | # | キー名 | データ型 | 説明 | 採番 | add | 備考 |
  |---|---|---|---|---|---|---|
  | 1. | id*              | int64_t | カラム統計情報オブジェクトのOID。 | ○  | -  | -  |
  | 2. | name*            | string  | カラム名。                        | -   | -  | 任意の名前を設定可能。 |
  | 3. | ordinalPosition  | int64_t | カラム番号。                      | -   | ○ | -  |
  | 4. | columnStatistic  | string  | カラム統計情報。                  | -   | -  | -  |

  - *はすべてのメタデータオブジェクトに含まれるパラメータであることを示す。
  - "採番"の項目はadd()メソッド実行時に値を入力しても無視される。
  - "add"はaddメソッド実行時に入力が必須の項目。

#### 統計情報オブジェクト例

JSON形式での記述イメージ。

- 例１）すべてのパラメータが埋まっている場合

  ```json
  {
    "foramtVersion": 1,
    "generation": 1,
    "id": 2,
    "name": "foo_table",
    "tableId": 2,
    "tableName": "foo_table",
    "relTuples": 10000,
    "columns": [
      {"id": 3, "name": "col1", "ordinalPosition": 1, "columnStatistic": {...}  },
      {"id": 4, "name": "col2", "ordinalPosition": 2, "columnStatistic": {...}  },
      {"id": 5, "name": "col3", "ordinalPosition": 3, "columnStatistic": {...}  },
    ]
  }
  ```

- 例２）addメソッド実行時（最低限必要なパラメータ）

  ```json
  {
    "tableId": 2,
    "columns": [
      {"id": 3, "ordinalPosition": 1 },
      {"id": 4, "ordinalPosition": 2 },
      {"id": 5, "ordinalPosition": 3 },
    ]
  }
  ```

- 例３）例２実行後にgetメソッドを実行した場合

  ```json
  {
    "foramtVersion": 1,
    "generation": 1,
    "id": 2,
    "name": "",
    "tableId": 2,
    "relTuples": 10000,
    "columns": [
      {"id": 3, "name": "col1", "ordinalPosition": 1, "columnStatistic": {...}  },
      {"id": 4, "name": "col2", "ordinalPosition": 2, "columnStatistic": {...}  },
      {"id": 5, "name": "col3", "ordinalPosition": 3, "columnStatistic": {...}  },
    ]
  }
  ```

### 追加するメソッド

- クラス名: manager::metadata::Statistics

- addメソッド

  2種類のaddメソッドを追加する。

  ```text
    ErrorCode add(boost::property_tree::ptree& object) override;
    ErrorCode add(boost::property_tree::ptree& object, ObjectIdType* object_id) override;
  ```

  - 概要

    統計情報オブジェクト(object)を統計情報メタデータテーブルに追加する。

    - 追加したオブジェクトに割り当てられたオブジェクトID(object_id)を返す
    - 統計情報オブジェクトの tableId または tableName のどちらかの入力が必須。

  - 戻り値

    成功した場合はErrorCode::OKを返す。失敗した場合はエラーコードを返す。

  - 想定するユースケース

    - 初めてテーブルにデータの追加／削除／更新が行われた後にOLTPが統計情報を計算したとき

- getメソッド

  2種類のgetメソッドを追加する。

  ```text
        virtual ErrorCode get(const ObjectIdType object_id, boost::property_tree::ptree& object);
        virtual ErrorCode get(std::string_view object_name, boost::property_tree::ptree& object);
  ```

  - 概要

    指定されたキー名（オブジェクトIDまたはオブジェクト名）

  - 想定するユースケース

    - Tsurugi起動時
    - OLTP/OLAPがDML文のプランニング／オプティマイズをするとき

- remove()

  2種類のremove()メソッドを追加する。

各メソッドの戻り値は既存メソッドと同様である。

## 内部設計


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

以上

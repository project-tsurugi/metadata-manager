【Project-Tsurugi Intern User Only】

# table_testについて

2021.12.07 KCC

## JSON版

### 説明

JSONファイル版の流用元となるV2.0のテストを移植したレグレッションテストの意味合いが強いテスト。  
テストは、戻り値の検査はテストモジュール内で実施し、データ内容については`expected`配下の期待値データと手動で比較する。

- oid：テーブルメタデータのオブジェクトIDの採番データファイル比較用
- tables.json：テーブルメタデータ格納ファイル比較用
- result.txt：テスト内で標準出力に出力した内容の比較用  
  ※標準出力の結果を比較するため、`table_test`を実行する際に標準出力をファイルにリダイレクトする必要がある。  
  ※`expected`内の`diff.sh`を用いて比較を行う場合は、標準出力をリダイレクトしたファイルは`output/result.txt`とする。

### テスト前提条件

- メタデータ格納ディレクトリ（~/.local/tsurugi/metadata/）にアクセス可能であること。
- メタデータ格納ディレクトリ内にデータファイルが存在しないこと。

### コマンド実行例

```sh
cd manager/build.json
rm ~/.local/tsurugi/metadata/*
output/table_test > output/result.txt
cd ../metadata-manager/test/v4.0/json_extended/expected
./diff.sh
cd -
```

※5行目の`./diff.sh`で差分が表示されなければテストOKとなる。

### テスト内容

#### 正常系

##### テーブルメタデータ追加・取得

  1. テーブルメタデータの追加（`Tables::add()`）
     - 手順
       - テーブルメタデータ（[テストデータ①](#テストデータ①)）を追加する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
  2. テーブル名をキーとしたテーブルメタデータの取得（`Tables::get()`）
     - 手順
       - テーブル名をキーにテーブルメタデータを取得する。
       - 取得したテーブルメタデータの内容を出力[^1]する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
  3. テーブルIDをキーとしたテーブルメタデータの取得（`Tables::get()`）
     - 手順
       - テーブルIDをキーにテーブルメタデータを取得する。
       - 取得したテーブルメタデータの内容を出力[^2]する。
     - 期待値
       - 戻り値がErrorCode::OKであること。

##### テーブル名によるテーブルメタデータ削除

  1. テーブルメタデータの追加（`Tables::add()`）
     - 手順
       - テーブルメタデータ（[テストデータ①](#テストデータ①)）を **5件** 追加する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
  2. テーブルメタデータの削除（`Tables::remove()`）
     - 手順
       - テーブル名をキーとして、テーブルメタデータを下記の順番で削除する。  
          4件目→2件目→5件目→1件目→3件目
     - 期待値
       - 戻り値がErrorCode::OKであること。

##### テーブルIDによるテーブルメタデータ削除

  1. テーブルメタデータの追加（`Tables::add()`）
     - 手順
       - テーブルメタデータ（[テストデータ①](#テストデータ①)）を **6件** 追加[^3]する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
  2. テーブルメタデータの削除（`Tables::remove()`）
     - 手順
       - テーブルIDをキーとして、テーブルメタデータを下記の順番で削除する。  
          4件目→2件目→5件目→1件目→3件目
     - 期待値
       - 戻り値がErrorCode::OKであること。

##### データタイプIDによるデータタイプメタデータ取得

  1. INT32のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[4]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"INT32"であること。
  2. INT64のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[6]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"INT64"であること。
  3. FLOAT32のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[8]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"FLOAT32"であること。
  4. FLOAT64のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[9]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"FLOAT64"であること。
  5. CHARのデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[13]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"CHAR"であること。
  6. VARCHARのデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - IDに[14]を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのnameが"VARCHAR"であること。

##### データタイプ名によるデータタイプメタデータ取得

  1. INT32のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"INT32"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[4]であること。
  2. INT64のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"INT64"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[6]であること。
  3. FLOAT32のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"FLOAT32"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[8]であること。
  4. FLOAT64のデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"FLOAT64"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[9]であること。
  5. CHARのデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"CHAR"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[13]であること。
  6. VARCHARのデータタイプメタデータ取得（`DataTypes::get()`）
     - 手順
       - データタイプ名に"VARCHAR"を指定してデータタイプメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 取得したデータタイプメタデータのidが[14]であること。

[^1]: 手動で出力内容の確認に使用
[^2]: 手動で出力内容の確認に使用
[^3]: 削除しない1件は、手動でテーブルメタデータファイル内容の確認に使用

#### 準正常系

##### 存在しないテーブル名のテーブルメタデータ削除

  1. テーブルメタデータの削除（`Tables::remove()`）
     - 手順
        - 存在しないテーブル名のテーブルメタデータを削除する。
     - 期待値
       - 戻り値がErrorCode::OK以外であること。

##### 存在しないテーブルIDのテーブルメタデータ削除

  1. テーブルメタデータの削除（`Tables::remove()`）
     - 手順
        - 存在しないテーブルIDのテーブルメタデータを削除する。
     - 期待値
       - 戻り値がErrorCode::OK以外であること。

## PostgreSQL版

### 説明

Google Testを用いない簡易的な導通用のテスト。  
テストおよび結果の検査は、テストモジュール内で実施する。

### テスト前提条件

- postgresユーザで実施すること。

### コマンド実行例

```sh
cd manager/build.pg
output/tables_test
```

※テスト結果が`Success`となっていればOKとなる。

### テスト内容

#### 正常系

##### テーブルメタデータ追加・取得

  1. テーブルメタデータの追加（`Tables::add()`）
     - 手順
       - テーブルメタデータ（[テストデータ②](#テストデータ②)）を追加する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
  2. テーブルIDをキーとしたテーブルメタデータの取得（`Tables::get()`）
     - 手順
       - テーブルIDをキーにテーブルメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 追加したメタデータと取得したメタデータに差異がないこと。
  3. テーブル名をキーとしたテーブルメタデータの取得（`Tables::get()`）
     - 手順
       - テーブル名をキーにテーブルメタデータを取得する。
     - 期待値
       - 戻り値がErrorCode::OKであること。
       - 追加したメタデータと取得したメタデータに差異がないこと。
  4. テーブル名をキーとしてテーブルメタデータを削除する。（`Tables::remove()`）
     - 戻り値がErrorCode::OKであること。

## 別紙：テストデータ

### テストデータ①

|フィールド|||設定値|
|---|---|---|---|
|formatVersion|||1|
|generation|||1|
|name|||"table_"＋*テーブルID*|
|namespace|||*NULL*|
|primaryKey|[0]||1|
||[1]||2|
|tuples|||*NULL*|
|columns|[0]|name|"column_1"|
|||ordinalPosition|1|
|||dataTypeId|8 *(FLOAT32)*|
|||nullable|false|
|||defaultExpr|*NULL*|
|||direction|1 *(ASCENDANT)*|
||[1]|name|"column_2"|
|||ordinalPosition|2|
|||dataTypeId|14 *(VARCHAR)*|
|||dataLength|8|
|||varying|true|
|||nullable|false|
|||defaultExpr|*NULL*|
|||direction|0 *(DEFAULT)*|
||[2]|name|"column_3"|
|||ordinalPosition|3|
|||dataTypeId|13 *(CHAR)*|
|||dataLength|1|
|||varying|false|
|||nullable|true|
|||defaultExpr|*NULL*|
|||direction|0 *(DEFAULT)*|

### テストデータ②

|フィールド|||設定値|
|---|---|---|---|
|formatVersion|||1|
|generation|||1|
|name|||"UTex_test_table_name_"＋ *\_\_LINE\_\_*|
|namespace|||"namespace"|
|primaryKey|[0]||1|
|tuples|||*NULL*|
|columns|[0]|name|"col-1"|
|||ordinalPosition|1|
|||dataTypeId|6 *(INT64)*|
|||dataLength|*NULL*|
|||varying|*NULL*|
|||nullable|true|
|||defaultExpr|*NULL*|
|||direction|*NULL*|

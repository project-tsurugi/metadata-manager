【Project-Tsurugi Intern User Only】

# 認証管理基盤 ユーザ認証機能 詳細設計

2021.11.11 NEC

## 目次

- [認証管理基盤 ユーザ認証機能 詳細設計](#認証管理基盤-ユーザ認証機能-詳細設計)
  - [目次](#目次)
  - [外部設計](#外部設計)
    - [ライブラリ](#ライブラリ)
    - [Authenticationクラス](#authenticationクラス)
      - [auth_userメソッド](#auth_userメソッド)
  - [内部設計](#内部設計)
    - [ネームスペース一覧](#ネームスペース一覧)
    - [クラス一覧](#クラス一覧)
    - [クラス図](#クラス図)
    - [シーケンス図](#シーケンス図)
    - [Authenticationクラス](#authenticationクラス-1)
      - [auth_userメソッド](#auth_userメソッド-1)
    - [AuthenticationProviderクラス](#authenticationproviderクラス)
      - [auth_userメソッド](#auth_userメソッド-2)
    - [DBSessionManagerクラス](#dbsessionmanagerクラス)
      - [PostgreSQL](#postgresql)
      - [attempt_connectionメソッド](#attempt_connectionメソッド)
  - [その他](#その他)
    - [ライブラリ名](#ライブラリ名)
    - [ビルド方法](#ビルド方法)

## 外部設計

### ライブラリ

> 認証管理基盤ライブラリ（`libmanager-authentication.so`）

### Authenticationクラス

```C++
// Header <manager/authentiction/authentication.h>

namespace manager::authentiction {
  class Authentication {
    static ErrorCode auth_user(const boost::property_tree::ptree& params);
    static ErrorCode auth_user(std::string_view conninfo);
  }
}
```

#### auth_userメソッド

- **概要**  
  入力されたDB接続パラメータが有効であるかを確認する。

- **書式**

  ```C++
  static ErrorCode auth_user(const boost::property_tree::ptree& params);
  static ErrorCode auth_user(std::string_view conninfo);
  ```

- **引数**
  - `params`  
      ptree形式で指定した接続パラメータ。  
      参考：[(PostgreSQL) パラメータキーワード](https://www.postgresql.jp/document/12/html/libpq-connect.html#LIBPQ-PARAMKEYWORDS)
  - `conninfo`  
      DBに依存した接続文字列の形式で指定した接続パラメータ。  
      参考：[(PostgreSQL) 接続文字列](https://www.postgresql.jp/document/12/html/libpq-connect.html#LIBPQ-CONNSTRING)

- **戻り値**
  - `ErrorCode::OK`  
    入力された接続情報が有効である場合。
  - `ErrorCode::AUTHENTICATION_FAILURE`  
    入力された接続情報が無効である場合。
  - `ErrorCode::CONNECTION_FAILURE`  
    データベースへの接続に失敗した場合。

- **使用例**
  - 接続情報確認API（パラメータキーワード）

    ```C++
    boost::property_tree::ptree params;
    params.put("user", "foo");
    params.put("password", "secret");
    params.put("host", "localhost");
    params.put("port", "5432");
    params.put("dbname", "tsurugi_db");

    ErrorCode error = Authentication::auth_user(params);
    if (error != ErrorCode::OK) {
      ...
    }  
    ```

  - 接続情報確認API（接続文字列）

    ```C++
    std::string conninfo = "postgresql://foo:secret@localhost:5432/tsurugi_db";

    ErrorCode error = Authentication::auth_user(conninfo);
    if (error != ErrorCode::OK) {
      ...
    }  
    ```

    ```C++
    std::string conninfo = "user=foo password=secret host=localhost port=5432 dbname=tsurugi_db";

    ErrorCode error = Authentication::auth_user(conninfo);
    if (error != ErrorCode::OK) {
      ...
    }  
    ```

---

## 内部設計

### ネームスペース一覧

|ネームスペース名|概要|
|---|---|
|`manager::authentication`|サービス層ネームスペース|
|`manager::authentication::db`|DAO用ネームスペース|
|`manager::authentication::db::postgresql`|PostgreSQL用ネームスペース|

### クラス一覧

|クラス/構造体/列挙体|ヘッダファイル|概要|依存|
|---|---|---|---|
|`manager::authentication::Authentication`|`authentication.h`|ユーザ認証クラス（API）|-----|
|`manager::authentication::db::AuthenticationProvider`|`provider/authentication_provider.h`|ユーザ認証プロバイダクラス|-----|
|`manager::authentication::db::postgresql::DBSessionManager`|`dao/postgresql/db_session_manager.h`|DBセッション管理クラス|PostgreSQL|

### クラス図

![クラス図](./out/uml/ClassDiagram_overview.png)

### シーケンス図

![シーケンス図](./out/uml/SequenceDiagram_authentication.png)

### Authenticationクラス

```C++
// Header <manager/authentiction/authentication.h>

namespace manager::authentiction {
  class Authentication {
    static ErrorCode auth_user(const boost::property_tree::ptree& params);
    static ErrorCode auth_user(std::string_view conninfo);
  }
}
```

#### auth_userメソッド

- **概要**  
  入力されたDB接続パラメータが有効であるかを確認する。

- **書式**

  ```C++
  static ErrorCode auth_user(const boost::property_tree::ptree& params);
  static ErrorCode auth_user(std::string_view conninfo);
  ```

- **引数**
  - `params`  
      ptree形式で指定した接続パラメータ。
  - `conninfo`  
      DBに依存した接続文字列の形式で指定した接続パラメータ。

- **戻り値**
  - `ErrorCode::OK`  
    入力された接続情報が有効である場合。
  - `ErrorCode::AUTHENTICATION_FAILURE`  
    入力された接続情報が無効である場合。
  - `ErrorCode::CONNECTION_FAILURE`  
    データベースへの接続に失敗した場合。

- **処理詳細**
  1. [AuthenticationProvider](#authenticationproviderクラス)::[auth_user()](#auth_userメソッド-1)を実行する。
  2. 「1.」の実行結果を戻り値として処理を終了する。

### AuthenticationProviderクラス

```C++
// Header <manager/authentication/provider/authentication_provider.h>

namespace manager::authentication::db {
  class AuthenticationProvider {
    static ErrorCode auth_user(const boost::property_tree::ptree& params);
    static ErrorCode auth_user(std::string_view conninfo);
  }
}
```

#### auth_userメソッド

- **概要**  
  入力されたDB接続パラメータが有効であるかを確認する。

- **書式**

  ```C++
   static ErrorCode auth_user(const boost::property_tree::ptree& params); 
   static ErrorCode auth_user(std::string_view conninfo);
   ```

- **引数**
  - `params`  
   ptree形式で指定した接続パラメータ。
  - `conninfo`  
   DBに依存した接続文字列の形式で指定した接続パラメータ。

- **戻り値**
  - `ErrorCode::OK`  
    入力された接続情報が有効である場合。
  - `ErrorCode::AUTHENTICATION_FAILURE`  
    入力された接続情報が無効である場合。
  - `ErrorCode::CONNECTION_FAILURE`  
    データベースへの接続に失敗した場合。

- **処理詳細**
  1. 引数の型が「`boost::property_tree::ptree`」である場合は、[DBSessionManager](#dbsessionmanagerクラス)::[attempt_connection()](#attempt_connectionメソッド)を実行する。
  2. 引数の型が「`std::string_view`」である場合は、当該引数値を`boost::property_tree::ptree`型の接続情報パラメータに設定（キー名：`connection_strings`）し、[DBSessionManager](#dbsessionmanagerクラス)::[attempt_connection()](#attempt_connectionメソッド)を実行する。
  3. 「1.」または「2.」の実行結果を戻り値として処理を終了する。

### DBSessionManagerクラス

#### PostgreSQL

```C++
// Header <manager/authentication/dao/postgresql/db_session_manager.h>

namespace manager::authentication::db::postgresql {
  class DBSessionManager {
    static ErrorCode attempt_connection(const boost::property_tree::ptree& params);
  }
}
```

#### attempt_connectionメソッド

- **概要**  
  入力されたDB接続パラメータを用いて、DBへの接続を試行した結果を返す。

- **書式**

  ```C++
   static ErrorCode attempt_connection(const boost::property_tree::ptree& params);
   ```

- **引数**
  - `params`  
   接続パラメータ。

- **戻り値**
  - `ErrorCode::OK`  
    入力された接続情報が有効である場合。
  - `ErrorCode::AUTHENTICATION_FAILURE`  
    入力された接続情報が無効である場合。
  - `ErrorCode::CONNECTION_FAILURE`  
    データベースへの接続に失敗した場合。

- **処理詳細**
  1. 入力された接続パラメータに接続文字列（キー名：`connection_strings`）が設定されてる場合は、当該値を接続文字列[^1]として取得する。
    指定されていない場合は、接続パラメータに指定されている各情報を接続文字列（`keyword = value`形式）に変換する。
  2. 「1.」で取得／変換した接続文字列を引数に指定し`PQping()`関数を実行する。
  3. 実行結果（戻り値）が［`PQPING_OK`］以外の場合は［`ErrorCode::CONNECTION_FAILURE`］を戻り値として処理を終了し、［`PQPING_OK`］の場合は処理を継続する。
  4. 「1.」で取得／変換した接続文字列を引数に指定し`PQconnectdb()`関数を実行する。
  5. 実行結果（`PQstatus()`関数の戻り値）が［`CONNECTION_OK`］の場合は［`ErrorCode::OK`］、
    それ以外の場合は［`ErrorCode::AUTHENTICATION_FAILURE`］を戻り値として処理を終了する。

  [^1]: 文字列形式（`keyword = value ...`）やURI形式（RFC3986準拠）など

---

## その他

### ライブラリ名

> 認証管理基盤ライブラリ（`libmanager-authentication.so`）

### ビルド方法

cmakeにて「`BUILD_TARGET`」オプションを指定する。

- `ALL` - 統合メタデータ管理基盤および認証管理基盤（デフォルト）
- `AUTH` - 認証管理基盤のみ
- `METADATA` - 統合メタデータ管理基盤のみ

コマンド例：

```shell
◆認証管理基盤ライブラリのみ
cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TARGET=AUTH -H. -B./build.auth
cmake --build ./build.auth
```

```shell
◆統合メタデータ管理基盤、認証管理基盤ライブラリ
cmake-DCMAKE_BUILD_TYPE=Debug -H. -B./build.all
cmake --build ./build.all
```

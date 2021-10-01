【Project-Tsurugi Internal Use Only】

# 認証管理基盤 ユーザ認証機能仕様書

2021.09.15 NEC 初版

## 概要

### 想定するユースケース

ユーザはあらかめじPostgreSQLにユーザ（ロール）を作成しておく。  
ユーザはOLTPから直接バッチを実行したい場合は、あらかじめ作成したPostgreSQLのユーザ情報を付記してOLTPにバッチの実行を要求する。OLTPは内部で認証マネージャ（AuthManager）にユーザ情報の正当性の確認を要求し、正当なユーザであることが確認できた場合は、要求されたバッチを実行する。

![auth_user](images/auth_man/auth_user2.svg)

## authentication-manager

- 概要

  ユーザ認証機能を提供する。

- 名前空間

  manager::authentiction

## ユーザ認証API

### Authentication::auth_user

- 書式

  `static ErrorCode auth_user(boost::property_tree::ptree params);`  
  `static ErrorCode auth_user(std::string_view conninfo);`  

- 概要

  入力された接続情報が有効であるか確認する。

- パラメータ

  - params

    PostgreSQLにおけるパラメータキーワード。
    - <https://www.postgresql.jp/document/12/html/libpq-connect.html#LIBPQ-PARAMKEYWORDS>

  - conninfo
  
    PostgreSQLにおける接続文字列。
    - <https://www.postgresql.jp/document/12/html/libpq-connect.html#LIBPQ-CONNSTRING>

- 戻り値

  - ErrorCode::OK

    入力された接続情報が有効である。

  - ErrorCode::AUTHENTICATION_FAILURE

    入力された接続情報が無効である。

  - ErrorCode::CONNECTION_FAILURE

    データベース（PostgreSQL）への接続に失敗した。

- 使用例

  - 例１）パラメータキーワードによる有効性確認

    ```c++
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

  - 例２）接続文字列による有効性確認

    ```c++
      std::string conninfo = "postgresql://foo:secret@localhost:5432/tsurugi_db";
      ErrorCode error = Authentication::auth_user(conninfo);
      if (error != ErrorCode::OK) {
        ...
      }

    ```

以上

---

## 付録

### A. pg_hba.confファイル

<https://www.postgresql.jp/document/12/html/auth-pg-hba-conf.html>

1行につき1つのレコードが記述される。レコードはスペースまたはタブ、もしくはその両方で区切られた複数のフィールドで構成される。

それぞれのレコードは接続方式、クライアントのIPアドレス範囲、データベースの名前、ユーザ名およびこれらのパラメータに一致する接続で使用される認証方法を指定する。

- 基本的な書式

  ```text
  # TYPE  DATABASE    USER    ADDRESS               METHOD
  local   database    user                          auth-method [auth-options]
  host    database    user    address               auth-method [auth-options]
  host    database    user    IP-address  IP-mask   auth-method [auth-options]
  ```

- 接続方式

  大きくはlocalとhostに分かれる。
  - local

    Unixドメインソケット

  - host

    TCP/IPを使用した接続（SSLまたは非SSL接続、GSSAPI暗号化、非GSSAPI暗号化）

    - hostssl

      TCP/IPを使用した接続（SSL接続）

    - hostnossl

      TCP/IPを使用した接続（非SSL接続）

    - hostgssenc

      TCP/IPを使用した接続（GSSAPI暗号化）

- データベースの名前

  レコードで対応するデータベース名。

  - all
  
    すべてのデータベースに対応することを指定する。

  - sameuser

    要求されたデータベースが要求ユーザと同じ名前を持つ場合にレコードが対応することを指定する

  - samerole

    要求ユーザが要求されたデータベースと同じ名前のロールのメンバでなければならないことを指定する。

  - replication

    物理レプリケーション接続が要求された場合にレコードが一致することを指定する。この場合は特定のデータベースを指定しない。

- ユーザ名

  データベースユーザ名（ロール名）を指定する。カンマで区切ることによって複数指定できる。

  - all

    すべてのユーザが対応することを指定する。

  - （+で始まるグループ名（ロール名））

    +のマークは、「この－ルーの直接的もしくは間接的なメンバのどちらかに一致していること」を意味する。

- クライアントのIPアドレスまたはホスト名

  このレコードに対応しているクライアントマシンのアドレス。このフィールドはホスト名、IPアドレスの範囲、もしくは下記の特別なキーワードの1つを含んでいる。

  IPアドレスの範囲は、範囲の開始アドレス、続いてスラッシュとCIDRマスクの長さという標準の数値表記で指定される。
  IPv4アドレスとIPv6アドレスを記述できる。

  - all

    どのIPアドレスにも一致することを指定する。

  - samehost

    サーバ自身のIPアドレスの井塚に一致することを指定する。

  - samenet

    サーバが直接接続されているサブネット内のアドレスのいずれかに一致することを指定する。

- 認証方法

  |#| 認証方法  | 条件  |  分類 | 説明  |
  |---|---|---|---|---|
  |1. | reject        | -       | -  | 無条件で接続を拒否する。  |
  |2. | trust         | -       | trust認証  | 無条件で接続を許可する。  |
  |3. | scram-sha-256 | -       | パスワード認証  | SCRAM-SHA-256認証。      |
  |4. | md5           | -       | パスワード認証  | MD5ハッシュアルゴリズム。 |
  |5. | password      | -       | パスワード認証  | 平文パスワード。          |
  |6. | gss           | host    | GSSAPI認証  | RFC2743で定義されている安全な認証のためのプロトコル。    |
  |7. | sspi          | -       | SSPI認証    | シングルサインオンで安全な認証を行うためのWindowsの技術。 |
  |8. | ident         | host    | Ident認証   | クライアントのOSのユーザ名をidentサーバから入手し、それを許可されているデータベースのユーザ名として使用する。 |
  |9. | peer          | local   | Peer認証    | カーネルからクライアント上のOSのユーザ名を取得し、それをデータベースユーザ名として使用する。
  |10.| ldap          | -       | LDAP認証（パスワード認証）    | パスワード確認にLDAPを使用する。  |
  |11.| radius        | -       | RADIUS認証（パスワード認証）  | RADIUSをパスワード検証として使用する。  |
  |12.| cert          | hostssl | 証明書認証  | SSLクライアント証明書を使用する。 |
  |13.| pam           | -       | PAM認証（パスワード認証） | PAM(Pluggable Authentication Modules)を使用する。 |
  |14.| bsd           | -       | BSD認証（パスワード認証） | BSD認証を使用する。 |

以上

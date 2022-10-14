# ユーザ管理機能 ROLE機能設計

2021.07.30 NEC

- 今回開発するバージョンは、V4.0とします。

## 目次

<!-- @import "[TOC]" {cmd="toc" depthFrom=1 depthTo=6 orderedList=false} -->

<!-- code_chunk_output -->

- [ユーザ管理機能 ROLE機能設計](#ユーザ管理機能-role機能設計)
  - [目次](#目次)
  - [目的](#目的)
  - [基本方針](#基本方針)
  - [サポートするSQLコマンド](#サポートするsqlコマンド)
    - [全体のシーケンス概要](#全体のシーケンス概要)
    - [構文・型・概念スキーマ](#構文型概念スキーマ)
      - [サポートするCREATE ROLE DDL構文・型](#サポートするcreate-role-ddl構文型)
      - [サポートするDROP ROLE DDL構文・型](#サポートするdrop-role-ddl構文型)
      - [サポートするALTER ROLE DDL構文・型](#サポートするalter-role-ddl構文型)
      - [作成される ROLE 概念スキーマ](#作成される-role-概念スキーマ)
  - [改造・追加内容](#改造追加内容)
    - [frontend](#frontend)
      - [alt_utility変更概要](#alt_utility変更概要)
        - [alt_utilityインターフェース](#alt_utilityインターフェース)
    - [manager](#manager)
      - [message-broker変更概要](#message-broker変更概要)
        - [send_messageメソッド](#send_messageメソッド)
        - [Message](#message)
          - [Message説明](#message説明)
          - [Messageフィールド](#messageフィールド)
        - [set_receiverメソッド](#set_receiverメソッド)
          - [派生Receiverクラス一覧](#派生receiverクラス一覧)
        - [Receiver](#receiver)
          - [Receiver説明](#receiver説明)
          - [receive_messageメソッド](#receive_messageメソッド)
      - [metadata-manager](#metadata-manager)
        - [metadata-managerインターフェース](#metadata-managerインターフェース)
        - [roleオブジェクトの取得データ](#roleオブジェクトの取得データ)
      - [参考](#参考)
        - [Status](#status)
          - [説明](#説明)
          - [フィールド](#フィールド)

<!-- /code_chunk_output -->

## 目的

- ユーザ管理のため、ROLE機能をTABLE/VIEW/PROCEDUREに追加する。

## 基本方針

- ここでは、ユーザ管理機能のROLE処理(追加・削除・変更)の追加のみ記載する。
- DDL構文・型・概念スキーマは、既存のv3.0やPostgreSQLから変更はしない。
- PostgreSQL12を基準とする。

## サポートするSQLコマンド

以下の3個のDDLを実装する。

| #   | DDL         | 説明                   | 備考 |
| --- | ----------- | ---------------------- | ---- |
| 1   | CREATE ROLE | ロールを作成する       |      |
| 2   | DROP ROLE   | ロールを削除する       |      |
| 3   | ALTER ROLE  | ロールの属性を変更する |      |

### 全体のシーケンス概要

全体のシーケンスにDDL用の処理を追加する。

![全体シーケンス概要](./images/UserManagement_ROLE_FD/entire_seq.svg)

### 構文・型・概念スキーマ

ROLEのDDL構文・型は、PostgreSQLのDDL構文・型を使用する。

#### サポートするCREATE ROLE DDL構文・型

<https://www.postgresql.jp/document/12/html/sql-createrole.html>

#### サポートするDROP ROLE DDL構文・型

<https://www.postgresql.jp/document/12/html/sql-droprole.html>

#### サポートするALTER ROLE DDL構文・型

<https://www.postgresql.jp/document/12/html/sql-alterrole.html>

#### 作成される ROLE 概念スキーマ

PostgreSQLのpg_authid型でCREATEなどで作成される。

詳細は、pg_authidを確認してください。
<https://www.postgresql.jp/document/12/html/catalog-pg-authid.html>

## 改造・追加内容

### frontend

#### alt_utility変更概要

以下のメソッドでHook箇所をNode TAG毎に追加する。
Hook後に使用する作成(create_role)・削除(drop_role)・変更(alter_role)の処理を追加する。

- tsurugi_ProcessUtility
- tsurugi_ProcessUtilitySlow

| #   | DDL         | メッセージ    | Node TAG         | 備考 |
| --- | ----------- | ------------- | ---------------- | ---- |
| 1   | CREATE ROLE | "CREATE ROLE" | T_CreateRoleStmt |      |
| 2   | DROP ROLE   | "DROP ROLE"   | T_DropRoleStmt   |      |
| 3   | ALTER ROLE  | "ALTER ROLE"  | T_AlterRoleStmt  |      |

##### alt_utilityインターフェース

ユーザ管理用のインタフェースとしてfrontendのalt_utilityに新たに以下を追加する。

- create_roleメソッド
  - bool create_role(List *stmts)
    - 処理概要
      - PostgreSQLへロール作成する。
      - gawayamaへメッセージを送る。
    - 戻値
      | 型   | 値                                                                |
      | ---- | ----------------------------------------------------------------- |
      | bool | ROLE作成の結果を返却する。<br>成功の場合:TRUE、失敗の場合 : FALSE |
    - 引数
      | 型    | 変数  | 値            |
      | ----- | ----- | ------------- |
      | List* | stmts | SQL文のリスト |
- drop_roleメソッド
  - bool drop_role(List *stmts)
    - 処理概要
      - PostgreSQLのロールを削除する。
      - gawayamaへメッセージを送る。
    - 戻値
      | 型   | 値                                                                |
      | ---- | ----------------------------------------------------------------- |
      | bool | ROLE削除の結果を返却する。<br>成功の場合:TRUE、失敗の場合 : FALSE |
    - 引数
      | 型    | 変数  | 値            |
      | ----- | ----- | ------------- |
      | List* | stmts | SQL文のリスト |
- alter_roleメソッド
  - bool alter_role(List *stmts)
    - 処理概要
      - PostgreSQLのロールの属性を変更する。
      - gawayamaへメッセージを送る。
    - 戻値
      | 型   | 値                                                                      |
      | ---- | ----------------------------------------------------------------------- |
      | bool | ROLEの属性変更の結果を返却する。<br>成功の場合:TRUE  失敗の場合 : FALSE |
    - 引数
      | 型    | 変数  | 値            |
      | ----- | ----- | ------------- |
      | List* | stmts | SQL文のリスト |

### manager

#### message-broker変更概要

v3.0から変更有

- メッセージに以下のROLEのDDLのメッセージを追加する。

| #   | DDL         | メッセージ    | 備考 |
| --- | ----------- | ------------- | ---- |
| 1   | CREATE ROLE | "CREATE ROLE" |      |
| 2   | DROP ROLE   | "DROP ROLE"   |      |
| 3   | ALTER ROLE  | "ALTER ROLE"  |      |

##### send_messageメソッド

v3.0からインタフェースの変更なし。

- Status send_message(Message* message)
  - 処理内容：MessageBrokerは、Messageクラスにセットされたすべての派生Receiverに対して、receive_message()メソッドでメッセージを送信する。
  - 条件
    - 事前条件：Messageクラスのすべてのフィールドがセットされている。
    - 事後条件：
      - 派生Receiverが返した概要エラーコードが「FAILURE」である場合、「FAILURE」が返ってきた時点で即座に、Statusクラスのインスタンスを返す。
      - すべての派生Receiverが返した概要エラーコードが「SUCCESS」である場合、Statusクラスのコンストラクタに次の値をセットして返す。
          | フィールド名   | 値           |
          | -------------- | ------------ |
          | error_code     | SUCCESS      |
          | sub_error_code | (int)SUCCESS |
      - 詳細は[Status](#status)を参照。

##### Message

###### Message説明

- メッセージの内容、メッセージの受信者である派生Receiverリストを保持する。

###### Messageフィールド

| フィールド名      | 説明                                                                                         |
| ----------------- | -------------------------------------------------------------------------------------------- |
| id                | メッセージID。ユーザーが入力した構文に応じて、すべての派生Receiverにその構文を伝えるためID。 |
| object_id         | 追加・更新・削除される対象のオブジェクトID 例）テーブルメタデータのオブジェクトID            |
| receivers         | メッセージの受信者である派生Receiverリスト。例）OltpReceiver、OlapReceiver                   |
| message_type_name | エラーメッセージ出力用の文字列　例）"CREATE TABLE"                                           |

- id
  - 型:列挙型(enum class)
    - 規定型:int
    - 次の通り管理する。
      - リポジトリ名：manager/message-broker
      - 名前空間：manager::message
    - メッセージID一覧
      | メッセージID    | ユーザーが入力した構文 |
      | --------------- | ---------------------- |
      | CREATE_TABLE    | CREATE TABLE構文       |
      | **CREATE_ROLE** | **CREATE ROLE構文**    |
      | **DROP_ROLE**   | **DROP ROLE構文**      |
      | **ALTER_ROLE**  | **ALTER ROLE構文**     |

##### set_receiverメソッド

v3.0からインタフェースの変更なし。

- void set_receiver(Receiver *receiver_)
  - Messageクラスの派生Receiverリストに、派生Receiverをセットする。

###### 派生Receiverクラス一覧

| クラス名              | ユーザーが入力した構文 |
| --------------------- | ---------------------- |
| CreateTableMessage    | CREATE TABLE構文       |
| **CreateRoleMessage** | **CREATE ROLE構文**    |
| **DropRoleMessage**   | **DROP ROLE構文**      |
| **AlterRoleMessage**  | **ALTER ROLE構文**     |

##### Receiver

###### Receiver説明

- メッセージを受信する。
- 抽象クラス。

###### receive_messageメソッド

v3.0からインタフェースの変更なし。

- Status receive_message(Message* message)
  - 抽象メソッド。実際の処理は、派生Receiverが行う。
  - 処理内容：派生Receiverは、Messageクラスのインスタンスを受け取り、message->get_id()を利用してMessageIdを取得する。MessageIdに応じて処理の実行または、実行を指示する。get_object_id()を利用して、追加・更新・削除される対象のオブジェクトIDを取得する。
  - 条件
    - 事前条件：なし
    - 事後条件：
      - 派生Receiverは、MessageIdに対応する処理を実行後、Statusクラスのインスタンスを生成する。このとき、コンストラクタで概要エラーコード・詳細エラーコードをセットする。生成したStatusクラスのインスタンスを返す。
      - 詳細は[Status](#status)を参照。

#### metadata-manager

v3.0から変更あり。

##### metadata-managerインターフェース

以下のmetadata.hのクラスをオーバライドする。
ROLEの追加は、PostgreSQLからのコマンドのみとする。

- init
  - ErrorCode init()
    - DBアクセスを初期化する。
- load
  - 何もしない。
- add
  - 何もしない。
- get
  - ErrorCode get(const ObjectIdType object_id, boost::property_tree::ptree& object)
    - object_idに指定されたOIDのROLEデータを取得する。
  - ErrorCode get(std::string_view object_name, boost::property_tree::ptree& object)
    - object_nameに指定されたROLE名のROLEデータを取得する。
- remove
  - 何もしない。

##### roleオブジェクトの取得データ

メソッドによって取得できるROLEデータの内容を以下に示す。

- 取得するデータの項目については、今後使用する可能性を考えて、pg_authidの要素をすべて保持する。
- Metadata内では、ptreeで管理し、データはすべて文字列(string)に変換して保持する。
- ROOT Objectの名前は"ROLES"とする。
  ROOT Objectの名前は定数としては、ROLES_NODEを定義する。

| #   | 名前(key)      | 型(pg_authid) | 型(Metadata) | 定義(pg_authid) | 定義(Metadata) | 説明                                                                                  |
| --- | -------------- | ------------- | ------------ | --------------- | -------------- | ------------------------------------------------------------------------------------- |
| 1   | oid            | oid           | string       | 〇              | 〇             | 識別子                                                                                |
| 2   | rolname        | name          | string       | 〇              | 〇             | ロール名                                                                              |
| 3   | rolsuper       | bool          | string       | 〇              | 〇             | スーパーユーザの権限を持っています                                                    |
| 4   | rolinherit     | bool          | string       | 〇              | 〇             | 自動的にメンバとして属するロールの権限を継承します                                    |
| 5   | rolcreaterole  | bool          | string       | 〇              | 〇             | ロールを作成することができます                                                        |
| 6   | rolcreatedb    | bool          | string       | 〇              | 〇             | データベースを作成することができます                                                  |
| 7   | rolcanlogin    | bool          | string       | 〇              | 〇             | ロールはログインすることができます。                                                  |
| 8   | rolreplication | bool          | string       | 〇              | 〇             | ロールはレプリケーション用のロールです。                                              |
| 9   | rolbypassrls   | bool          | string       | 〇              | 〇             | すべての行単位セキュリティポリシーを無視するロール。                                  |
| 10  | rolconnlimit   | int4          | string       | 〇              | 〇             | ロールが確立できる同時実行接続数。 -1は制限無しを意味します。                         |
| 11  | rolpassword    | text          | string       | 〇              | 〇             | パスワード。無い場合はNULLです。 書式は使用される暗号化の形式に依存します。           |
| 12  | rolvaliduntil  | timestamptz   | string       | 〇              | 〇             | パスワード有効期限（パスワード認証でのみ使用）。 NULLの場合には満了時間はありません。 |

- 懸念事項
  - パスワードについては、設定でどのようにとれるか、確認しながら進める必要がある。
    - 暗号化されていない場合は、取得しないほうが良いと考える。
  - timestamptzについては、Text型に変換して受け取る場合のフォーマットをどうするか。

#### 参考

以下、v3.0からの変更なし。参考として記載しています。

##### Status

###### 説明

- send_message()およびreceive_message()の戻り値

###### フィールド

- フィールド一覧

| フィールド名   | 説明             | エラーコードを管理するリポジトリ名 | エラーコードを管理する名前空間 |
| -------------- | ---------------- | ---------------------------------- | ------------------------------ |
| error_code     | 概要エラーコード | manager/message-broker             | manager::message               |
| sub_error_code | 詳細エラーコード | 派生Receiverが配置されるリポジトリ | 派生Receiverで管理             |

- 概要エラーコードと詳細エラーコードの対応表

| error_code | sub_error_code                                                                                                                                                          |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SUCCESS    | 派生Receiverで管理される成功したときのエラーコードをint型にキャストした値。 例)(int)ogawayama::stub::ErrorCode::OK                                                      |
| FAILURE    | 派生Receiverで管理される成功以外のエラーコードをint型にキャストした値。 例)(int)ogawayama::stub::ErrorCode::UNKNOWN,(int)ogawayama::stub::ErrorCode::SERVER_FAILUREなど |

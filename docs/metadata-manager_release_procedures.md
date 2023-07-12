【Project-Tsurugi Internal User Only】

# `metadata-manager`リリース手順 （検討資料）

2023.07.10 KCC

---

## 目次

- [`metadata-manager`リリース手順 （検討資料）](#metadata-managerリリース手順-検討資料)
  - [目次](#目次)
  - [1 はじめに](#1-はじめに)
    - [1.1 本書の目的](#11-本書の目的)
    - [1.2 前提条件](#12-前提条件)
  - [2 リリースの流れ](#2-リリースの流れ)
  - [3 リリース前手順](#3-リリース前手順)
    - [3.1 ビルド](#31-ビルド)
      - [3.1.1 `metadata-manager`](#311-metadata-manager)
      - [3.1.2 `frontend`](#312-frontend)
      - [3.1.3 `ogawayama`](#313-ogawayama)
    - [3.2 結合テスト](#32-結合テスト)
  - [4 リリース手順](#4-リリース手順)
    - [4.1 リリース手順](#41-リリース手順)
    - [4.2 リリース通知](#42-リリース通知)

---

## 1 はじめに

### 1.1 本書の目的

本書は、Tsurugiのメタデータを管理する`metadata-manager`（統合メタデータ管理基盤）をリリース（メインブランチにコミット）する手順を示す。

### 1.2 前提条件

ビルドやレグレッションテスト、リリース等の各種手順は、各コンポーネントに準ずる。

## 2 リリースの流れ

`metadata-manager`をメインブランチにリリースする際の作業の流れを示す。

```mermaid
sequenceDiagram

actor UsrMetaMng as metadata-manager担当
actor UsrFrontend as frontend担当
actor UsrOgawayama as ogawayama担当
participant Local as 各開発環境
participant Remote as GitHub

rect rgb(255, 255, 255)
  activate UsrMetaMng
    rect rgb(255, 255, 255)
      Note over UsrMetaMng,Local: リリース前の結合テスト
      UsrMetaMng ->> Local: リリース用の metadata-manager をビルド＆インストール
      Local -->> Local: 
      UsrMetaMng ->> Local: リリース用の metadata-manager を用いて<br>frontend, ogawayamaをリビルド＆インストール
      Local -->> Local: 
      UsrMetaMng ->> Local: 結合テスト
      Local -->> Local: 
    end

    Note over UsrMetaMng,Remote: リリース
    UsrMetaMng ->> Remote: metadata-manager をメインブランチにコミット

    rect rgb(255, 255, 255)
      Note over UsrMetaMng,UsrOgawayama: リリース通知
      UsrMetaMng -) UsrFrontend: メタデータ管理基盤の更新を通知
      UsrMetaMng -) UsrOgawayama: メタデータ管理基盤の更新を通知
    end
  deactivate UsrMetaMng

  rect rgb(255, 255, 255)
    par frontend
      activate UsrFrontend
        Note over UsrFrontend,Remote: metadata-manager 更新作業
        UsrFrontend ->> Local: サブモジュールの metadata-manager を最新に更新
        Remote -->> Local: 最新の metadata-manager を取得
        UsrFrontend ->> Local: ビルド・テストなど
        Local -->> Local: 
        UsrFrontend ->> Remote: frontend をメインブランチにコミット
      deactivate UsrFrontend

    and ogawayama
      activate UsrOgawayama
        Note over UsrOgawayama,Remote: metadata-manager 更新作業
        UsrOgawayama ->> Local: サブモジュールの metadata-manager を最新に更新
        Remote -->> Local: 最新の metadata-manager を取得
        UsrOgawayama ->> Local: ビルド・テストなど
        Local -->> Local: 
        UsrOgawayama ->> Remote: ogawayama をメインブランチにコミット
      deactivate UsrOgawayama
    end
  end
end
```

## 3 リリース前手順

### 3.1 ビルド

#### 3.1.1 `metadata-manager`

1. `README`の[`How to build`]に従い、リリース予定の`metadata-manager`をリビルドおよびインストールする。

#### 3.1.2 `frontend`

> ビルド環境がない場合は、事前に`frontend`の`README`に従い、`frontend`のビルド環境を構築する。  
> [`How to build frontend`] > [`Install required packages.`] ~ [`Clone frontend.`]

1. サブモジュールの`metadata-manager`をリリースする`metadata-manager`に入れ替える。

    ```sh
    cd /path/to/frontend
    rm -rf third_party/metadata-manager

    ## e.g., GitHubの metadata-manager リポジトリのブランチと入れ替える場合
    git clone -b <ブランチ名> git@github.com:project-tsurugi/metadata-manager.git third_party/metadata-manager

    ## e.g., 開発環境の metadata-manager と入れ替える場合
    ln -s /path/to/release/metadata-manager third_party/metadata-manager
    ```

2. `frontend`をリビルドおよびインストールする。

   ```sh
   cd /path/to/frontend
   make clean && make
   make install
   ```

#### 3.1.3 `ogawayama`

> ビルド環境がない場合は、事前に`ogawayama`の`README`に従い、`ogawayama`のビルド環境を構築する。  
> [`How to build`]

1. `ogawayama`の`README`に従い、`ogawayama`の取得およびサブモジュールの取得を行う。
2. サブモジュールの`metadata-manager`をリリースする`metadata-manager`に入れ替える。  
   ※入れ替え手順は ［[`2.1.2 frontend`](#212-frontend)］>［`frontend`のビルド環境がある場合］を参照。
3. `ogawayama`の`README`に従い、`ogawayama`をリビルドおよびインストールする。  
   [`How to build`]

### 3.2 結合テスト

1. `PostgreSQL`を起動する。

   ```sh
   pg_ctl start
   ```

2. 結合テストを実行する。

   ```sh
   cd /path/to/frontend
   make tests
   ```

   `$HOME/.local/bin`以外に`oltp`がインストールされている場合、`oltp`コマンドが存在しない旨のエラーが表示される。  

   ```shell-session
   test.sh: line 7: /home/postgres/.local/bin/oltp: No such file or directory
   test.sh: line 8: /home/postgres/.local/bin/oltp: No such file or directory
   ```

   この場合、手動で`oltp`を起動した後に結合テストを実行する必要がある。

   ```sh
   oltp start
   make installcheck
   ```

## 4 リリース手順

### 4.1 リリース手順

1. `metadata-manager`のリリース対象ブランチをリモートリポジトリのメインブランチにマージする。

### 4.2 リリース通知

1. `metadata-manager`のリリース通知を各コンポーネント担当に通知する。  
   ※通知方法等は別途検討

<!--
## 4 リリース手順 (`frontend`)

コンポーネントの各種手順およびルールに従い、`metadata-manager`を最新に更新しリリースする。

## 5. リリース手順 (`ogawayama`)

コンポーネントの各種手順およびルールに従い、`metadata-manager`を最新に更新しリリースする。
-->

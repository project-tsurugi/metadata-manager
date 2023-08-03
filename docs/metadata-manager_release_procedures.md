【Project-Tsurugi Internal User Only】

# metadata-managerリリース手順

2023.08.03 KCC  

---

## 目次

- [metadata-managerリリース手順](#metadata-managerリリース手順)
  - [目次](#目次)
  - [1 はじめに](#1-はじめに)
    - [1.1 本書の目的](#11-本書の目的)
    - [1.2 前提条件](#12-前提条件)
  - [2 リリースの流れ](#2-リリースの流れ)
    - [2.1 リリース管理](#21-リリース管理)
      - [2.1.1 RC版 (Release Candidate)](#211-rc版-release-candidate)
      - [2.1.2 プレリリース版](#212-プレリリース版)
      - [2.1.3 正式版](#213-正式版)
  - [3 担当毎の作業](#3-担当毎の作業)
    - [3.1 metadata-manager担当](#31-metadata-manager担当)
    - [3.2 ogawayama担当](#32-ogawayama担当)
    - [3.3 frontend担当](#33-frontend担当)

---

## 1 はじめに

### 1.1 本書の目的

本書は、Tsurugiのメタデータを管理する`metadata-manager`（統合メタデータ管理基盤）をリリース（メインブランチにコミット）する手順を示す。

### 1.2 前提条件

ビルドやレグレッションテスト、リリース等の各種手順は、各コンポーネントに準ずる。

## 2 リリースの流れ

リリース通知や各種連絡等は、metadata-managerリポジトリのIssueを用いて行う。  
Issueはリリース毎に新規に作成し、適切なポイントでIssueにコメントを追記することでワークフローを形成する。  

```mermaid
sequenceDiagram
  actor meta as metadata-manager担当者
  participant Issue

  rect rgb(255, 255, 255)
    meta ->> Issue : メインブランチの更新予告<br>(リリース予告)
  end

  rect rgb(255, 255, 255)
  meta ->> meta: メインブランチの更新
  meta ->> Issue : メインブランチの更新<br>(リリース)
  end
  
  actor oga as ogawayama担当者
  Issue -->> oga : 通知
  oga ->> oga : third_party/metadata-manager更新  
  oga ->> oga : メインブランチ更新
  oga ->> Issue : メインブランチ更新報告

  actor fe as frontend担当者
  Issue -->> fe : 通知
  fe ->> fe : third_party/metadata-manager更新  
  fe ->> fe : third_party/ogawayama更新  
  fe ->> fe : メインブランチ更新
  fe ->> Issue : メインブランチ更新報告
```

### 2.1 リリース管理

metadata-managerのリリースの管理は、GitHubのリリース機能を用いる。  
リリースは、開発ブランチのリリースであるRC版 (Release Candidate)と、メインブランチの正式リリースとなる正式版に大別される。  
また、正式版はプレリリースと正式リリースに分かれ、ogawayamaおよびfrontendにおけるサブモジュール(metadata-manager)の更新状況によって遷移する。

```mermaid
stateDiagram
  RC : RC版
  PRE : 正式版 (プレリリース)
  GA : 正式版
  [*] --> RC : 開発ブランチのテスト完了
  RC --> PRE : メインブランチへのマージ・<br>レグレッションテストの完了
  PRE --> GA : ogawayama, frontendにて<br>サブモジュール更新完了
  GA --> [*]
```

#### 2.1.1 RC版 (Release Candidate)

- 開発ブランチの一過性のリリースバージョン。
- 通常は使用されないが、大規模開発やインタフェース変更など、必要に応じて正式リリース前に他コンポーネントで使用する事を目的としたバージョンとなる。

#### 2.1.2 プレリリース版

- 開発ブランチをマージしたメインブランチのプレリリースバージョン。
- ogawayamaおよびfrontendにおけるサブモジュール更新およびテストを行うためのリリースバージョンとなる。
- 正式版と差分がない最新状態ではあるが、GitHub上にて`pre-release`としてマークされたバージョンとなる。

#### 2.1.3 正式版

- メインブランチの正式公開の最新リリースバージョン。
- ogawayamaおよびfrontendにおけるサブモジュール更新が完了し、コンポーネント間の同期がとれたバージョンとなる。
- GitHub上にて`Latest`としてマークされたバージョンとなり、プレリリース版との違いはリリース状態のみ。

## 3 担当毎の作業

### 3.1 metadata-manager担当

```mermaid
flowchart TB
  subgraph metadata [metadata-manager担当]
    M_WF_1("RC版 リリース<br>[開発ブランチ]")
    M_WF_2("リリース予告通知<br>[開発ブランチ]")
    M_WF_3("レグレッションテストなど<br>[開発ブランチ]")
    M_WF_4("開発ブランチをメインブランチにマージ")
    M_WF_5("正式版 プレリリース<br>[メインブランチ]")
    M_WF_6("リリース通知<br>[メインブランチ]")
    M_WF_7("正式版 リリース<br>[メインブランチ]")
  end

  subgraph ogawayama [ogawayama担当]
    O_WF[["metadata-managerの最新化"]]
  end
  subgraph frontend [frontend担当]
    direction TB
    F_WF_1[["metadata-managerの最新化"]]
    F_WF_2[["ogawayamaの最新化"]]
    F_WF_1 -.-> F_WF_2
  end

  M_WF_1 --> M_WF_2
  M_WF_2 --> M_WF_3
  M_WF_3 --> M_WF_4
  M_WF_4 --> M_WF_5
  M_WF_5 --> M_WF_6
  M_WF_6 -.-> ogawayama
  ogawayama -. "metadata-manager<br>更新完了連絡" .-> frontend
  frontend -. "metadata-manager<br>更新完了連絡" .-> M_WF_7
```

1. metadata-managerの開発ブランチにコミットする。
2. GitHubにてRC版のリリースを作成する。
   - **`GitHub` > `Releases`**
     - `Choose a tag`: リリースバージョン (e.g., `v1.0.0-rc.1`)
     - `Target`: 開発ブランチ (e.g., `new-feature`)
     - `Release title`: リリースバージョン (e.g., `v1.0.0-rc.1`)
     - `Describe this`: リリース内容 (e.g., 変更内容など)
     - `Set as a pre-release`
3. リリース通知用のIssueを作成する。
   - リリース内容 (GitHubリリースの `Describe this` と同等の内容)
   - 影響範囲 (コンポーネントへの影響の有無や内容)
   - 特記事項 (注意点や条件など)
   - RC版のリリース情報
     - タグ名 (e.g., `v1.0.0-rc.1`)
     - ソースファイル (e.g., `https://github.com/project-xxxxxx/metadata-manager/tree/v1.0.0-rc.1`)
4. リリース通知Issueにコメントを追加する。
   - メインブランチの更新予告
5. メインブランチへのマージ前作業 (レグレッションテストなど)
6. 開発ブランチをメインブランチにマージする。
7. GitHubにて正式版リリースを作成する。
   - **`GitHub` > `Releases`**
     - `Choose a tag`: リリースバージョン (e.g., `v1.0.0`)
     - `Target`: メインブランチ (e.g., `master`)
     - `Release title`: リリースバージョン (e.g., `v1.0.0`)
     - `Describe this`: リリース内容 (e.g., 変更内容など)
     - `Set as a pre-release`
8. リリース通知Issueの概要を更新する。
   - 正式版のリリース情報
     - タグ名 (e.g., `v1.0.0`)
     - ソースファイル (e.g., `https://github.com/project-xxxxxx/metadata-manager/tree/v1.0.0`)
9. リリース通知Issueにコメントを追加する。
   - メインブランチの更新
10. ogawayamaおよびfrontend担当より更新完了の連絡待ち。
11. [7]にて作成したリリースを`Latest release`に変更する。
    - **`GitHub` > `Releases`**
      - `Set as the latest release`

### 3.2 ogawayama担当

```mermaid
flowchart TB
  M_WF_1("[Issue]<br>(metadata-manager リリース通知)")
  M_WF_2("[Issue]<br>(metadata-manager リリース通知)")

  M_WF_1 -.-> O_WF_1

  subgraph ogawayama [ogawayama担当]
    O_WF_1("サブモジュールのアップデート<br>(metadata-manager)")
    O_WF_2("テストの実施")
    O_WF_3("メインブランチの更新")

    O_WF_1 --> O_WF_2
    O_WF_2 --> O_WF_3
  end

  O_WF_2 -. "テスト結果の連絡<br>(NGの場合など)" .-> M_WF_2
  O_WF_3 -. "メインブランチ更新の連絡" .-> M_WF_2
```

1. metadata-managerのメインブランチの更新を確認する。
2. ogawayamaのサブモジュールをアップデートする。
   - metadata-manager
3. レグレッションテストを実施する。
4. リリース通知用のIssueにコメントを追加する。  
   ※特に問題や連絡事項がない場合は省略可
   - テスト結果
5. ogawayamaのメインブランチにサブモジュールの更新をコミットする。
6. リリース通知Issueにコメントを追加する。
   - メインブランチの更新

### 3.3 frontend担当

```mermaid
flowchart TB
  M_WF_1("[Issue]<br>(metadata-manager リリース通知)<br>(ogawayama リリース通知)")
  M_WF_2("[Issue]<br>(metadata-manager リリース通知)<br>(ogawayama リリース通知)")

  M_WF_1 -.-> F_WF_1
  subgraph frontend [frontend担当]
    F_WF_1("サブモジュールのアップデート<br>(metadata-manager)<br>(ogawayama)")
    F_WF_2("テストの実施")
    F_WF_3("メインブランチの更新")

    F_WF_1 --> F_WF_2
    F_WF_2 --> F_WF_3
  end

  F_WF_2 -. "テスト結果の連絡<br>(NGの場合など)" .-> M_WF_2
  F_WF_3 -. "メインブランチ更新の連絡" .-> M_WF_2
```

1. metadata-managerおよびogawayamaのメインブランチの更新を確認する。
2. frontendのサブモジュールをアップデートする。
   - metadata-manager
   - ogawayama
3. レグレッションテストを実施する。
4. リリース通知用のIssueにコメントを追加する。  
   ※特に問題や連絡事項がない場合は省略可
   - テスト結果
5. frontendのメインブランチにサブモジュールの更新をコミットする。
6. リリース通知Issueにコメントを追加する。
   - メインブランチの更新

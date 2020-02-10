# V1 テーブルメタデータ データ形式 
2020.02.07 NEC

* '*' : メタデータ登録時に必須の項目
* '+' : メタデータ登録時に入力可能な項目

```
    {
        "version": number,  // バージョン ※V1未使用
        "tables" :                                                                              [*]
        [
            {   // テーブルメタデータ                                                           
                "id"                        : number,   // テーブルID                           
                "name"                      : text,     // テーブル名                           [*]
                "schema_name"               : text,     // スキーマ名                           [*]
                "columns" :                                                                     [*]
                [
                    {   // カラムメタデータ                                                     
                        "id"                : number,   // カラムID                             
                        "table_id"          : number,   // カラムが属するテーブルのID           
                        "name"              : text,     // カラム名                             [*]
                        "ordinal_position"  : number,   // カラム番号(1 origin)                 [*]
                        "data_type_id"      : number,   // カラムのデータ型のID                 [*]
                                                        // データタイプメタデータを参照(別途)
                        "data_type_name"    : string    // カラムのデータ型名                   [*]
                                                        // "data_type_id" と "data_type_name" は選択必須
                        "data_length"       : array,    // データ長(配列長)                     [+]
                                                        // varchar(20)など ※V1では未使用
                        "nullable"          : bool,     // NOT NULL制約の有無                   [*]
                        "constraints"       :                                                   [+]
                        [
                            {   // 制約メタデータ(カラム制約)                                   [+]
                                "id"        : number,   // 制約ID                               
                                "table_id"  : number,   // 制約が属するテーブルのID             
                                "column_pos": array,    // 制約が属するカラムのordinal_position [+]
                                "name"      : text,     // 制約名                               [+]
                                "type"      : text      // 制約の種類                           [*]
                                                        // PostgreSQLのメタデータに合わせる
                                                        // 	c = 検査制約、 f = 外部キー制約、 p = 主キー制約、
                                                        //  u = 一意性制約、 t = 制約トリガ, x = 排他制約 
                                "contents"  : text      // 制約の補足情報(式など)               [+]
                            },
                            …
                        ]
                    },
                    …
                ],
                "constraints" : 
                [
                    {   // 制約メタデータ(テーブル制約)  ※カラム制約と同じ                     [+]
                        "id"        : number,   // 制約ID                                       
                        "table_id"  : number,   // 制約が属するテーブルのID                     
                        "column_pos": array,    // 制約が属するカラムのordinal_position         [+]
                        "name"      : text,     // 制約名                                       [+]
                        "type"      : text,     // 制約の種類(列制約と同じ)                     [*]
                        "contents"  : text      // 制約の補足情報                               [+]
                    },
                    …
                ]
            },
            …
        ]
    }
```
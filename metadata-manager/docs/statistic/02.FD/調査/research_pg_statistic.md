# pg_statisticに格納される値の調査

## pg_stats

```sql
CREATE VIEW pg_stats WITH (security_barrier) AS
    SELECT
        nspname AS schemaname,
        relname AS tablename,
        attname AS attname,
        stainherit AS inherited,
        stanullfrac AS null_frac,
        stawidth AS avg_width,
        stadistinct AS n_distinct,
        CASE
            WHEN stakind1 = 1 THEN stavalues1
            WHEN stakind2 = 1 THEN stavalues2
            WHEN stakind3 = 1 THEN stavalues3
            WHEN stakind4 = 1 THEN stavalues4
            WHEN stakind5 = 1 THEN stavalues5
        END AS most_common_vals,
        CASE
            WHEN stakind1 = 1 THEN stanumbers1
            WHEN stakind2 = 1 THEN stanumbers2
            WHEN stakind3 = 1 THEN stanumbers3
            WHEN stakind4 = 1 THEN stanumbers4
            WHEN stakind5 = 1 THEN stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN stakind1 = 2 THEN stavalues1
            WHEN stakind2 = 2 THEN stavalues2
            WHEN stakind3 = 2 THEN stavalues3
            WHEN stakind4 = 2 THEN stavalues4
            WHEN stakind5 = 2 THEN stavalues5
        END AS histogram_bounds,
        CASE
            WHEN stakind1 = 3 THEN stanumbers1[1]
            WHEN stakind2 = 3 THEN stanumbers2[1]
            WHEN stakind3 = 3 THEN stanumbers3[1]
            WHEN stakind4 = 3 THEN stanumbers4[1]
            WHEN stakind5 = 3 THEN stanumbers5[1]
        END AS correlation,
        CASE
            WHEN stakind1 = 4 THEN stavalues1
            WHEN stakind2 = 4 THEN stavalues2
            WHEN stakind3 = 4 THEN stavalues3
            WHEN stakind4 = 4 THEN stavalues4
            WHEN stakind5 = 4 THEN stavalues5
        END AS most_common_elems,
        CASE
            WHEN stakind1 = 4 THEN stanumbers1
            WHEN stakind2 = 4 THEN stanumbers2
            WHEN stakind3 = 4 THEN stanumbers3
            WHEN stakind4 = 4 THEN stanumbers4
            WHEN stakind5 = 4 THEN stanumbers5
        END AS most_common_elem_freqs,
        CASE
            WHEN stakind1 = 5 THEN stanumbers1
            WHEN stakind2 = 5 THEN stanumbers2
            WHEN stakind3 = 5 THEN stanumbers3
            WHEN stakind4 = 5 THEN stanumbers4
            WHEN stakind5 = 5 THEN stanumbers5
        END AS elem_count_histogram
    FROM pg_statistic s JOIN pg_class c ON (c.oid = s.starelid)
         JOIN pg_attribute a ON (c.oid = attrelid AND attnum = s.staattnum)
         LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE NOT attisdropped
    AND has_column_privilege(c.oid, a.attnum, 'select')
    AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));

```



## 略語

* MCV
  * 最頻値（スカラ型または非スカラ型値）
* MCF
  * 最頻度（スカラ型または非スカラ型値）
* Uncommon
  * Uncommonのヒストグラム（スカラ型または非スカラ型値）
* correlation
  * 物理的な（訳注：ディスク上の）行の並び順と論理的な列の値の並び順に関する統計的相関
* MCE
  * 非スカラ型値の要素の最頻値
* MCEF
  * 非スカラ型値の要素の最頻度
* UncommonE
  * 非スカラ型値の要素のUncommonのヒストグラム

## stakindNに挿入する番号

| 列統計の種類 | stakindN |
| ------------ | -------- |
| MCV/MCF      | 1        |
| Uncommon     | 2        |
| correlation  | 3        |
| MCE/MCEF     | 4        |
| UncommonE    | 5        |

## pg_statisticに格納される値

### スカラ型

| stakind1-5    | MCV/MCF | Uncommon | correlation | MCE/MCEF | UncommonE |
| ------------- | ------- | -------- | ----------- | -------- | --------- |
| 1, 2, 3, 0, 0 | 〇      | 〇       | 〇          | ×        | ×         |
| 1, 3, 0, 0, 0 | 〇      | ×        | 〇          | ×        | ×         |
| 1, 0, 0, 0, 0 | 〇      | ×        | ×           | ×        | ×         |
| 2, 3, 0, 0, 0 | ×       | 〇       | 〇          | ×        | ×         |
| 0, 0, 0, 0, 0 | ×       | ×        | ×           | ×        | ×         |

〇…値が格納されている、×...値が格納されていない

### 非スカラ

| stakind1-5    | MCV/MCF | Uncommon | correlation | MCE/MCEF | UncommonE |
| :------------ | ------- | -------- | ----------- | -------- | --------- |
| 1, 2, 3, 4, 5 | 〇      | 〇       | 〇          | 〇       | 〇        |
| 1, 3, 4, 5, 0 | 〇      | ×        | 〇          | 〇       | 〇        |
| 4, 5, 0, 0, 0 | ×       | ×        | ×           | 〇       | 〇        |

〇…値が格納されている、×...値が格納されていない
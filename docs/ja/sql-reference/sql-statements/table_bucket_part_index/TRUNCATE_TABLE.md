---
displayed_sidebar: docs
---

# TRUNCATE TABLE

## 説明

このステートメントは、指定されたテーブルおよびパーティションデータを切り詰めるために使用されます。

構文:

```sql
TRUNCATE TABLE [db.]tbl[ PARTITION(PartitionName1, PartitionName2, ...)]
```

注意:

1. このステートメントは、テーブルまたはパーティションを保持しながらデータを切り詰めるために使用されます。
2. DELETE とは異なり、このステートメントは指定されたテーブルまたはパーティション全体を空にすることしかできず、フィルタリング条件を追加することはできません。
3. DELETE とは異なり、この方法を使用してデータをクリアしてもクエリパフォーマンスには影響しません。
4. このステートメントはデータを直接削除します。削除されたデータは復元できません。
5. この操作を行うテーブルは NORMAL 状態でなければなりません。たとえば、SCHEMA CHANGE が進行中のテーブルに対して TRUNCATE TABLE を実行することはできません。

## 例

1. `example_db` の下のテーブル `tbl` を切り詰めます。

    ```sql
    TRUNCATE TABLE example_db.tbl;
    ```

2. テーブル `tbl` のパーティション `PartitionName1` と `PartitionName2` を切り詰めます。

    ```sql
    TRUNCATE TABLE tbl PARTITION(PartitionName1, PartitionName2);
    ```

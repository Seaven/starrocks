---
displayed_sidebar: docs
---

# collations

`collations` には、利用可能な照合順序が含まれています。

`collations` には次のフィールドが提供されています。

| **Field**          | **Description**                                              |
| ------------------ | ------------------------------------------------------------ |
| COLLATION_NAME     | 照合順序の名前。                                             |
| CHARACTER_SET_NAME | 照合順序が関連付けられている文字セットの名前。               |
| ID                 | 照合順序の ID。                                              |
| IS_DEFAULT         | 照合順序がその文字セットのデフォルトかどうか。               |
| IS_COMPILED        | 文字セットがサーバーにコンパイルされているかどうか。         |
| SORTLEN            | 文字セットで表現された文字列をソートするのに必要なメモリ量に関連しています。 |
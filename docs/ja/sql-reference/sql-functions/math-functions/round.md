---
displayed_sidebar: docs
---

# round, dround

数値を指定された桁数に丸めます。

- `n` が指定されていない場合、`x` は最も近い整数に丸められます。
- `n` が指定されている場合、`x` は `n` 桁の小数点に丸められます。`n` が負の場合、`x` は小数点の左側に丸められます。オーバーフローが発生した場合、エラーが返されます。

## 構文

```Haskell
ROUND(x [,n]);
```

## パラメータ

`x`: 丸める対象の数値です。DOUBLE および DECIMAL128 データ型をサポートします。

`n`: 数値を丸める小数点以下の桁数です。INT データ型をサポートします。このパラメータはオプションです。

## 戻り値

- `x` のみが指定された場合、戻り値のデータ型は次のとおりです：

  ["DECIMAL128"] -> "DECIMAL128"

  ["DOUBLE"] -> "BIGINT"

- `x` と `n` の両方が指定された場合、戻り値のデータ型は次のとおりです：

  ["DECIMAL128", "INT"] -> "DECIMAL128"

  ["DOUBLE", "INT"] -> "DOUBLE"

## 例

```Plain
mysql> select round(3.14);
+-------------+
| round(3.14) |
+-------------+
|           3 |
+-------------+

mysql> select round(3.14,1);
+----------------+
| round(3.14, 1) |
+----------------+
|            3.1 |
+----------------+

mysql> select round(13.14,-1);
+------------------+
| round(13.14, -1) |
+------------------+
|               10 |
+------------------+

mysql> select round(122.14,-1);
+-------------------+
| round(122.14, -1) |
+-------------------+
|               120 |
+-------------------+

mysql> select round(122.14,-2);
+-------------------+
| round(122.14, -2) |
+-------------------+
|               100 |
+-------------------+

mysql> select round(122.14,-3);
+-------------------+
| round(122.14, -3) |
+-------------------+
|                 0 |
+-------------------+
```
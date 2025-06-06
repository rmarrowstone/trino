---
myst:
  substitutions:
    default_domain_compaction_threshold: '`1000`'
---

# Ignite connector

```{raw} html
<img src="../_static/img/ignite.png" class="connector-logo">
```

The Ignite connector allows querying an [Apache Ignite](https://ignite.apache.org/)
database from Trino.

## Requirements

To connect to a Ignite server, you need:

- Ignite version 2.9.0 or latter
- Network access from the Trino coordinator and workers to the Ignite
  server. Port 10800 is the default port.
- Specify `--add-opens=java.base/java.nio=ALL-UNNAMED` in the `jvm.config` when starting the Trino server.

## Configuration

The Ignite connector expose `public` schema by default.

The connector can query a Ignite instance. Create a catalog properties file
that specifies the Ignite connector by setting the `connector.name` to
`ignite`.

For example, to access an instance as `example`, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```text
connector.name=ignite
connection-url=jdbc:ignite:thin://host1:10800/
connection-user=exampleuser
connection-password=examplepassword
```

The `connection-url` defines the connection information and parameters to pass
to the Ignite JDBC driver. The parameters for the URL are available in the
[Ignite JDBC driver documentation](https://ignite.apache.org/docs/latest/SQL/JDBC/jdbc-driver).
Some parameters can have adverse effects on the connector behavior or not work
with the connector.

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Multiple Ignite servers

If you have multiple Ignite servers you need to configure one
catalog for each server. To add another catalog:

- Add another properties file to `etc/catalog`
- Save it with a different name that ends in `.properties`

For example, if you name the property file `sales.properties`, Trino uses the
configured connector to create a catalog named `sales`.

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Table properties

Table property usage example:

```
CREATE TABLE public.person (
  id BIGINT NOT NULL,
  birthday DATE NOT NULL,
  name VARCHAR(26),
  age BIGINT,
  logdate DATE
)
WITH (
  primary_key = ARRAY['id', 'birthday']
);
```

The following are supported Ignite table properties from [https://ignite.apache.org/docs/latest/sql-reference/ddl](https://ignite.apache.org/docs/latest/sql-reference/ddl)

:::{list-table}
:widths: 30, 10, 60
:header-rows: 1

* - Property name
  - Required
  - Description
* - `primary_key`
  - No
  - The primary key of the table, can choose multi columns as the table primary
    key. Table at least contains one column not in primary key.
:::

### `primary_key`

This is a list of columns to be used as the table's primary key. If not specified, a `VARCHAR` primary key column named `DUMMY_ID` is generated,
the value is derived from the value generated by the `UUID` function in Ignite.

(ignite-type-mapping)=
## Type mapping

The following are supported Ignite SQL data types from [https://ignite.apache.org/docs/latest/sql-reference/data-types](https://ignite.apache.org/docs/latest/sql-reference/data-types)

:::{list-table}
:widths: 25, 25, 50
:header-rows: 1

* - Ignite SQL data type name
  - Map to Trino type
  - Possible values
* - `BOOLEAN`
  - `BOOLEAN`
  - `TRUE` and `FALSE`
* - `BIGINT`
  - `BIGINT`
  - `-9223372036854775808`, `9223372036854775807`, etc.
* - `DECIMAL`
  - `DECIMAL`
  - Data type with fixed precision and scale
* - `DOUBLE`
  - `DOUBLE`
  - `3.14`, `-10.24`, etc.
* - `INT`
  - `INT`
  - `-2147483648`, `2147483647`, etc.
* - `REAL`
  - `REAL`
  - `3.14`, `-10.24`, etc.
* - `SMALLINT`
  - `SMALLINT`
  - `-32768`, `32767`, etc.
* - `TINYINT`
  - `TINYINT`
  - `-128`, `127`, etc.
* - `CHAR`
  - `CHAR`
  - `hello`, `Trino`, etc.
* - `VARCHAR`
  - `VARCHAR`
  - `hello`, `Trino`, etc.
* - `DATE`
  - `DATE`
  - `1972-01-01`, `2021-07-15`, etc.
* - `BINARY`
  - `VARBINARY`
  - Represents a byte array.
:::

(ignite-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
Ignite.  In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- [](/sql/insert), see also [](ignite-insert)
- [](/sql/update), see also [](ignite-update)
- [](/sql/delete)
- [](/sql/merge), see also [](ignite-merge)
- [](/sql/create-table)
- [](/sql/create-table-as)
- [](/sql/drop-table)
- [](/sql/alter-table), see also [](ignite-alter-table)
- [](ignite-procedures)

(ignite-insert)=
```{include} non-transactional-insert.fragment
```

(ignite-update)=
```{include} sql-update-limitation.fragment
```

(ignite-merge)=
```{include} non-transactional-merge.fragment
```

(ignite-alter-table)=
```{include} alter-table-limitation.fragment
```

(ignite-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

(ignite-pushdown)=
### Pushdown

The connector supports pushdown for a number of operations:

- {ref}`join-pushdown`
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`avg`
- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`


```{include} no-pushdown-text-type.fragment
```

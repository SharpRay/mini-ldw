# mini-ldw

A logical data warehouse demo. 
The supported databases & engines in this demo include:
* MySQL (compatible with mysql-server-5.7.36, mysql-jdbc-5.1.41)
* PostgreSQL (compatible with postgresql-server-9.5.19, postgresql-jdbc-42.3.1)
* Clickhouse (compatible with clickhouse-server-22.1.3.7, clickhouse-jdbc-0.3.2-patch4)
* Doris (compatible with doris-server-0.15.0-rc4, mysql-jdbc-5.1.41)
* Elasticsearch (compatible with elasticsearch-7.10, elasticsearch-rest-client-7.10.2)

This logical data warehouse compatible with MySQL protocol and can be accessd with MySQL CLI and MySQL JDBC.

The goal of this project is simply to demonstrate the capabilities of Calcite in data virtualization, therefore, metadata persistence and privileges is not implemented. After server restarted, the schemas, tables and views that previously created will be lost.

This project currently supports creating view with views and tables in different data sources, this is the core feature of data virtualization product.

# Usage

## Compile

```
mvn clean package -DskipTests
```

## Start server

```
cd target && java -cp lib/*:mini-ldw-demo.jar com.mininglamp.MiniLDW
```

## Access using MySQL CLI

```
mysql -h 127.0.0.1 -uroot -P9031 -A
```
Due to hard-to-locate bug, the -A parameter must be specified to disable auto-rehash.

# Features

## Create Schema

### Create MySql schema

``` sql
create schema mysql_schema 'mysql:127.0.0.1:3306:test_db:root:root';
```

### Create PostgreSQL schema

``` sql
create schema pg_schema 'postgresql:127.0.0.1:5432:test_db:root:root';
```

### Create Clickhouse schema

``` sql
create schema ck_schema 'clickhouse:127.0.0.1:8123:test_db:root:root';
```

### Create Doris schema

``` sql
create schema doris_schema 'mysql:127.0.0.1:9030:test_db:root:root';
```

### Create Elasticsearch schema

``` sql
create schema es_schema 'elasticsearch:127.0.0.1:9200:test_index:root:root';
```

If the index is not specified, then all the indexes will be mapped to tables in the elasticsearch schema.

## Drop Schema

``` sql
drop schema mysql_schema;
```

## Query

``` sql
use mysql_schema;
select * from tbl where id = 1;
```

or

``` sql
select * mysql_schema.tbl where id = 1;
```

Of course, it also supports multi-data-source fusion：

``` sql
select t1.* from mysql_schema.tbl t1 join clickhouse_schema.tbl t2 on t1.object_id = t2.id where t2.name in (’happy‘, 'Chinese', 'new', 'year');
```

## Create Table

``` sql
create table tbl (id int, name varchar(50));
```

The data of this new creating table will be stay in heap memory, and will be lost after the server restarted.

## Create View

``` sql
create view test_view as select t1.* from mysql_schema.tbl t1 join clickhouse_schema.tbl t2 on t1.object_id = t2.id;
```

The data of this new creating view consists of multiple tables or views belonging to different data sources.

## Update & Delete

``` sql
update tbl set name = 'paradise' where id = 1;
```

``` sql
delete from tbl where id = 1;
```

The update and delete operations on tables in the physical data source will be persisted.

## Show Schemas

``` sql
show schemas;
```

## Show Tables

``` sql
show tables;
```

## Describe Table

``` sql
desc tbl;
```

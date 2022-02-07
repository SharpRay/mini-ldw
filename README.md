# mini-ldw

A logical data warehouse demo. 
The supported databases & engines in this demo version include:
* MySQL (compatible with mysql-server-5.7.36, mysql-jdbc-5.1.41)
* PostgreSQL (compatible with postgresql-server-9.5.19, postgresql-jdbc-42.3.1)
* Clickhouse (compatible with clickhouse-server-22.1.3.7, clickhouse-jdbc-0.3.2-patch4)
* Doris (compatible with doris-server-0.15.0-rc4, mysql-jdbc-5.1.41)
* Elasticsearch (compatible with elasticsearch-7.10, elasticsearch-rest-client-7.10.2)

This logical data warehouse compatible with MySQL protocol and can be accessd with MySQL CLI and MySQL JDBC.

The goal of this project is simply to demonstrate the capabilities of Calcite in data virtualization, therefore, metadata persistence is not implemented. After server restarted, the schemas, tables and views that previously created will be lost.

# Usage

## Compile

```
mvn clean package -DskipTests
```

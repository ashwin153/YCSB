# MySQL
1. Install MySQL.

```
brew install mysql
brew services start mysql
```

2. Create the ```usertable``` in database ```ycsb```.

```
mysql -u root -e "CREATE DATABASE `ycsb`"
mysql -u root -e "CREATE TABLE `ycsb`.`usertable` (
  `YCSB_KEY` VARCHAR(255) PRIMARY KEY,
  `FIELD0` TEXT, `FIELD1` TEXT,
  `FIELD2` TEXT, `FIELD3` TEXT,
  `FIELD4` TEXT, `FIELD5` TEXT,
  `FIELD6` TEXT, `FIELD7` TEXT,
  `FIELD8` TEXT, `FIELD9` TEXT
);"
```

3. Benchmark the JDBC implementation.

```
bin/ycsb load jdbc -P workloads/workloada -P caustic/setup/mysql/db.properties -cp caustic/setup/mysql/mysql-connector-java-6.0.6.jar
bin/ycsb run jdbc -P workloads/workloada -P caustic/setup/mysql/db.properties -cp caustic/setup/mysql/mysql-connector-java-6.0.6.jar
```

4. Benchmark the Caustic implementation.

```
bin/ycsb load caustic -P workloads/workloada -P caustic/setup/mysql/db.properties
bin/ycsb run caustic -P workloads/workloada -P caustic/setup/mysql/db.properties
```

5. Because deletes in Caustic are tombstones, be sure to truncate the database after each test.

```
mysql -u root -e "TRUNCATE ycsb.caustic"
```

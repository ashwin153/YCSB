#!/bin/bash
workloads=("a" "b" "c" "d" "f")
properties="./beaker/setup/mysql/db.properties"
driver="./beaker/setup/mysql/mysql-connector-java-6.0.6.jar"

for i in "${workloads[@]}" ; do
  workload="./workloads/workload$i"
  
  # Run the workload on MySQL.
  eval "./bin/ycsb load jdbc -P $workload -P $properties -cp $driver"
  eval "./bin/ycsb run jdbc -P $workload -P $properties -cp $driver > ./beaker/results/mysql-$i.txt"

  # Run the workload on Caustic.
  eval "./bin/ycsb load beaker -P $workload -P $properties"
  eval "./bin/ycsb run beaker -P $workload -P $properties > ./beaker/results/beaker-$i.txt"

  # Purge all test data from the database.
  eval "mysql -u root -e \"TRUNCATE test.beaker\""
  eval "mysql -u root -e \"TRUNCATE ycsb.usertable\""
done

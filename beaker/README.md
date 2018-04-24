# Beaker
Because deletes in Beaker are tombstones, the underlying database must be manually
truncated or the servers must be manually restarted between workloads.

```bash
# Setup an in-memory Beaker.
git clone https://github.com/ashwin153/beaker && cd beaker/
./run.sh -p 9090 -o beaker.server.seed=127.0.0.1:9090 && cd ..

# Setup YCSB.
sudo apt install maven
git clone https://github.com/ashwin153/YCSB && cd YCSB/
mvn package -DskipTests

# Benchmark Beaker.
bin/ycsb load beaker -P workloads/workloada 
bin/ycsb run  beaker -P workloads/workloada 
```

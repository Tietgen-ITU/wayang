.PHONY: build run

build: 
	rm -rf ./wayang-0.7.1
	./mvnw clean install -DskipTests -Drat.skip=true
	./mvnw clean package -pl :wayang-assembly -Pdistribution
	tar -xvf wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist.tar.gz

run: 
	wayang-submit org.apache.wayang.apps.parquet.Main file:///workspaces/wayang/test-00000-of-00001.parquet

.PHONY: install-benchmark
install-benchmark:
	./mvnw clean install -DskipTests -pl wayang-benchmark

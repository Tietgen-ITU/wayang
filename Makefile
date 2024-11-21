.PHONY: build run

build: 
	rm -rf ./wayang-0.7.1
	./mvnw clean install -DskipTests -Drat.skip=true
	./mvnw clean package -pl :wayang-assembly -Pdistribution

run: 
	wayang-submit org.apache.wayang.apps.parquet.Main file:///workspaces/wayang/dataset/yelp/yelp-650000.parquet

.PHONY: install-benchmark
install-benchmark:
	./mvnw clean install -DskipTests -pl wayang-benchmark

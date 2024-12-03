.PHONY: build run

build: 
	rm -rf ./wayang-0.7.1
	./mvnw clean install -DskipTests -Drat.skip=true
	./mvnw clean package -pl :wayang-assembly -Pdistribution

run: 
	OTHER_FLAGS="-Xmx10g" wayang-submit org.apache.wayang.apps.parquet.Main /workspaces/wayang/datasets

.PHONY: install-benchmark
install-benchmark:
	./mvnw clean install -DskipTests -pl wayang-benchmark

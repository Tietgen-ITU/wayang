.PHONY: build

build: 
	./mvnw clean install -DskipTests -Drat.skip=true
	./mvnw clean package -pl :wayang-assembly -Pdistribution
	tar -xvf wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist.tar.gz

.PHONY: build

build: 
	./mvnw clean install -DskipTests -Drat.skip=true

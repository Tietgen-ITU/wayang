#!/bin/bash

/opt/coursier/cs install scala:2.12.20 && /opt/coursier/cs install scalac:2.12.20

./mvnw clean install -DskipTests -Drat.skip=true

./mvnw clean package -pl :wayang-assembly -Pdistribution

tar -xvf wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist.tar.gz

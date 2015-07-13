#!/bin/bash
bash ./fetchDataset.sh
bash ./fetchDependencies.sh
mvn install:install-file -Dfile=./jars/psl-groovy-1.3.1-SNAPSHOT.jar -DgroupId=edu.umd.cs -DartifactId=psl-groovy -Dversion=1.3.1-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=./jars/psl-core-1.3.1-SNAPSHOT.jar -DgroupId=edu.umd.cs -DartifactId=psl-core -Dversion=1.3.1-SNAPSHOT -Dpackaging=jar
cd model
mkdir output
mvn clean compile
java -Xmx4G -cp ../jars/psl-core-1.3.1-SNAPSHOT.jar:../jars/psl-groovy-1.3.1-SNAPSHOT.jar:../jars/psl-archetype-groovy-1.3.1-SNAPSHOT.jar:./target/classes  edu.umd.cs.updateinfer.LoadData ../data/ 0 10 50
java -Xmx4G -cp ../jars/psl-core-1.3.1-SNAPSHOT.jar:../jars/psl-groovy-1.3.1-SNAPSHOT.jar:../jars/psl-archetype-groovy-1.3.1-SNAPSHOT.jar:./target/classes   edu.umd.cs.updateinfer.OnlineModel output/fullinf_0 10 50
java -Xmx4G -cp ../jars/psl-core-1.3.1-SNAPSHOT.jar:../jars/psl-groovy-1.3.1-SNAPSHOT.jar:../jars/psl-archetype-groovy-1.3.1-SNAPSHOT.jar:./target/classes  edu.umd.cs.updateinfer.LoadData ../data/ 0 10 50
java -Xmx4G -cp ../jars/psl-core-1.3.1-SNAPSHOT.jar:../jars/psl-groovy-1.3.1-SNAPSHOT.jar:../jars/psl-archetype-groovy-1.3.1-SNAPSHOT.jar:./target/classes   edu.umd.cs.updateinfer.OnlineApproximateModel output/onlineinf_0 10 50



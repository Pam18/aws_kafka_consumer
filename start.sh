export KAFKA_HOST="localhost:9092"
export KAFKA_TOPIC="TOPICO_1"
export KAFKA_GROUP_ID_READER="consomer_1"

export CLASSPATH=target/aws_kafka_consumer-1.0-SNAPSHOT.jar
export className=App
echo "## Running $className..."
shift
mvn exec:java -Dexec.mainClass="br.com.aws_kafka_consumer.$className"
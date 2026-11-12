FROM dinkydocker/dinky-standalone-server:1.2.5-flink1.20

# Kafka SQL connector. Dinky builds every job with this jar and sends it to
# Flink, which runs it with its own copy. The version MUST match
# streaming/flink/Dockerfile; different versions fail at runtime with
# "KafkaSink; local class incompatible" (reading still works, writing does not).
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
        -o /opt/dinky/extends/flink1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# MySQL JDBC driver — DinkyMysqlCatalog requires it on the classpath even
# when running in H2 standalone mode, otherwise DefaultCatalog persistence
# fails on startup.
RUN curl -fSL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
        -o /opt/dinky/lib/mysql-connector-j-8.3.0.jar

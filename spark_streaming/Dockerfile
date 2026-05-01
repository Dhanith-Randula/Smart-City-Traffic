FROM apache/spark:3.5.1-python3

USER root


RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
             /opt/spark/checkpoints/raw \
             /opt/spark/checkpoints/windows \
             /opt/spark/checkpoints/alerts && \
    chown -R spark:spark /home/spark/.ivy2 /opt/spark/checkpoints && \
    chmod -R 777 /opt/spark/checkpoints


RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
         -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
         -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
         -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
         -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
         -o /opt/spark/jars/postgresql-42.7.3.jar

USER spark
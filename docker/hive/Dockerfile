FROM apache/hive:4.0.0

# The stock Hive image does not ship with the PostgreSQL JDBC driver.
# We add it here so the metastore can initialize its schema in Postgres.
ADD --chmod=644 https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql-42.7.3.jar

# Hive Metastore needs the S3A jars on its own classpath to create schemas in MinIO.
COPY lib/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar
COPY lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

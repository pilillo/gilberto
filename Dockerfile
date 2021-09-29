ARG PROJ_NAME=gilberto
ARG HADOOP_VERSION
ARG SPARK_VERSION

# spark builds with java-8
ARG MVN_JDK_VERSION=3.6.3-openjdk-8
FROM maven:${MVN_JDK_VERSION} AS builder

ARG PROJ_NAME

# https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html
RUN apt-get update \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
    && echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add \
    && apt-get update \
    && apt-get install sbt

ADD . /${PROJ_NAME}
WORKDIR /${PROJ_NAME}

RUN sbt assembly \
    && mv target/scala-*/gilberto-assembly-*.jar gilberto.jar \
    && chmod -R ag+rx gilberto.jar

FROM pilillo/spark:${HADOOP_VERSION}_${SPARK_VERSION} as spark

ARG PROJ_NAME

COPY --from=builder /${PROJ_NAME}/gilberto.jar /gilberto.jar
COPY submitter-entrypoint.sh ${SPARK_HOME}/work-dir
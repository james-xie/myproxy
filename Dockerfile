# 阶段 1: 构建 jar 包
FROM registry.cn-hangzhou.aliyuncs.com/gllue/openjdk:11 as builder

ARG APP
ARG VERSION
ARG MAVEN_REPOSITORY_USERNAME
ARG MAVEN_REPOSITORY_PASSWORD
ARG GRADLE_BUCKET_NUMBER

WORKDIR /source

COPY . /source

ENV MAVEN_REPOSITORY_USERNAME=${MAVEN_REPOSITORY_USERNAME}
ENV MAVEN_REPOSITORY_PASSWORD=${MAVEN_REPOSITORY_PASSWORD}

RUN mkdir /source/scripts || true
RUN ./gradlew --console plain \
  --gradle-user-home /root/.gradle/${APP}/${GRADLE_BUCKET_NUMBER} \
  -Pversion=${VERSION} \
  fatJar

# 阶段 2: 把 jar 和 log4j 配置搞一起
FROM registry.cn-hangzhou.aliyuncs.com/gllue/openjdk:11

ARG APP
ARG VERSION
ARG GIT_REVISION

WORKDIR /app

COPY --from=builder /source/${APP}/build/libs/${APP}-${VERSION}.jar /app/app.jar
COPY --from=builder /source/${APP}/build/resources/main/log4j2.yaml /app/log4j2.yaml
COPY --from=builder /source/scripts /app/scripts
COPY --from=builder /source/Makefile /app/Makefile

# 将 java 软链接到 /usr/bin/java
RUN ln -s `which java` /usr/bin/java 2> /dev/null || true

ENV GIT_REVISION=${GIT_REVISION}

LABEL git.revision=${GIT_REVISION}

CMD java ${JAVA_OPTS} \
  -Dlog4j.configurationFile=./log4j2.yaml \
  -Dmyproxy.properties.location=./myproxy.properties \
  -Dsentry.release=${GIT_REVISION} \
  -jar app.jar

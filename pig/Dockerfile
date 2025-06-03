FROM openjdk:8-jdk-slim

# Variables de versión
ARG PIG_VERSION=0.17.0

# Instalar dependencias
RUN apt-get update && \
    apt-get install -y wget maven git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Descargar y configurar Apache Pig
RUN wget -q https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} /opt/pig && \
    rm pig-${PIG_VERSION}.tar.gz

# Crear directorios necesarios
RUN mkdir -p /opt/pig/lib /opt/pig/contrib/piggybank/java

# Descargar json-simple
RUN wget -q https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/json-simple/json-simple-1.1.1.jar -O /opt/pig/lib/json-simple-1.1.jar

# Descargar piggybank
RUN wget -q https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.16.0/piggybank-0.16.0.jar -O /opt/pig/contrib/piggybank/java/piggybank.jar

# Copiar piggybank.jar a la ubicación correcta
COPY piggybank.jar /opt/pig/lib/piggybank.jar

# Configurar variables de entorno
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:/opt/pig/bin
ENV PIG_CLASSPATH=/opt/pig/lib/json-simple-1.1.jar:/opt/pig/contrib/piggybank/java/piggybank.jar

# Directorio de trabajo
WORKDIR /scripts

# Comando por defecto
ENTRYPOINT ["pig"]


FROM docker.elastic.co/logstash/logstash:7.10.2

RUN bin/logstash-plugin install logstash-input-mongodb

# Crear directorios con permisos correctos
USER root
RUN mkdir -p /usr/share/logstash/data /opt/logstash/data /tmp/logstash && \
    chown -R logstash:logstash /usr/share/logstash/data /opt/logstash/data /tmp/logstash && \
    chmod -R 755 /usr/share/logstash/data /opt/logstash/data /tmp/logstash

USER logstash
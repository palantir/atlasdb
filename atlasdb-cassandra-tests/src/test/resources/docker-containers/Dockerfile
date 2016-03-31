FROM cassandra:2.2

ENV CASSANDRA_CONFIG /etc/cassandra
ENV CASSANDRA_YAML $CASSANDRA_CONFIG/cassandra.yaml
COPY cassandra.yaml $CASSANDRA_YAML
RUN sed -i 's/JVM_OPTS="$JVM_OPTS \-Dcom.sun.management.jmxremote.authenticate=true"/JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"/' "$CASSANDRA_CONFIG/cassandra-env.sh"
RUN sed -i '/JVM_OPTS="$JVM_OPTS \-Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"/a   JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote"' "$CASSANDRA_CONFIG/cassandra-env.sh"
RUN sed -i -e "s/^commitlog_segment_size_in_mb.*/commitlog_segment_size_in_mb: 64/"                  $CASSANDRA_CONFIG/cassandra.yaml
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 7000 7001 7199 9042 9160
CMD ["cassandra", "-f"]

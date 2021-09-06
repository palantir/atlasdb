install:
  paxos:
    data-directory: "${dataDirectory}"
    sqlite-persistence:
      data-directory: "${sqliteDataDirectory}"
    is-new-service: false
    leader-mode: ${leaderMode}
  timestampBoundPersistence:
    type: database
    key-value-service:
      type: "relational"
      ddl:
        type: "postgres"
      connection:
        type: "postgres"
        host: "localhost"
        port: 5432
        dbName: "atlas"
        dbLogin: "palantir"
        dbPassword: "palantir"

runtime:
  paxos:
    leader-ping-response-wait-in-ms: 1000
    timestamp-paxos:
      use-batch-paxos: ${clientPaxos.useBatchPaxosTimestamp?c}
    enable-batching-for-single-leader: ${clientPaxos.batchSingleLeader?c}
  cluster:
    cluster:
      security:
        trustStorePath: "var/security/trustStore.jks"
        trustStoreType: "JKS"
        keyStorePath: "var/security/keyStore.jks"
        keyStorePassword: "keystore"
        keyStoreType: "JKS"
      uris:
<#list serverProxyPorts as serverProxyPort>
      - "localhost:${serverProxyPort?c}"
</#list>
    local-server: "localhost:${localProxyPort?c}"

logging:
  appenders:
    - type: console
      logFormat: "server-${localServerPort?c} %-5p [%d{ISO8601,UTC}] %c: %m%n%rEx"

server:
  requestLog:
    appenders:
      - type: console
        logFormat: 'server-${localServerPort?c}       [%t{ISO8601,UTC}] %s %localPort %r %b "%i{User-Agent}" %D'

  applicationConnectors:
  - type: https
    port: ${localServerPort?c}
    keyStorePath: var/security/keyStore.jks
    keyStorePassword: keystore
    trustStorePath: var/security/trustStore.jks
    validateCerts: false
    supportedCipherSuites:
      - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
      - TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256
      - TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384
      - TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256
      - TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384
      - TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256
      - TLS_RSA_WITH_AES_128_CBC_SHA256
      - TLS_RSA_WITH_AES_256_CBC_SHA256
      - TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
      - TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
      - TLS_ECDH_RSA_WITH_AES_256_CBC_SHA
      - TLS_ECDH_RSA_WITH_AES_128_CBC_SHA
      - TLS_RSA_WITH_AES_256_CBC_SHA
      - TLS_RSA_WITH_AES_128_CBC_SHA
      - TLS_EMPTY_RENEGOTIATION_INFO_SCSV
  adminConnectors: []

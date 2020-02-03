install:
  paxos:
    data-directory: "${dataDirectory}"
    is-new-service: false
  cluster:
    cluster:
      security:
        trustStorePath: "var/security/trustStore.jks"
        trustStoreType: "JKS"
        keyStorePath: "var/security/keyStore.jks"
        keyStorePassword: "keystore"
        keyStoreType: "JKS"
      uris:
<#list serverPorts as serverPort>
      - "localhost:${serverPort?c}"
</#list>
    local-server: "localhost:${localServerPort?c}"
  timestampBoundPersistence:

runtime:
  paxos:
    leader-ping-response-wait-in-ms: 1000
    timestamp-paxos:
      use-batch-paxos: ${clientPaxos.useBatchPaxos?c}

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
  - type: h2
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

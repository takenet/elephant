## Running tests using local Kafka instance

You have two alternatives, run with lenses or run in your machine only the kafka with docker. It's preferable that you use lenses

### Using Lenses

There is a free tool that provide monitoring and local development for your kafka env. Only Lenses box is free, is paid for production use.

##### Important: Lenses requires at least 4GB of RAM to works fine. Make sure that your Docker Desktop is providing it.

Just follow:

https://lenses.io/box/

### Using Docker

#### Whats is needed

Docker

#### Installing and starting kafka

Create one file named `docker-compose.yml` with this content:
```
services:
  broker:
    image: confluentinc/cp-kafka:8.0.0
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

  schema-registry:
    image: confluentinc/cp-schema-registry:8.0.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

Basically this file is starting two services on your machine, the `schema registry` and the `broker` itself. Inside the folder that you created the docker compose file, run the command `docker compose up -d`, the `-d` is for second plan run. After the command run sucessfully you should run `docker compose ps` to check if the services are running on your pc. You should see:


```
➜  docker compose ps
NAME              IMAGE                                   COMMAND                  SERVICE           CREATED         STATUS         PORTS
broker            confluentinc/cp-kafka:8.0.0             "/etc/confluent/dock…"   broker            3 seconds ago   Up 2 seconds   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:9101->9101/tcp, [::]:9101->9101/tcp
schema-registry   confluentinc/cp-schema-registry:8.0.0   "/etc/confluent/dock…"   schema-registry   3 seconds ago   Up 2 seconds   0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp
```

Checking the logs, you should see the logs of the service schema-registry:

```
➜  docker compose logs schema-registry | grep -i started
schema-registry  | [2025-12-30 12:52:27,850] INFO Started oeje10s.ServletContextHandler@682af059{ROOT,/,b=null,a=AVAILABLE,h=icksr.RequestHeaderHandler@315105f{STARTED}} (org.eclipse.jetty.server.handler.ContextHandler)
schema-registry  | [2025-12-30 12:52:28,173] INFO Started oeje10s.ServletContextHandler@682af059{ROOT,/,b=null,a=AVAILABLE,h=icksr.RequestHeaderHandler@315105f{STARTED}} (org.eclipse.jetty.ee10.servlet.ServletContextHandler)
schema-registry  | [2025-12-30 12:52:28,190] INFO Started oeje10s.ServletContextHandler@1c92a549{/ws,/ws,b=null,a=AVAILABLE,h=oeje10s.SessionHandler@79b63325{STARTED}} (org.eclipse.jetty.server.handler.ContextHandler)
schema-registry  | [2025-12-30 12:52:28,191] INFO Started oeje10s.ServletContextHandler@1c92a549{/ws,/ws,b=null,a=AVAILABLE,h=oeje10s.SessionHandler@79b63325{STARTED}} (org.eclipse.jetty.ee10.servlet.ServletContextHandler)
schema-registry  | [2025-12-30 12:52:28,196] INFO Started NetworkTrafficServerConnector@67207d8a{HTTP/1.1, (http/1.1, h2c)}{0.0.0.0:8081} (org.eclipse.jetty.server.AbstractConnector)
schema-registry  | [2025-12-30 12:52:28,197] INFO Started icr.ApplicationServer@2d0566ba{STARTING}[12.0.16,sto=5000] @2404ms (org.eclipse.jetty.server.Server)
schema-registry  | [2025-12-30 12:52:28,198] INFO Server started, listening for requests... (io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain)
```

And the kafka service:


```
➜  docker compose logs broker | grep -i started
broker  | [2025-12-30 12:52:24,362] INFO [ControllerServer id=1] Waiting for all of the SocketServer Acceptors to be started (kafka.server.ControllerServer)
broker  | [2025-12-30 12:52:24,362] INFO [ControllerServer id=1] Waiting for all of the SocketServer Acceptors to be started (kafka.server.ControllerServer)
broker  | [2025-12-30 12:52:24,362] INFO [ControllerServer id=1] Finished waiting for all of the SocketServer Acceptors to be started (kafka.server.ControllerServer)
broker  | [2025-12-30 12:52:24,362] INFO [ControllerServer id=1] Finished waiting for all of the SocketServer Acceptors to be started (kafka.server.ControllerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Waiting for all of the SocketServer Acceptors to be started (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Waiting for all of the SocketServer Acceptors to be started (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Finished waiting for all of the SocketServer Acceptors to be started (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Finished waiting for all of the SocketServer Acceptors to be started (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Transition from STARTING to STARTED (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,579] INFO [BrokerServer id=1] Transition from STARTING to STARTED (kafka.server.BrokerServer)
broker  | [2025-12-30 12:52:24,580] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
broker  | [2025-12-30 12:52:24,580] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```
#### Check logs on kafka to see events of consuming and producing

`docker compose logs -f`

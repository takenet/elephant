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
Docker Compose

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

Basically this file is starting two services on your machine, the `Zookeeper` and the `kafka` itself. Inside the folder that you created the docker compose file, run the command `docker-compose up -d`, the `-d` is for second plan run. After the command run sucessfully you should run `docker-compose ps` to check if the services are running on your pc. You should see:


```
λ docker-compose ps

          Name                      Command            State                     Ports
---------------------------------------------------------------------------------------------------------
2-experiment_kafka_1       /etc/confluent/docker/run   Up      0.0.0.0:9092->9092/tcp
2-experiment_zookeeper_1   /etc/confluent/docker/run   Up      0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp
```

Checking the logs, you should see the logs of the service zookeeper:

```
λ docker-compose logs zookeeper | grep -i binding
zookeeper_1  | [2019-11-26 11:36:23,867] INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
```

And the kafka service:


```
λ docker-compose logs kafka | grep -i started
kafka_1      | [2019-11-25 16:39:33,791] INFO [SocketServer brokerId=1] Started 1 acceptor threads for data-plane (kafka.network.SocketServer)
kafka_1      | [2019-11-25 16:39:34,153] DEBUG [ReplicaStateMachine controllerId=1] Started replica state machine with initial state -> Map() (kafka.controller.ZkReplicaStateMachine)
kafka_1      | [2019-11-25 16:39:34,156] DEBUG [PartitionStateMachine controllerId=1] Started partition state machine with initial state -> Map() (kafka.controller.ZkPartitionStateMachine)
kafka_1      | [2019-11-25 16:39:34,200] INFO [SocketServer brokerId=1] Started data-plane processors for 1 acceptors (kafka.network.SocketServer)
kafka_1      | [2019-11-25 16:39:34,220] INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```
#### Check logs on kafka to see events of consuming and producing

`docker-compose logs --f`

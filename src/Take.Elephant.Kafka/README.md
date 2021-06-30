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
version: "3"
services:
  zookeeper:
    image: "confluentinc/cp-zookeeper"
    ports:
      - 2181:2181
    environment:
      - ZOOKEEPER_CLIENT_PORT=2181
    volumes:
      - zookeeperData:/var/lib/zookeeper/data
      - zookeeperLogs:/var/lib/zookeeper/log

  kafka:
    image: "confluentinc/cp-kafka"
    ports:
      - 9092:9092
    environment:
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
    volumes:
      - kafka:/var/lib/kafka/data
    depends_on:
      - zookeeper
volumes:
  kafka:
  zookeeperData:
  zookeeperLogs:

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
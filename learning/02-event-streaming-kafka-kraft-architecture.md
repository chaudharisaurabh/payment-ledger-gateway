# 02. Event Streaming Architecture & Kafka KRaft Mode

---

## 1. Context: Asynchronous Settlement via Event Streaming

In this Payment Gateway architecture, payment acceptance is strictly decoupled from accounting ledger settlement.
- **Fast Path (Ingestion)**: Accepts the payment intent, performs atomic fund reservation, writes to the Transactional Outbox, and returns `HTTP 202 Accepted` to the client in `<25ms`.
- **Durable Path (Settlement)**: Asynchronously consumes events, records double-entry bookkeeping rows, and dispatches webhook notifications.

**Apache Kafka** acts as the high-throughput, partitioned, distributed commit log driving this asynchronous event pipeline.

---

## 2. The Architectural Evolution: ZooKeeper vs. KRaft

Historically, Apache Kafka was unable to operate as an independent distributed system. It relied on an external coordination cluster: **Apache ZooKeeper**.

```
[ Legacy Architecture (Kafka <= 2.8) ]
   +--------------------+          +--------------------+
   |   Kafka Broker 1   |          |   Kafka Broker 2   |
   +---------+----------+          +---------+----------+
             |                               |
             +---------------+---------------+
                             | (Metadata & State Sync)
                             v
               +---------------------------+
               |   Apache ZooKeeper Cluster|
               +---------------------------+

[ Modern KRaft Architecture (Kafka 3.0+ & Production Standard) ]
   +----------------------------------------------------+
   |               Kafka Cluster (KRaft)                |
   |                                                    |
   |  Broker Role                    Controller Role    |
   |  (Message Storage & Routing)    (Metadata & Raft)  |
   +----------------------------------------------------+
              (No External ZooKeeper Dependency)
```

### Limitations of the ZooKeeper Architecture
1. **Dual Metadata State (Dual-Brain Problem)**: Metadata lived in both ZooKeeper and the active Kafka controller broker. Synchronizing changes across both systems introduced race conditions and potential state drift.
2. **Controller Failover Latency**: When the active controller broker failed, the new controller had to reload the entire cluster metadata from ZooKeeper into memory. On large clusters with thousands of partitions, this caused failover pauses lasting several minutes.
3. **Operational Overhead**: Required deploying, monitoring, securing, and backing up two entirely separate distributed systems with distinct JVM configurations and network topologies.

---

## 3. What is KRaft (Kafka Raft Metadata Mode)?

KRaft replaces ZooKeeper by implementing a specialized variant of the **Raft Consensus Algorithm** natively inside Kafka.

### Key Architectural Characteristics
- **Metadata as an Internal Event Log**: Cluster metadata (partition assignments, topic configurations, broker liveness) is stored internally in a dedicated, compacted Kafka topic called `@metadata`.
- **Raft Quorum Controllers**: A subset of Kafka nodes (or combined nodes) act as the Raft Quorum Controller, maintaining an in-memory, replicated log of all cluster state changes.
- **Instantaneous Failover**: Because all controller nodes continuously replicate the `@metadata` partition, if the active controller leader fails, another controller node takes over **in milliseconds** without needing to reload state.
- **Scalability**: Enables Kafka to scale to millions of partitions per cluster, far exceeding ZooKeeper's throughput ceiling.

---

## 4. KRaft Implementation in Our Docker Compose Infrastructure

In this project, we leverage Kafka 3.7+ running in combined **Broker + Controller** mode, which allows running a production-equivalent Kafka setup without the overhead of a separate ZooKeeper container.

```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.0
  container_name: payment-kafka
  restart: always
  ports:
    - "9092:9092"   # External access for host machine / tests
    - "29092:29092" # Internal access for Docker network containers
  environment:
    KAFKA_NODE_ID: 1
    # KRaft Process Roles: node acts as both data broker and quorum controller
    KAFKA_PROCESS_ROLES: 'broker,controller'
    # Voters quorum definition: node 1 is the leader/controller on internal port 29093
    KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
    # Cluster ID: Unique base64 UUID identifying this KRaft cluster
    CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
    KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
    KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
    KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### Configuration Breakdown
- `KAFKA_PROCESS_ROLES: 'broker,controller'`: In development and small-scale environments, a single JVM process serves both roles (data routing and metadata consensus). In multi-rack enterprise deployments, these roles can be separated across dedicated nodes.
- `CLUSTER_ID`: In KRaft, storage directories must be formatted with a cluster ID before starting. By providing a fixed UUID (`MkU3OEVBNTcwNTJENDM2Qk`), container restarts remain deterministic.
- Dual Listeners:
  - `PLAINTEXT://kafka:29092`: Used by Kafdrop, backend containers, and services running within the Docker network.
  - `PLAINTEXT_HOST://localhost:9092`: Used by local Spring Boot instances or integration test suites running on the host machine.

---

## 5. Architectural Talking Points for Technical Interviews

When explaining the messaging layer design in system design interviews:

- **Why did you use KRaft instead of traditional ZooKeeper?**
  > *"We standardized on Kafka in KRaft mode to eliminate the dual-metadata problem and operational overhead of ZooKeeper. KRaft manages cluster consensus natively via an internal metadata log using the Raft protocol. This yields sub-second controller failovers, simpler infrastructure orchestration in Docker/Kubernetes, and a significantly smaller attack surface."*

- **How do you ensure ordered transaction processing in Kafka?**
  > *"Kafka guarantees ordering strictly within an individual partition. In our settlement architecture, every outbox event is published with the `source_account_id` as the message key. Kafka's hash partitioner ensures all events for a given account land on the exact same partition, guaranteeing chronological debit/credit ordering while scaling horizontally across partitions for different accounts."*

- **What producer durability settings do you enforce?**
  > *"For financial settlement events, we configure `acks=all` (wait for full in-sync replica acknowledgment), `enable.idempotence=true` (assigns producer IDs and sequence numbers to eliminate duplicate broker writes), and `retries=Integer.MAX_VALUE` with exponential backoff."*

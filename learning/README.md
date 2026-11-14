# 🧠 Engineering Deep Dives & Architecture Notes

This directory contains technical deep-dives, architectural trade-off analyses, and design rationale documented during the engineering of the **Distributed Payment & Ledger Gateway System**.

These notes serve as technical documentation for the core distributed systems challenges solved in this repository.

---

## 📑 Index of Deep Dives

| # | Topic | Key Concepts Covered |
| :--- | :--- | :--- |
| **01** | [Concurrency Control & JPA Optimistic Locking](01-concurrency-control-jpa-optimistic-locking.md) | The Lost Update Anomaly, Pessimistic vs. Optimistic Locking, JPA `@Version` internals, SQL execution trace, Defense-in-depth strategy. |
| **02** | [Kafka Architecture & KRaft Consensus Mode](02-event-streaming-kafka-kraft-architecture.md) | Distributed commit logs, ZooKeeper limitations, Raft consensus protocol (KRaft), Single-process broker/controller, Docker Compose architecture. |

---

## 🎯 Architectural Philosophy

Every distributed systems choice in this gateway is governed by three non-negotiable principles:
1. **Safety over raw latency**: In financial ledger systems, an aborted transaction is always preferable to an inconsistent balance or double-spend.
2. **Defense in depth**: Never rely on a single layer (e.g. database constraints alone) for data integrity. Edge caching, distributed locks, entity versioning, and SQL-level constraints must reinforce each other.
3. **Operational simplicity**: Eliminate unnecessary cluster dependencies (e.g., leveraging Kafka KRaft to remove ZooKeeper) to reduce operational surface area and failure modes.

# 01. Concurrency Control & JPA Optimistic Locking

---

## 1. Context & The Problem: The "Lost Update" Anomaly

In payment processing, multiple operations often target the same account balance concurrently (e.g., rapid debit card transactions, automated recurring subscriptions, or concurrent API calls).

### The Failure Scenario Without Concurrency Control
Consider an account with an initial balance of **$100.00**. Two independent transactions arrive at the same millisecond:
- **Transaction A** wants to debit **$30.00**.
- **Transaction B** wants to debit **$40.00**.

```
Time    Transaction A                        Transaction B
---------------------------------------------------------------------------------
T1      Reads balance = $100.00              
T2                                           Reads balance = $100.00
T3      Calculates: 100 - 30 = $70.00        
T4                                           Calculates: 100 - 40 = $60.00
T5      Writes balance = $70.00              
T6                                           Writes balance = $60.00 (OVERWRITES A!)
---------------------------------------------------------------------------------
Result: Final balance = $60.00.
        Real total debited = $70.00.
        Financial Loss to Bank = $30.00 (Transaction A's debit was silently lost).
```

This classic race condition is known in database literature as the **Lost Update Anomaly**.

---

## 2. Concurrency Control Paradigms: Pessimistic vs. Optimistic

To maintain ACID consistency across concurrent transactions, systems choose between two primary locking strategies:

| Dimension | Pessimistic Locking (`SELECT ... FOR UPDATE`) | Optimistic Locking (`@Version` / CAS) |
| :--- | :--- | :--- |
| **Philosophical Assumption** | High collision frequency expected. Assume conflicts *will* happen. | Low to moderate collision frequency. Assume conflicts are *rare*. |
| **Mechanism** | Acquires a database row-level lock immediately upon reading. Blocks all other readers/writers until commit. | No database locks acquired during read. Compares version state at write time. |
| **Throughput / Latency** | Lower throughput; threads wait in DB lock queues; higher latency. | High throughput; non-blocking reads; zero lock contention on read. |
| **Deadlock Risk** | Significant risk of deadlocks if multiple tables are locked in differing orders. | **Zero deadlock risk** at the database row lock level. |
| **Best Used For** | Extremely high contention resources (e.g., last ticket for a concert). | Financial accounts with distributed edge locking and retry capabilities. |

---

## 3. How JPA Optimistic Locking Works Under the Hood

### The `@Version` Annotation
In Java/Spring Data JPA, optimistic locking is activated by adding a `@Version` attribute to the entity class.

```java
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    private String id;

    @Column(nullable = false, precision = 18, scale = 4)
    private BigDecimal balance;

    @Version
    private Long version; // Managed automatically by Hibernate/JPA
    
    // getters and setters...
}
```

### The Behind-The-Scenes SQL Protocol
When JPA executes an update on an `@Version`-annotated entity, it transforms standard `UPDATE` statements into a **Compare-And-Swap (CAS)** query:

1. **Transaction A reads**: `id = 'ACC-1', balance = 100.00, version = 1`
2. **Transaction B reads**: `id = 'ACC-1', balance = 100.00, version = 1`
3. **Transaction A commits first**:
   ```sql
   UPDATE accounts 
   SET balance = 70.00, version = 2 
   WHERE id = 'ACC-1' AND version = 1;
   ```
   - **PostgreSQL Result**: `UPDATE 1` (Row found, balance updated to $70, version incremented to 2).
4. **Transaction B attempts to commit**:
   ```sql
   UPDATE accounts 
   SET balance = 60.00, version = 2 
   WHERE id = 'ACC-1' AND version = 1;
   ```
   - **PostgreSQL Result**: `UPDATE 0` (No row matches `WHERE id = 'ACC-1' AND version = 1` because the version in the database is already 2).
5. **JPA Detection & Exception**:
   - Hibernate inspects the row count returned by the JDBC driver.
   - Because `updatedRows == 0`, Hibernate detects that a concurrent update occurred.
   - It aborts the transaction and throws:
     `org.springframework.orm.ObjectOptimisticLockingFailureException` (wrapping `jakarta.persistence.OptimisticLockException`).

---

## 4. Multi-Layer Defense in Our Payment Gateway

Relying solely on JPA optimistic locking can cause high exception rates under bursts. Therefore, this architecture implements a **three-tier defense-in-depth model**:

```
[ Incoming Request ]
        │
        ▼
[ Layer 1: Edge Distributed Lock ]  ──> Redisson (Redis) serializes access per accountId (3s wait, 5s lease)
        │
        ▼
[ Layer 2: JPA Optimistic Lock ]   ──> @Version prevents stale entity updates if edge lock expires
        │
        ▼
[ Layer 3: Relational Invariant ]  ──> Atomic SQL: UPDATE ... WHERE balance >= :amount
```

1. **Layer 1 (Redis Redisson Lock)**: Serializes requests for the *same account* before hitting the database, eliminating 99% of lock collisions.
2. **Layer 2 (JPA `@Version`)**: Guards against race conditions if multiple JVM nodes or background jobs bypass the Redis lock.
3. **Layer 3 (Database Constraint)**: SQL-level conditional decrement (`balance >= amount`) guarantees balance never drops below zero even if application-level calculations fail.

---

## 5. Architectural Talking Points for Technical Interviews

When discussing this design choice in system design reviews:

- **Why not just use Pessimistic Locking (`SELECT FOR UPDATE`)?**
  > *"Pessimistic locking holds active database connections while business logic executes, reducing connection pool availability and introducing deadlock vectors under high traffic. By combining Redis distributed locks at the edge with JPA Optimistic Locking at the entity layer, we achieve non-blocking database reads, zero database deadlocks, and strong data consistency."*

- **How does the system recover from an `OptimisticLockException`?**
  > *"In non-critical workflows, we apply Spring's `@Retryable(retryFor = OptimisticLockException.class, maxAttempts = 3, backoff = @Backoff(delay = 50, multiplier = 1.5))`. However, for financial payments, retrying blindly is dangerous. Instead, we re-fetch the latest account balance, re-validate fund sufficiency, and only then re-attempt the atomic decrement."*

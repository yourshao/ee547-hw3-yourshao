# Problem 2 — ArXiv Paper Discovery with DynamoDB

## Schema Design Decisions

**Why this partition key structure?**  
The table uses a composite key (`PK`, `SK`) so that each required access pattern can be served with an efficient `Query` operation (no table scans).
- **Category browsing / date range:**  
  `PK = "CATEGORY#<category>"`, `SK = "<YYYY-MM-DD>#<arxiv_id>"`.This allows `Query` with `ScanIndexForward=False` for recent papers, or `between()` for date ranges.
- **Paper lookup:**  
  `PK = "PAPER#<arxiv_id>"`, `SK = "META"` — used by GSI2 for direct lookup by ID.
- **Author and keyword queries:**  
  Additional GSIs (`AUTHOR#`, `KEYWORD#`) group items by the same author or keyword for efficient lookups.

**How many GSIs and why?**  
Three Global Secondary Indexes (GSIs) were created:
1. **AuthorIndex (GSI1)** – supports “find all papers by author.”
2. **PaperIdIndex (GSI2)** – enables direct lookup by `arxiv_id`.
3. **KeywordIndex (GSI3)** – supports keyword-based paper search.

These three GSIs cover all five required access patterns, making every query a single-partition `Query` call.

**Denormalization trade-offs**
- **Pros:** All required queries are fast (`Query` only) and predictable in latency.
- **Cons:** Each paper generates multiple duplicate items (for authors, categories, and keywords), increasing storage and write cost.
- **Trade-off:** Prioritize read performance and low-latency access over storage efficiency and strict consistency.

---

## Denormalization Analysis

| Item Type | Avg. Items per Paper |
|------------|----------------------|
| Category items | ≈ 2.0 |
| Author items | ≈ 5.0 |
| Keyword items | ≈ 10.0 |
| Paper ID items | 1.0 |
| **Total** | ≈ 18 items/paper |

**Storage multiplication factor:** ~18×  
(Example: 157 papers → 2,345 total items)

**Most duplication:**  
Keyword index items (each paper generates up to 10 for its top keywords).  
Multi-author papers also contribute to duplication in the AuthorIndex.

---

## Query Limitations

**Queries not efficiently supported:**
- “Count total papers by each author.”
- “Find globally most cited papers.”
- “Aggregate all keywords and their frequencies.”

**Why these are hard in DynamoDB:**  
DynamoDB has no server-side joins, aggregations, or cross-partition sorting.  
Each query must target a specific partition key.  
Global aggregation requires external systems (e.g., Athena, Glue, or MapReduce).

---

## When to Use DynamoDB (vs PostgreSQL)

**Choose DynamoDB when:**
- Access patterns are well-defined and predictable.
- You need millisecond-level read latency and horizontal scalability.
- Denormalization and eventual consistency are acceptable.

**Choose PostgreSQL when:**
- You need ad-hoc queries, JOINs, or aggregation.
- You require strong consistency or complex transactional logic.
- Data model relationships are tightly coupled.

**Trade-off summary:**
- **DynamoDB:** scalability and low latency vs. flexibility
- **PostgreSQL:** query expressiveness vs. scalability

---

## EC2 Deployment

- **EC2 Public IP:** `50.18.13.197`
- **IAM Role ARN:** `arn:aws:iam::JekwenShao:role/EC2-DynamoDB-Role`  
  (with `AmazonDynamoDBFullAccess` permissions)

**Deployment Steps**
1. Launch a `t2.micro` instance (Amazon Linux 2023 x86_64).
2. Attach the IAM role `EC2-DynamoDB-Role` with DynamoDB FullAccess.
3. In the security group, add a **Custom TCP rule** to open **port 8080**.
4. Deploy using the provided script:
   ```bash
   ./deploy.sh <key.pem> <50.18.13.197>

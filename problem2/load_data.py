"""
load_data.py
Load ArXiv papers (papers.json) into a DynamoDB table with denormalized items.
Only uses: boto3 + stdlib.
"""

import sys, os, json, time, re, collections, argparse, datetime
import boto3
from botocore.exceptions import ClientError

STOPWORDS = {
    'the','a','an','and','or','but','in','on','at','to','for','of','with','by','from','up','about','into','through','during',
    'is','are','was','were','be','been','being','have','has','had','do','does','did','will','would','could','should','may','might',
    'can','this','that','these','those','we','our','use','using','based','approach','method','paper','propose','proposed','show'
}

# ---------- Helpers ----------
WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9\-]+")

def iso_date(date_str):
    """normalize to YYYY-MM-DD (accepts date or ISO datetime)."""
    try:
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str
        return datetime.datetime.fromisoformat(date_str.replace('Z','')).date().isoformat()
    except Exception:
        # fallback: keep prefix 10 chars if plausible
        return (date_str or "")[:10]

def tokenize_keywords(text, topk=10):
    if not text:
        return []
    words = [w.lower() for w in WORD_RE.findall(text)]
    words = [w for w in words if w not in STOPWORDS and len(w) >= 3]
    cnt = collections.Counter(words)
    return [w for w, _ in cnt.most_common(topk)]

def ensure_table(dynamodb, table_name, region):
    """Create table with GSIs if not exists."""
    ddb = dynamodb
    try:
        tbl = ddb.Table(table_name)
        tbl.load()
        print(f"Table exists: {table_name}")
        return tbl
    except ClientError:
        pass

    print(f"Creating DynamoDB table: {table_name}")
    # Main table: PK + SK (both strings)
    # GSIs:
    #  - AuthorIndex:     GSI1PK (AUTHOR#name),   GSI1SK (YYYY-MM-DD#arxiv_id)
    #  - PaperIdIndex:    GSI2PK (PAPER#id),      GSI2SK ('A')
    #  - KeywordIndex:    GSI3PK (KEYWORD#word),  GSI3SK (YYYY-MM-DD#arxiv_id)
    ddb_resource = ddb
    params = {
        "TableName": table_name,
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
            {"AttributeName": "GSI2SK", "AttributeType": "S"},
            {"AttributeName": "GSI3PK", "AttributeType": "S"},
            {"AttributeName": "GSI3SK", "AttributeType": "S"},
        ],
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "AuthorIndex",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
            {
                "IndexName": "PaperIdIndex",
                "KeySchema": [
                    {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
            {
                "IndexName": "KeywordIndex",
                "KeySchema": [
                    {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            },
        ],
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    }

    table = ddb_resource.create_table(**params)
    print("Creating GSIs: AuthorIndex, PaperIdIndex, KeywordIndex")
    table.wait_until_exists()
    print("Table status:", table.table_status)
    return table

def put_batch(table, items):
    with table.batch_writer(overwrite_by_pkeys=['PK', 'SK']) as bw:
        for it in items:
            bw.put_item(Item=it)

def build_items_for_paper(p):
    """
    Input paper schema expected from HW#1:
    {
      "arxiv_id": "2301.12345",
      "title": "...",
      "authors": ["Author1", "Author2"],
      "abstract": "...",
      "categories": ["cs.LG", "cs.AI"],
      "published": "2023-01-15T10:30:00Z"
    }
    """
    arxiv_id = p.get("arxiv_id")
    title = p.get("title")
    authors = p.get("authors") or []
    abstract = p.get("abstract") or ""
    categories = p.get("categories") or []
    published_iso = p.get("published") or ""
    date_only = iso_date(published_iso)

    keywords = p.get("keywords")
    if not keywords:
        keywords = tokenize_keywords(abstract, topk=10)

    base_attrs = {
        "arxiv_id": arxiv_id,
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "categories": categories,
        "keywords": [k.lower() for k in keywords],
        "published": published_iso or (date_only + "T00:00:00Z"),
    }

    items = []

    # 1) Category items (for recent & daterange)
    for cat in categories:
        items.append({
            "PK": f"CATEGORY#{cat}",
            "SK": f"{date_only}#{arxiv_id}",
            **base_attrs
        })

    # 2) Canonical paper item (for ID lookup via GSI2)
    items.append({
        "PK": f"PAPER#{arxiv_id}",
        "SK": "A",
        "GSI2PK": f"PAPER#{arxiv_id}",
        "GSI2SK": "A",
        **base_attrs
    })

    # 3) Author pointer items (for AuthorIndex)
    for a in authors:
        items.append({
            "PK": f"AUTHOR#{a}",
            "SK": f"{date_only}#{arxiv_id}",
            "GSI1PK": f"AUTHOR#{a}",
            "GSI1SK": f"{date_only}#{arxiv_id}",
            **base_attrs
        })

    # 4) Keyword pointer items (for KeywordIndex)
    for kw in base_attrs["keywords"]:
        items.append({
            "PK": f"KEYWORD#{kw}",
            "SK": f"{date_only}#{arxiv_id}",
            "GSI3PK": f"KEYWORD#{kw}",
            "GSI3SK": f"{date_only}#{arxiv_id}",
            **base_attrs
        })

    breakdown = {
        "category": len(categories),
        "author": len(authors),
        "keyword": len(base_attrs["keywords"]),
        "paper": 1
    }
    return items, breakdown

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("papers_json_path")
    ap.add_argument("table_name")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-west-2"))
    args = ap.parse_args()

    session = boto3.Session(region_name=args.region)
    dynamodb = session.resource("dynamodb")

    table = ensure_table(dynamodb, args.table_name, args.region)

    # Load papers.json
    print(f"Loading papers from {args.papers_json_path} ...")
    with open(args.papers_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "papers" in data:
        papers = data["papers"]
    else:
        papers = data

    print("Extracting keywords from abstracts...")
    total_items = 0
    total_breakdown = collections.Counter()
    batch = []
    BATCH_FLUSH = 24  # small batches to keep memory low

    start = time.time()
    for p in papers:
        items, bkd = build_items_for_paper(p)
        batch.extend(items)
        total_items += len(items)
        total_breakdown.update(bkd)
        if len(batch) >= BATCH_FLUSH:
            put_batch(table, batch)
            batch.clear()

    if batch:
        put_batch(table, batch)

    dur = time.time() - start
    n_papers = len(papers)
    factor = (total_items / n_papers) if n_papers else 0.0
    print(f"Loaded {n_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {factor:.1f}x")
    print("\nStorage breakdown:")
    for k in ("category","author","keyword","paper"):
        avg = (total_breakdown[k] / n_papers) if n_papers else 0.0
        print(f"  - {k.capitalize()} items: {total_breakdown[k]} ({avg:.1f} per paper avg)")
    print(f"\nDone in {dur:.2f}s.")

if __name__ == "__main__":
    main()

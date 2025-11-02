#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_papers.py
Implements five query patterns against DynamoDB.
Outputs JSON to stdout.
"""

import sys, os, json, time, argparse
import boto3
from boto3.dynamodb.conditions import Key

def ddb_table(table_name, region):
    session = boto3.Session(region_name=region)
    return session.resource("dynamodb").Table(table_name)

def _print_json(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

def query_recent_in_category(table_name, category, limit=20, region="us-west-2"):
    t0 = time.time()
    table = ddb_table(table_name, region)
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    items = resp.get('Items', [])
    payload = {
        "query_type": "recent_in_category",
        "parameters": {"category": category, "limit": int(limit)},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }
    _print_json(payload)

def query_papers_by_author(table_name, author_name, region="us-west-2"):
    t0 = time.time()
    table = ddb_table(table_name, region)
    resp = table.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    items = resp.get('Items', [])
    payload = {
        "query_type": "papers_by_author",
        "parameters": {"author": author_name},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }
    _print_json(payload)

def get_paper_by_id(table_name, arxiv_id, region="us-west-2"):
    t0 = time.time()
    table = ddb_table(table_name, region)
    resp = table.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    items = resp.get('Items', [])
    result = items[0] if items else None
    payload = {
        "query_type": "get_paper_by_id",
        "parameters": {"arxiv_id": arxiv_id},
        "results": [result] if result else [],
        "count": 1 if result else 0,
        "execution_time_ms": int((time.time()-t0)*1000)
    }
    _print_json(payload)

def query_papers_in_date_range(table_name, category, start_date, end_date, region="us-west-2"):
    # start_date/end_date format: YYYY-MM-DD
    t0 = time.time()
    table = ddb_table(table_name, region)
    resp = table.query(
        KeyConditionExpression=(
                Key('PK').eq(f'CATEGORY#{category}') &
                Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        )
    )
    items = resp.get('Items', [])
    payload = {
        "query_type": "papers_in_date_range",
        "parameters": {"category": category, "start": start_date, "end": end_date},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }
    _print_json(payload)

def query_papers_by_keyword(table_name, keyword, limit=20, region="us-west-2"):
    t0 = time.time()
    table = ddb_table(table_name, region)
    resp = table.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    items = resp.get('Items', [])
    payload = {
        "query_type": "papers_by_keyword",
        "parameters": {"keyword": keyword, "limit": int(limit)},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }
    _print_json(payload)

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_common(p):
        p.add_argument("--table", default=os.environ.get("DDB_TABLE","arxiv-papers"))
        p.add_argument("--region", default=os.environ.get("AWS_REGION","us-west-1"))

    p1 = sub.add_parser("recent")
    p1.add_argument("category")
    p1.add_argument("--limit", type=int, default=20)
    add_common(p1)

    p2 = sub.add_parser("author")
    p2.add_argument("author_name")
    add_common(p2)

    p3 = sub.add_parser("get")
    p3.add_argument("arxiv_id")
    add_common(p3)

    p4 = sub.add_parser("daterange")
    p4.add_argument("category")
    p4.add_argument("start_date")
    p4.add_argument("end_date")
    add_common(p4)

    p5 = sub.add_parser("keyword")
    p5.add_argument("keyword")
    p5.add_argument("--limit", type=int, default=20)
    add_common(p5)

    args = ap.parse_args()

    if args.cmd == "recent":
        query_recent_in_category(args.table, args.category, args.limit, args.region)
    elif args.cmd == "author":
        query_papers_by_author(args.table, args.author_name, args.region)
    elif args.cmd == "get":
        get_paper_by_id(args.table, args.arxiv_id, args.region)
    elif args.cmd == "daterange":
        query_papers_in_date_range(args.table, args.category, args.start_date, args.end_date, args.region)
    elif args.cmd == "keyword":
        query_papers_by_keyword(args.table, args.keyword, args.limit, args.region)

if __name__ == "__main__":
    main()

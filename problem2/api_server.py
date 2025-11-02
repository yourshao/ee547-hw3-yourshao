"""
api_server.py
HTTP API using only http.server + boto3.
"""

import os, sys, json, urllib.parse, argparse, time
from http.server import BaseHTTPRequestHandler, HTTPServer
import boto3
from boto3.dynamodb.conditions import Key

DEFAULT_REGION = os.environ.get("AWS_REGION","us-west-1")
DEFAULT_TABLE  = os.environ.get("DDB_TABLE","arxiv-papers")

def table():
    session = boto3.Session(region_name=DEFAULT_REGION)
    return session.resource("dynamodb").Table(DEFAULT_TABLE)

def json_response(handler, code, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stdout.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt%args))

    def do_GET(self):
        t0 = time.time()
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/")
            qs = urllib.parse.parse_qs(parsed.query)

            if path == "/papers/recent":
                category = (qs.get("category") or [""])[0]
                limit = int((qs.get("limit") or ["20"])[0])
                if not category:
                    return json_response(self, 400, {"error":"missing category"})
                resp = table().query(
                    KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
                    ScanIndexForward=False,
                    Limit=limit
                )
                return json_response(self, 200, {
                    "category": category,
                    "papers": resp.get("Items", []),
                    "count": len(resp.get("Items", [])),
                    "latency_ms": int((time.time()-t0)*1000)
                })

            if path.startswith("/papers/author/"):
                author_name = urllib.parse.unquote(path.split("/papers/author/",1)[1])
                resp = table().query(
                    IndexName='AuthorIndex',
                    KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
                )
                return json_response(self, 200, {
                    "author": author_name,
                    "papers": resp.get("Items", []),
                    "count": len(resp.get("Items", [])),
                    "latency_ms": int((time.time()-t0)*1000)
                })

            if path.startswith("/papers/keyword/"):
                keyword = urllib.parse.unquote(path.split("/papers/keyword/",1)[1]).lower()
                limit = int((qs.get("limit") or ["20"])[0])
                resp = table().query(
                    IndexName='KeywordIndex',
                    KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword}'),
                    ScanIndexForward=False,
                    Limit=limit
                )
                return json_response(self, 200, {
                    "keyword": keyword,
                    "papers": resp.get("Items", []),
                    "count": len(resp.get("Items", [])),
                    "latency_ms": int((time.time()-t0)*1000)
                })

            if path.startswith("/papers/") and path.count("/") == 2:
                arxiv_id = urllib.parse.unquote(path.split("/papers/",1)[1])
                resp = table().query(
                    IndexName='PaperIdIndex',
                    KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
                )
                items = resp.get("Items", [])
                if not items:
                    return json_response(self, 404, {"error":"paper not found"})
                return json_response(self, 200, {
                    "paper": items[0],
                    "latency_ms": int((time.time()-t0)*1000)
                })

            if path == "/papers/search":
                category = (qs.get("category") or [""])[0]
                start = (qs.get("start") or [""])[0]
                end   = (qs.get("end") or [""])[0]
                if not (category and start and end):
                    return json_response(self, 400, {"error":"missing category/start/end"})
                resp = table().query(
                    KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') &
                                           Key('SK').between(f'{start}#', f'{end}#zzzzzzz')
                )
                return json_response(self, 200, {
                    "category": category,
                    "start": start,
                    "end": end,
                    "papers": resp.get("Items", []),
                    "count": len(resp.get("Items", [])),
                    "latency_ms": int((time.time()-t0)*1000)
                })

            return json_response(self, 404, {"error":"not found"})
        except Exception as e:
            return json_response(self, 500, {"error":"server error", "detail": str(e)})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", nargs="?", default="8080")
    args = ap.parse_args()
    port = int(args.port)
    srv = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Server listening on :{port} (table={DEFAULT_TABLE}, region={DEFAULT_REGION})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.server_close()
    finally:
        srv.server_close()

if __name__ == "__main__":
    main()

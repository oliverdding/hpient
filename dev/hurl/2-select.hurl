POST http://dev:8123/
Content-Type: text/plain
Connection: Close
```
SELECT
    *
FROM system.table_engines
```

HTTP/1.1 200
Connection: Close
Transfer-Encoding: chunked
X-ClickHouse-Format: TabSeparated
[Asserts]
body contains "Null"

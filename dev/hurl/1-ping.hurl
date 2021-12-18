GET http://dev:8123/ping
Connection: Close

HTTP/1.1 200
Connection: Close
Transfer-Encoding: chunked
[Asserts]
body == "Ok.\n"

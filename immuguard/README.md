# Immuguard

## Abstract

This tool can be used to check immudb health.
It periodically connects to all databases configured in immudb and fetch their status, and exposes a simple HTTP interface that reports the global health.

## Usage
```
Usage of ./immuguard:
  -addr string
        IP address of immudb server
  -debug
        Enable debug output
  -delay int
        Delay between scans (default 60)
  -pass string
        Password for authenticating to immudb; overrides env variable IMMUDB_ADMIN_PASSWORD (default "immudb")
  -port int
        Port number of immudb server (default 3322)
  -progress int
        Threshold for making progress reducing queue (ms) (default 60000)
  -reqtimeout int
        Max Pending request time (ms) (default 60000)
  -timeout int
        API timeout (default 5)
  -user string
        Username for authenticating to immudb (default "immudb")

```

Use `-addr` and `-port` to open a connection to immudb. Use `-user` and `-pass` to specify credentials of a immudb user with admin privileges. You can also specify the password using the environmental variable `IMMUDB_ADMIN_PASSWORD`. The command line option (if present) overrides the environmental variable (if present).

## Timeout

User can specify three different timeouts:
- `progress`
- `reqtimeout`
- `timeout`

### Progress threshold
When inserting massive data, the database can start queueing some of that. If the queue size is not reducing but it continues to grow for more than this threshold, the database is marked as failing.

### reqtimeout threshold
If the latest served request is older than this threshold, the database is marked as failing.

### API timeout
General timeout for every gRPC call from immuguard to immudb: request must be served within this timeout.

## Endpoint

This tool exposes three REST endpoints:
- `/livez`
- `/immustatus`
- `/version`

### livez

Use this endpoint to monitor immuguard (the tool) status. It should always return a 200 code and the string `alive`.

### immustatus
This endpoint gives the status of immudb. It will return a 200 code and the string `OK` if everything is fine, and a 500 HTTP code with the string "FAIL" if immudb is failing.

### version

Return immuguard version and build information.

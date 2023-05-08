# Paralsql

Inserts rows into a table using different workers in parallel

```
Usage of ./paralsql:
  -addr string
    	IP address of immudb server
  -db string
    	Name of the database to use (default "defaultdb")
  -debug
    	log level: debug
  -pass string
    	Password for authenticating to immudb (default "immudb")
  -port int
    	Port number of immudb server (default 3322)
  -rows int
    	Rows to be inserted by a worker (default 1000)
  -txsize int
    	Transaction size (default 256)
  -user string
    	Username for authenticating to immudb (default "immudb")
  -workers int
    	Number of workers (default 1)
```


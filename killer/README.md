# Immudb Killer 

Do you want to know what happens to your transaction if immudb is killed for any reason?
This is the tool for you.

This tools spawns a immudb subprocess, launch multiple writer goroutines inserting data on it, and periodically kills immudb,  and then it respawns it. Again and again.

All data is also recorded on a mere text file, and there is also an option to check if every key/value recorded in it is actually present in immudb.

## Usage
```
Usage of ./killer:
  -addr string
        IP address of immudb server
  -batchsize int
        Batch size (default 1000)
  -db string
        Name of the database to use (default "defaultdb")
  -duration int
        total test duration (seconds) (default 60)
  -immudb string
        full immudb binary path (default "immudb")
  -interval int
        Single immudb run duration (seconds) (default 30)
  -operation string
        Operation { read | write } (default "write")
  -pass string
        Password for authenticating to immudb (default "immudb")
  -port int
        Port number of immudb server (default 3322)
  -rest int
        Time between immudb restarts (seconds) (default 3)
  -seed int
        Key seed
  -statefile string
        State file (default "stor.txt")
  -user string
        Username for authenticating to immudb (default "immudb")
  -workers int
        Number of concurrent insertion processes (default 1)
```

You have to provide a immudb binary: you can specify its full path using the `-immudb` option. If immudb is in current dir, please use `./immudb`.
The database is killed every `interval` seconds. Then there is a small delay (parameter `rest`) and it is restarted. The full cycle is repeated until a total of `duration` seconds has elapsed.

`operation` can be `write` (insertion with periodic killing) or `read`. In that case, no process killing is performed, just a quick readback and check.

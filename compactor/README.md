# Immudb Compactor tool

## Abstract

Indexes are the most disk-consuming structure in immudb, and can grow fast.

This tool is used to perform periodic maintenance to immudb index structure.
Over time, immudb indexes need to be purged of old references. Immudb engine exposes some
endpoint to perform this kind of maintenance, and this tool just interfaces with them to
do index vacuuming.

## Modes of operation

The maintenance can be performed in three different ways:
- online compaction
- percentage compaction
- full flush

In all three modes, new indexes are calculated and old one are discarded. Indexes are organized in chunk files; each time a file only contains discarded indexes, it is automatically deleted.

### Online compaction

This kind of compaction is performed by immudb during normal write operations: once the new written data reaches the percentage threshold configured per one database, immudb rebuilds its indexes up to the same percentage, discarding old ones.

For every database, users can specify a percentage of total written data to be reindexed on every write.

This tool can be used to enable this mode, and to set the percentage threshold. Once this is done, there is no need to run this tool: the compaction will happen automatically.

### Flush (percentage) compaction

In this mode, the tool calls for immudb to immediately perform a partial compaction, reindexing the oldest data up to the specified percentage. It is similar to the previous mode, but it is performed immediately and must be periodically issued.
The advantage is that you have control on the time when compaction is performed, so that you can leverage periods of less intense activity (e.g.: weekends or nights).

### Full compaction

All indexes are rebuilt. Very resource intensive, but it gives you the most compact representation of indexes.

## Usage
```
Usage of ./compactor:
  -addr string
        IP address of immudb server; overrides env variable IMMUDB_ADDRESS (default "127.0.0.1")
  -all
        Compact all databases
  -cleanup-percentage float
        Set cleanup percentage for online compaction (requires oneshot and immudb restart) (default -1)
  -db string
        Name of the database to use (default "defaultdb")
  -delay int
        Delay between compactions (default 3600)
  -flush
        If set, instead of setting cleanup-percentage, does a flush compaction. Note: cleanup-percentage MUST be set
  -oneshot
        Perform just one compaction then exit
  -pass string
        Password for authenticating to immudb; overrides env variable IMMUDB_ADMIN_PASSWORD (default "immudb")
  -port int
        Port number of immudb server (default 3322)
  -user string
        Username for authenticating to immudb (default "immudb")

Env variables:
        - IMMUDB_ADMIN_PASSWORD (optionally encrypted) immudb password
        - IMMUDB_ADDRESS        immudb server address
```

Use `-addr` and `-port` to open a connection to immudb. Address can be also fetched from environmental variable `IMMUDB_ADDRESS`. Use `-user` and `-pass` to specify credentials of a immudb user with admin privileges. You can also specify the password using the environmental variable `IMMUDB_ADMIN_PASSWORD`. In both cases, the command line option (if present) overrides the environmental variable (if present).

You can operate on a single database or on all databases at once. Use `-db` and `-all` flags to select. By default, only one database (`defaultdb`) is affected.

This tool can perform a run and exit, or ou can also choose to have the tool perform a compaction, then sleep for a delay, then run again and so on. Use option `-oneshot` in the former case and `delay` in the latter. By default, this tool is continuosly running and a compaction is executed every 3600 seconds.

Finally, you can then select the compaction mode: by default, a full compaction is performed. You can enable flush (percentage) compaction by setting `-cleanup-percentage` **and** `-flush`.
To simply set the online compaction, specify `-cleanup-percentage` and `-oneshot` but not `-flush`

## Kubernetes example

In kubernetes, you can configure a cronjob so that the compactor is periodically scheduled.
In file `compactor.yaml` you find an example of such a configuration. You will have to configure
a secret holding credentials for immudb, and put the reference to such secret in `compactor.yaml`, and configure the service name (or stateful set) for the compactor.


## AES encoding/decoding
To help you keep credentials encrypted at rest, you can specify an AES key (in form of 32 hex digits) that encrypts immudb password. You can then provide at compile time this key and this tools will decrypt the password at runtime.
To achieve this, just set the `AES_KEY` environmental variable when invoking the `Makefile` and then prepend the encrypted hex representation of the password with the prefix `enc:`.

# Replicator

## Abstract

This tool takes care of the configuration needed to setup and maintain a full replica of an immudb instance. Periodically, it polls the master instance, extract the list of databases, checks which databases are missing on the replica, and configure the (eventually) missing ones. It creates a user on the master and creates the replicated database with all setting needed.

It has also backup functionality: it can perform a backup of all databases on the replica, saving all immudb data files in a tarball, local or remote (via sftp or s3).

## Settings and usage

```
Usage of ./replicator:
  -data-dir string
        Immudb data directory (for backup) [DATADIR] (default "/var/lib/immudb")
  -delay int
        Delay between compactions in seconds (default 3600)
  -follower-pass string
        Follower password for immudb databases [FOLLOWER_PASSWORD] (default "zyQQurat.0")
  -follower-user string
        Follower username for immudb databases (default "follower")
  -master-addr string
        IP address of immudb master [MASTER_ADDRESS]
  -master-pass string
        Admin password for master immudb [MASTER_PASSWORD] (default "immudb")
  -master-port int
        Port number of immudb master (default 3322)
  -master-user string
        Admin username for master immudb (default "immudb")
  -replica-addr string
        IP address of immudb replica [REPLICA_ADDRESS]
  -replica-pass string
        Admin password for replica immudb [REPLICA_PASSWORD] (default "immudb")
  -replica-port int
        Port number of immudb replica (default 3322)
  -replica-user string
        Admin username for replica immudb (default "immudb")
```

Since the tool needs to connect to two different immudb instance, you have to provide credentials for both of them: address, port, username and password must be set for both the master instance and the replica.

In addiction of that, you also need to set the username and the password you want to use for replication (`-follower-user` and `-follower-pass`: when a new database appears on the master, this user will be created on it and it will be given all permissions needed for replication.

You can specify a delay between polls with `-delay` (default, 1 hour). Finally, if you want to use backup functionality, you need to specify the directory that is holding immudb data, with `-data-dir`.

## Typical usage

Tipically, this tool is deployed alongside the replica. In Kubernetes, this can be a sidecar container in the same pod as the immudb replica, so that the follower is on the loopback (127.0.0.1).


## Backup
This tool is listening for REST request at port 8081.
You can start a backup making a POST request at the endpoint `/backup/start`, with a json structure holding this data:

```json
{
"type" : "string",
"path" : "string",
"host" : "string",
"port" : 1234,
"username" : "string",
"password" : "string",
}
```

`type` can be (at the moment) `file`, `sftp` or `s3`. Not all remaining data are needed for every backup type.

For `file` backup type, you only need to specify the path of the desired resulting tarball archive. For `sftp`, you also need to specify `host`, `port`, `username` and `password` of the remote host. SSH certificates are not supported at the moment. For `s3` backup, you must set the `host`field to the AWS region you are using, the `path` must be in the form `s3://bucket_name/full/path.tar.gz`, in the `username` you set the access key and in `password` the secret key.

To perform a backup, for every database present on the replica, this tools unloads the database from immudb, so that all data is flushed to disk and no modification are incoming, saves the data files, and then load the database back again.
This has the advantage of backing up data at max speed (it is a file copy) without having to worry about consistency.

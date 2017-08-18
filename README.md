# Docker instance to Route53 SRV record synchronization daemon
 
manages the SRV record registration in Route53 for Docker containers running on this host.

When a container is start, the registrator will create a SRV record for each
exposed port of a container which has a matching `SERVICE_<exposed-port>_NAME` environment
variable. If the container exposes a single port, it is sufficient to have a `SERVICE_NAME`
environment variable.

The registrator will create SRV records in Route53 with '<hostname>:<container-id>'
as set identifier. This allows the registrator to synchronise SRV records with
the current state of the running instances on the host. 

The registrator has three commands: remove\_all, sync and daemon.

```
remove\_all - remove all service records point to this host, but run on host shutdown
sync        - synchronise the service records with the running containers 
daemon      - continuously update the SRV records by subscribing to the Docker event stream
```

you must specify, either the dns name or the Route53 hosted zone id:

```
  --dns-name TEXT        to synchronize the SRV records with.
  --hosted-zone-id TEXT  to synchronize the SRV records with.
```

it is recommended that you specify the fully qualified hostname of your machine too
```
  --hostname HOSTNAME        to use in SRV records.
```

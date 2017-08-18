# Docker service registrator for Route53 
 
manages the SRV record registration in Route53 for Docker containers running on this host.

When a container is start, the registrator will create a SRV record for each
exposed port of a container which has a matching `SERVICE_<exposed-port>_NAME` environment
variable. If the container exposes a single port, it is sufficient to have a `SERVICE_NAME`
environment variable.

The registrator will create SRV records in Route53 with '<hostname>:<container-id>'
as set identifier. This allows the registrator to synchronise SRV records with
the current state of the running instances on the host. 

The registrator has three commands: remove\_all, sync and daemon.

## Daemon mode
When the registrator starts in daemon mode it will first do a full sync, to ensure that
the SRV records in the zone are actually reflecting docker instances running on this host.

After that, it will process Docker container start and die events to update the zone.

## Sync
You can run a standalone sync command to ensure that the SRV records in the zone are 
actually reflecting docker instances running on this host. 

## Remove all
When the host is shutdown, it is wise to run the remove\_all command to remove all SRV
records pointing to this host.


```
remove_all  - remove all service records point to this host, but run on host shutdown
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

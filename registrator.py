#!/usr/bin/env python 
import sys
import json
import logging
import click
import docker
import boto3
import socket
from botocore.exceptions import ClientError
from route53cache import Route53Cache

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ECSServiceRegistrator')
log.setLevel('INFO')

class ECSServiceRegistrator:
    """
    manages the SRV record registration in Route53 for Docker containers running on this host.

    When a container is start, the registrator will create a SRV record for each
    exposed port which has a matching SERVICE_<exposed-port>_NAME environment
    variable. If the container exposes a single port, it suffices to have a SERVICE_NAME
    environment variable.

    The registrator will create SRV records in Route53 with '<hostname>:<container-id>'
    as set identifier. This allows the registrator to synchronise SRV records with
    the current state of the running instances on the host. 

    The registrator has three commands: 'remove_all', 'sync' and 'daemon'.
        
        remove_all  - remove all service records point to this host
        sync        - synchronise the service records with the running containers
        daemon      - continuously update the SRV records by subscribing to the Docker event stream
        
    """
    def __init__(self, dns_name, hosted_zone_id, hostname):
        """
        constructor. determines the Route53 hosted zone by either the dns_name or the hosted_zone_id.
        """
        assert hostname is not None
        assert hosted_zone_id is not None or dns_name is not None

        self.dockr = docker.from_env()
        self.hostname = hostname
        self.cache = Route53Cache(dns_name, hostname, hosted_zone_id)
        
        assert self.hostname == hostname        

    @property
    def dns_name(self):
        return self.cache.dns_name

    @property
    def hosted_zone_id(self):
        return self.cache.hosted_zone_id


    def get_environment_of_container(self, container):
        """
        returns the environment variables of the container as a dictionary.
        """
        assert container is not None

        result = {}
        env = container.attrs['Config']['Env']
        for e in env:
            parameter = e.split('=', 1)
            result[parameter[0]] = parameter[1]

        assert len(env) == len(container.attrs['Config']['Env'])

        return result

    def create_service_record(self, container_id, port, name):
        """
        create a SRV record for the specified container id, port and name.
        """
        return {
            "Name": "%s.%s" % (name, self.dns_name),
            "Weight": 1,
            "Type": "SRV",
            "ResourceRecords": [
                    {
                        "Value": "1 1 %s %s" % (str(port), self.hostname)
                    }
            ],
            "TTL": 0,
            "SetIdentifier": "%s:%s" % (self.hostname, container_id)
        }

    def create_service_records(self, container):
        """
        creates a service record for each exposed port of the specified container
        that has a matching environment variable 'SERVICE_<port>_NAME'.
        If a single port is exposed, a matching SERVICE_NAME suffices.

        """
        result = []
        env = self.get_environment_of_container(container)
        ports = container.attrs['NetworkSettings']['Ports']

        for port in ports:
            if ports[port] is None:
                # no ports exposed
                continue

            hostPort = ports[port][0]['HostPort']
            service_name = 'SERVICE_%s_NAME' % port.split('/')[0]
            if service_name in env:
                result.append(self.create_service_record(
                    container.attrs['Id'], hostPort, env[service_name]))
            elif 'SERVICE_NAME' in env and len(ports) == 1:
                result.append(self.create_service_record(
                    container.attrs['Id'], hostPort, env['SERVICE_NAME']))
            else:
                pass

        return result

    def container_started(self, container_id, event):
        """
        create a service record for all exposed services of the specified container.
        """
        try:
            container = self.dockr.containers.get(container_id)
            service_records = self.create_service_records(container)
            if len(service_records) > 0:
                log.info('registering %d SRV record for container %s' %
                         (len(service_records), container_id))
                self.cache.add(service_records)

        except docker.errors.NotFound as e:
            log.error('container %s does not exist.' % container_id)

    def container_died(self, container_id, event):
        """
        remove all service records associated with the specified container.
        """
        set_identifier = '%s:%s' % (self.hostname, container_id)
        records =  self.cache.find_service_records_by_set_identifier(set_identifier)
        if len(records) > 0:
            log.info('deregistering %d SRV records for container %s' %
                     (len(records), container_id))
            self.cache.remove(records)
        else:
            log.debug('No SRV record found for container with id %s' %
                      container_id)

    def sync(self):
        """
        synchronizes the service records of the hosted zone against the currently running docker instances.
        SRV records associated with containers on this host which are no longer running, will be removed.
        Missing SRV records from running containers are added.
        """
        containers = self.dockr.containers.list()
        in_zone = set(map(lambda rr: rr['SetIdentifier'], self.cache.get_all()))
        running = set(map(lambda c: '%s:%s' % (self.hostname, c.attrs['Id']), containers))
        to_delete = in_zone - running
        to_add = running - in_zone

        if len(to_delete) > 0 or len(to_add) > 0:
            log.info('zone "%s" for host %s out of sync, adding %d and removing %d records' % (
                self.dns_name, self.hostname, len(to_add), len(to_delete)))
        else:        
            log.info('zone "%s" for host %s in sync, %d records found for %d running containers' % (self.dns_name, self.hostname, len(in_zone), len(running)))

        if len(to_delete) > 0:
            records = filter(lambda r: r['SetIdentifier'] in to_delete, self.cache.get_all())
	    for r in records:
		log.info('removing SRV record %s %s' % (r['Name'], r['ResourceRecords'][0]['Value']))
            self.cache.remove(records)

        if len(to_add) > 0:
            containers = filter(lambda c: '%s:%s' % (self.hostname, c.attrs['Id']) in to_add, containers)
            records = []
            for c in containers:
                records.extend(self.create_service_records(c))

            if len(records) > 0:
                for r in records:
                    log.info('adding SRV record %s %s' % (r['Name'], r['ResourceRecords'][0]['Value']))
                self.cache.add(records)

    def remove_all(self):
        """
        remove all SRV records from the hosted zone associated with this host. Run this on the shutdown
        event of the host.
        """
        self.cache.remove(self.cache.get_all())

    def process_events(self):
        """
        Process docker container start and die events.
        """
        log.info('found %d SRV records for host %s' %
                 (len(self.cache.get_all()), self.hostname))
        for e in self.dockr.events():
            lines = filter(lambda l: len(l) > 0, e.split('\n'))
            for line in lines:
                event = json.loads(line)
                if event['Type'] == 'container':
                    if event['status'] == 'start':
                        self.container_started(event['id'], event)
                    elif event['status'] == 'die':
                        self.container_died(event['id'], event)
                    else:
                        log.debug('skipping event %s' % event['status'])
                else:
                    pass  # boring...

    
@click.group()
@click.option('--dns-name', required=False, help='to synchronize the SRV records with.')
@click.option('--hosted-zone-id', required=False, help='to synchronize the SRV records with.')
@click.option('--hostname', required=False, default=socket.getfqdn(), help='to use in SRV records.')
@click.pass_context
def cli(ctx, dns_name, hosted_zone_id, hostname):
    if dns_name is None and hosted_zone_id is None:
        click.echo('either --dns-name or --hosted-zone-id has to be specified.')
        sys.exit(1)
    ctx.obj['dns_name'] = dns_name
    ctx.obj['hosted_zone_id'] = hosted_zone_id
    ctx.obj['hostname'] = hostname


@cli.command()
@click.pass_context
def daemon(ctx):
    """
    process docker container 'start' and 'die' events to add and delete SRV records accordingly.
    """
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.sync()
    e.process_events()

@cli.command()
@click.pass_context
def remove_all(ctx):
    """
    remove all SRV records associated with this host.
    """
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.remove_all()

@cli.command()
@click.pass_context
def sync(ctx):
    """
    Synchronize the SRV records with the current docker containers.
    """
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.sync()

if __name__ == '__main__':
    cli(obj={})

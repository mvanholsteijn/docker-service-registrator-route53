#!/usr/bin/env python 
import sys
import json
import logging
import click
import docker
import boto3
import socket
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ECSServiceRegistrator')
log.setLevel('INFO')

class Route53Cache:
    def __init__(self, dns_name, hostname, hosted_zone_id):
        assert hostname is not None
        self.route53 = boto3.client('route53')

        self.dns_name = dns_name
        self.hostname = hostname
        self.hosted_zone_id = hosted_zone_id

        if hosted_zone_id is not None:
            self.get_dns_name_for_hosted_zone_id(hosted_zone_id)
        else:
            self.get_hosted_zone_id_for_dns_name(dns_name)

        self.srv_records = []
        self.load()

    def is_managed_resource_record_set(self, rr):
        """
        returns true if the resource record is a SRV record with the  SetIdentifier starting with 'self.hostname':

        require:
            rr is not None
            'Type' in rr
            'SetIdentifier' in rr
        ensure:
            result == (rr['Type'] == 'SRV' and rr['SetIdentifier'].startswith('%s:' % self.hostname)
        """
        return rr['Type'] == 'SRV' and 'SetIdentifier' in rr and rr['SetIdentifier'].startswith('%s:' % self.hostname)

    def load(self):
        """
        loads all SRV records from the hosted zone that are managed by this cache.
        """
        self.srv_records = []
        paginator = self.route53.get_paginator('list_resource_record_sets')
        page_iterator = paginator.paginate(HostedZoneId=self.hosted_zone_id)
        for page in page_iterator:
            records = filter(lambda rr: self.is_managed_resource_record_set(rr), page['ResourceRecordSets'])
            self.srv_records.extend(records)

        assert len(filter(lambda r: self.is_managed_resource_record_set(r), self.srv_records)) == len(self.srv_records)
       
    def add(self, service_records):
        """
        Adds or update all 'service_records' in the hosted zone. On succesfull completion, the
        cache is  updated with the new records.
        """
        assert len(filter(lambda r: self.is_managed_resource_record_set(r), service_records)) == len(service_records)

        try:
            batch = {"Changes": [
                {"Action": "UPSERT", "ResourceRecordSet": r} for r in service_records]}
            self.route53.change_resource_record_sets(
                HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
            for record in service_records:
                self._update_cache(record)
        except ClientError as e:
            log.error('could not add SRV records, %s' % (container_id, e))

    def get_all(self):
        result = []
        result.extend(self.srv_records)
        return result

    def remove(self, service_records):
        """
        deletes all the specified service records from the hosted zone. Updates 'self.srv_records' accordingly.
        """
        assert len(filter(lambda r: self.is_managed_resource_record_set(r), service_records)) == len(service_records)

        if len(service_records) > 0:
            try:
                batch = {"Changes": [
                    {"Action": "DELETE", "ResourceRecordSet": rr} for rr in service_records]}
                self.route53.change_resource_record_sets(
                    HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
                set_ids = set(map(lambda r: r['SetIdentifier'], service_records))
                self.srv_records = filter(
                    lambda r: r['SetIdentifier'] not in set_ids, self.srv_records)

            except ClientError as e:
                log.error('could not remove %d SRV records, %s' % (len(records), e))


    def _update_cache(self, service_record):
        """
        updates 'self.srv_records' with the new 'record'. If it did not exist, it is added.
        """
        assert self.is_managed_resource_record_set(service_record)

        for i, rr in enumerate(self.srv_records):
            if rr['SetIdentifier'] == service_record['SetIdentifier'] and rr['Name'] == service_record['Name']:
                self.srv_records[i] = service_record
                return
        self.srv_records.append(service_record)

    def find_service_records_by_set_identifier(self, set_identifier):
        """
        returns all service records in 'self.srv_records' belonging to the specified container.
        """
        assert set_identifier is not None
        return filter(lambda rr: rr['SetIdentifier'] == set_identifier, self.srv_records)

    def get_dns_name_for_hosted_zone_id(self, hosted_zone_id):
        """
        gets the dns name associated with the 'hosted_zone_id'.
        ensure:
            self.hosted_zone_id == hosted_zone_id
            self.dns_name[-1] == '.'
        """
        assert hosted_zone_id is not None
        self.hosted_zone_id = hosted_zone_id
        try:
            response = self.route53.get_hosted_zone(Id=hosted_zone_id)
            self.dns_name = response['HostedZone']['Name']
        except ClientError as e:
            raise click.UsageError('%s' % e)

        log.info('found hosted zone %s for dns name %s' % (self.hosted_zone_id, self.dns_name))

        assert self.dns_name[-1] == '.'
        assert self.hosted_zone_id is not None


    def get_hosted_zone_id_for_dns_name(self, dns_name):
        """
        gets the hosted_zone_id associated with the 'dns_name'. 
        require:
            there is exactly 1 hosted zone for  'dns_name' in Route53.
        ensure:
            self.hosted_zone_id == hosted_zone_id
            self.dns_name[-1] == '.'
            self.dns_name[-1] == dns_name
        """
        self.dns_name = dns_name if dns_name[-1] == '.' else '%s.' % dns_name
        response = self.route53.list_hosted_zones_by_name(DNSName=self.dns_name)
        zones = filter(lambda r: r['Name'] == self.dns_name, response['HostedZones'])
        if len(zones) == 1:
            self.hosted_zone_id = zones[0]['Id'].split('/')[-1]
        elif len(zones) > 1:
            raise click.UsageError('There are %d hosted zones for the DNS name %s, please specify --hosted-zone-id' % (len(zones), self.dns_name))
        else:
            raise click.UsageError('There is no hosted zones for the DNS name %s' % self.dns_name)

        assert self.dns_name[-1] == '.'
        assert self.hosted_zone_id is not None

        log.info('found dns name %s for hosted zone id %s' %
                 (self.dns_name, self.hosted_zone_id))


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

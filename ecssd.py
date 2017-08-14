"""
manages the SRV records for Docker containers running on a specific host.
When a container is start, the registrator will create a SRV record for each
exposed port which has a matching SERVICE_<exposed-port>_NAME environment
variable. If the container exposes a single port, it suffices to have a SERVICE_NAME
environment variable.

The registrator will create SRV records in Route53 with `<hostname>:<container-id>`
as set identifier. This allows the registrator to synchronise SRV records with
the current state of the running instances on the host. 

The registrator has three commands: 'remove_all', 'sync' and 'run'.
    
    remove_all  - remove all service records point to this host
    sync        - synchronise the service records with the running containers
    run         - continuously update the SRV records by subscribing to the Docker event stream

"""
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


class ECSServiceRegistrator:
    """
    manages SRV record registrations in Route53 for docker containers
    """
    def __init__(self, dns_name, hosted_zone_id, hostname):
        """
        constructor. determines the Route53 hosted zone by either the dns_name or the hosted_zone_id.
        requires:
        - hostname is not None
        - valid hosted_zone_id is hosted_zone_id is not None
        - dns_name exists in Route53
        ensures:
        - self.dns_name == dns_name if dns_name is not None and hosted_zone_id is None 
        - self.hosted_zone_id == hosted_zone_id if hosted_zone_id is not None
        - self.hostname == hostname
        """
        assert hostname is not None
        assert hosted_zone_id is not None or dns_name is not None

        self.dockr = docker.from_env()
        self.route53 = boto3.client('route53')

        self.hostname = hostname

        if hosted_zone_id is not None:
            self.get_dns_name_for_hosted_zone_id(hosted_zone_id)
        else:
            self.get_hosted_zone_id_for_dns_name(dns_name)
        
        assert self.hostname is not None
        assert self.dns_name is not None
        assert self.hosted_zone_id is not None


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
            log.warn('found hosted zone id %s for dns name %s' % (self.hosted_zone_id, self.dns_name))
        elif len(zones) > 1:
            raise click.UsageError('There are %d hosted zones for the DNS name %s, please specify --hosted-zone-id' % (len(zones), self.dns_name))
        else:
            raise click.UsageError('There is no hosted zones for the DNS name %s' % self.dns_name)

        assert self.dns_name[-1] == '.'
        assert self.hosted_zone_id is not None

        log.info('found dns name %s for hosted zone id %s' %
                 (self.dns_name, self.hosted_zone_id))

    def get_environment_of_container(self, container):
        """
        require:
            container is not None
            attrs in container 
            'Config' in container.attrs
            'Env' in container.attrs['Config']['Env']
        ensure:
            isinstance(result, dict)
            len(dict) == len(container.attrs['Config']['Env'])
        """
        result = {}
        env = container.attrs['Config']['Env']
        for e in env:
            parameter = e.split('=', 1)
            result[parameter[0]] = parameter[1]
        return result

    def is_managed_resource_record_set(self, rr):
        """
        returns true if the resource record is managed by this hostname.
        require:
            rr is not None
            'Type' in rr
            'SetIdentifier' in rr
        ensure:
            result == (rr['Type'] == 'SRV' and rr['SetIdentifier'].startswith('%s:' % self.hostname)
        """
        return rr['Type'] == 'SRV' and 'SetIdentifier' in rr and rr['SetIdentifier'].startswith('%s:' % self.hostname)

    def load_hosted_zone(self):
        self.srv_records = []
        paginator = self.route53.get_paginator('list_resource_record_sets')
        page_iterator = paginator.paginate(HostedZoneId=self.hosted_zone_id)
        for page in page_iterator:
            self.srv_records.extend(filter(
                lambda rr: self.is_managed_resource_record_set(rr), page['ResourceRecordSets']))

    def create_service_record(self, container_id, port, name):
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

    def register_service(self, service_records):
        try:
            batch = {"Changes": [
                {"Action": "UPSERT", "ResourceRecordSet": r} for r in service_records]}
            self.route53.change_resource_record_sets(
                HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
            for r in service_records:
                self.update_srv_record(r)
        except ClientError as e:
            log.error('could not add SRV records, %s' % (container_id, e))

    def update_srv_record(self, record):
        for i, rr in enumerate(self.srv_records):
            if rr['SetIdentifier'] == record['SetIdentifier'] and rr['Name'] == record['Name']:
                self.srv_records[i] = record
                return
        self.srv_records.append(record)

    def find_srv_records(self, container_id):
        set_id = '%s:%s' % (self.hostname, container_id)
        return filter(lambda rr: rr['SetIdentifier'] == set_id, self.srv_records)

    def deregister_service(self, container_id):
        records = self.find_srv_records(container_id)
        if len(records) > 0:
            log.info('deregistering %d SRV records for container %s' %
                     (len(records), container_id))

            try:
                batch = {"Changes": [
                    {"Action": "DELETE", "ResourceRecordSet": rr} for rr in records]}
                self.route53.change_resource_record_sets(
                    HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
            except ClientError as e:
                log.error('could not remove %d SRV records for container %s, %s' %
                          (len(records), container_id, e))
        else:
            log.debug('No SRV record found for container with id %s' %
                      container_id)

    def create_service_records(self, container):
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
        try:
            container = self.dockr.containers.get(container_id)
            service_records = self.create_service_records(container)
            if len(service_records) > 0:
                log.info('registering %d SRV record for container %s' %
                         (len(service_records), container_id))
                self.register_service(service_records)

        except docker.errors.NotFound as e:
            log.error('container %s does not exist.' % container_id)

    def container_died(self, container_id, event):
        self.deregister_service(container_id)

    def deregister_orphaned_records(self, records):
        if len(records) > 0:
            log.info('deregistering srv records %s' % (
                ' '.join(map(lambda r: r['Name'], records))))

            try:
                batch = {"Changes": [
                    {"Action": "DELETE", "ResourceRecordSet": rr} for rr in records]}
                self.route53.change_resource_record_sets(
                    HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
                set_ids = set(map(lambda r: r['SetIdentifier'], records))
                self.srv_records = filter(
                    lambda r: r['SetIdentifier'] not in set_ids, self.srv_records)

            except ClientError as e:
                log.error('could not remove %d SRV records, %s' % (len(records), e))

    def register_running_containers(self, containers):
        records = []
        for c in containers:
            records.extend(self.create_service_records(c))

        if len(records) > 0:
            log.info('registering srv records %s' % (
                ' '.join(map(lambda r: r['Name'], records))))

            try:
                batch = {"Changes": [
                    {"Action": "UPSERT", "ResourceRecordSet": rr} for rr in records]}
                self.route53.change_resource_record_sets(
                    HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
                self.srv_records.extend(records)

            except ClientError as e:
                log.error('could not add %d SRV records, %s' %
                          (len(records), e))

    def sync(self):
        self.load_hosted_zone()
        containers = self.dockr.containers.list()
        in_zone = set(map(lambda rr: rr['SetIdentifier'], self.srv_records))
        running = set(map(lambda c: '%s:%s' %
                          (self.hostname, c.attrs['Id']), containers))
        to_delete = in_zone - running
        to_add = running - in_zone

        if len(to_delete) > 0 or len(to_add) > 0:
            log.info('zone for host %s out of sync, adding %d and removing %d records' % (
                self.hostname, len(to_add), len(to_delete)))
            records = filter(lambda r: r['SetIdentifier'] in to_delete, self.srv_records)
            self.deregister_orphaned_records(records)

            containers = filter(lambda c: '%s:%s' % (self.hostname, c.attrs['Id']) in to_add, containers)
            self.register_running_containers(containers)
        else:
            log.info('zone for host %s in sync', self.hostname)

    def process_events(self):
        self.load_hosted_zone()
        log.info('found %d SRV records for host %s' %
                 (len(self.srv_records), self.hostname))
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

    def remove_all(self):
        self.load_hosted_zone()
        self.deregister_orphaned_records(self.srv_records)
    
@click.group()
@click.option('--dns-name', required=False, help='to synchronize the SRV records with.')
@click.option('--hosted-zone-id', required=False, help='to synchronize the SRV records with.')
@click.option('--hostname', required=False, default=socket.getfqdn(), help='to use in SRV records.')
@click.pass_context
def cli(ctx, dns_name, hosted_zone_id, hostname):
    if dns_name is None and hosted_zone_id is None:
        click.echo('either --dns-name or --hosted-zone-id has to be spcified.')
        sys.exit(1)
    ctx.obj['dns_name'] = dns_name
    ctx.obj['hosted_zone_id'] = hosted_zone_id
    ctx.obj['hostname'] = hostname


@cli.command()
@click.pass_context
def run(ctx):
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.sync()
    e.process_events()

@cli.command()
@click.pass_context
def remove_all(ctx):
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.remove_all()

@cli.command()
@click.pass_context
def sync(ctx):
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj[
                              'hosted_zone_id'], ctx.obj['hostname'])
    e.sync()

if __name__ == '__main__':
    cli(obj={})

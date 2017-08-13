import sys
import json
import logging
import click
import docker
import boto3
import socket
from botocore.exceptions import ClientError

logging.basicConfig()
log = logging.getLogger('ECSServiceRegistrator')
log.setLevel('INFO')


class ECSServiceRegistrator:

    def __init__(self, dns_name, hosted_zone_id, hostname):
        self.dockr = docker.from_env()
        self.route53 = boto3.client('route53')

        self.hostname = hostname
        self.complete_hosted_zone_info(dns_name, hosted_zone_id)

    def complete_hosted_zone_info(self, dns_name, hosted_zone_id):
        assert dns_name is not None or hosted_zone_id is not None

        if hosted_zone_id is not None:
            self.hosted_zone_id = hosted_zone_id
            try:
                response = self.route53.get_hosted_zone(Id=hosted_zone_id)
                self.dns_name = response['HostedZone']['Name']
                log.info('found dns name %s for hosted zone id %s' %
                         (self.dns_name, self.hosted_zone_id))
            except ClientError as e:
                raise click.UsageError('%s' % e)
        else:
            self.dns_name = dns_name if dns_name[
                -1] == '.' else '%s.' % dns_name
            response = self.route53.list_hosted_zones_by_name(
                DNSName=self.dns_name)
            zones = filter(lambda r: r['Name'] ==
                           self.dns_name, response['HostedZones'])
            if len(zones) == 1:
                self.hosted_zone_id = zones[0]['Id'].split('/')[-1]
                log.warn('found hosted zone id %s for dns name %s' %
                         (self.hosted_zone_id, self.dns_name))
            elif len(zones) > 1:
                raise click.UsageError(
                    'There are %d hosted zones for the DNS name %s, please specify --hosted-zone-id' % (len(zones), self.dns_name))
            else:
                raise click.UsageError(
                    'There is no hosted zones for the DNS name %s' % self.dns_name)

        assert self.dns_name is not None and self.hosted_zone_id is not None

    def get_environment_of_container(self, container):
        result = {}
        env = container.attrs['Config']['Env']
        for e in env:
            parameter = e.split('=', 1)
            result[parameter[0]] = parameter[1]
        return result

    def is_managed_resource_record_set(self, rr):
        return rr['Type'] == 'SRV' and 'SetIdentifier' in rr and rr['SetIdentifier'].startswith('%s:' % self.hostname)

    def load_hosted_zone(self):
        self.srv_records = []
        paginator = self.route53.get_paginator('list_resource_record_sets')
        page_iterator = paginator.paginate(HostedZoneId=self.hosted_zone_id)
        for page in page_iterator:
            self.srv_records.extend(filter(
                lambda rr: self.is_managed_resource_record_set(rr), page['ResourceRecordSets']))
        log.info('found %d SRV records for host %s' %
                 (len(self.srv_records), self.hostname))

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

    def register_service(self, container_id, port, name):
        log.warn('registering port %s of container %s with service name %s' %
                 (port, container_id, name))

        set_id = '%s:%s' % (self.hostname, container_id)
        try:
            record = self.create_service_record(container_id, port, name)
            self.route53.change_resource_record_sets(HostedZoneId=self.hosted_zone_id,
                                                     ChangeBatch={"Changes": [{"Action": "UPSERT", "ResourceRecordSet": record}]})
            r = self.find_srv_records(container_id)
            self.update_srv_record(record)

        except ClientError as e:
            log.error('could not add SRV record for container %s, %s' %
                      (container_id, e))

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
            log.info('deregistering container %s with srv records %s' % (
                container_id, ' '.join(map(lambda r: r['Name'], records))))

            try:
                batch = {"Changes": [
                    {"Action": "DELETE", "ResourceRecordSet": rr} for rr in records]}
                self.route53.change_resource_record_sets(
                    HostedZoneId=self.hosted_zone_id, ChangeBatch=batch)
            except ClientError as e:
                log.error('could not remove %d SRV records for container %s, %s' %
                          (len(records), container_id, e))
        else:
            log.warn('No SRV record found for container with id %s' %
                     container_id)

    def container_started(self, container_id, event):
        try:
            container = self.dockr.containers.get(container_id)
            env = self.get_environment_of_container(container)
            ports = container.attrs['NetworkSettings']['Ports']

            for p in ports:
                if ports[p] is None:
                    log.debug('port %s of container %s is not exposed' %
                              (p, container_id))
                    continue

                port = p.split('/')
                hostPort = ports[p][0]['HostPort']
                service_name = 'SERVICE_%s_NAME' % port[0]
                if service_name in env:
                    self.register_service(
                        container_id, hostPort, env[service_name])
                elif 'SERVICE_NAME' in env and len(ports) == 1:
                    self.register_service(
                        container_id, hostPort, env['SERVICE_NAME'])
                else:
                    log.warn('port %s of container %s is exposed but ignored' % (
                        p, container_id))
        except docker.errors.NotFound as e:
            log.error('container %s does not exist.' % container_id)

    def container_died(self, container_id, event):
        self.deregister_service(container_id)

    def process_events(self):
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

    def main(self):
        self.process_events()


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
    e.load_hosted_zone()
    e.process_events()

if __name__ == '__main__':
    cli(obj={})

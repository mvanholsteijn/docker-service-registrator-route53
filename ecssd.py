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
log.setLevel('WARN')


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
                self.hosted_zone_id = zones[0]['Id']
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

    def register_service(self, container_id, container, port, name):
        log.warn('registering port %s of container %s with service name %s' %
                 (port, container_id, name))

    def container_started(self, container_id, event):
        try:
            container = self.dockr.containers.get(container_id)
            env = self.get_environment_of_container(container)
            ports = container.attrs['NetworkSettings']['Ports']
            for p in ports:
                port = p.split('/')
                service_name = 'SERVICE_%s_NAME' % port[0]
                if service_name in env:
                    self.register_service(container_id, container, port, name)
                elif 'SERVICE_NAME' in env and len(ports) == 1:
                    self.register_service(container_id, container, port, name)
                else:
                    log.warn('port %s of container %s is exposed but ignored' % (
                        p, container_id))
        except docker.errors.NotFound as e:
            log.error('container %s does not exist.' % container_id)

    def container_died(self, id, event):
        pass

    def process_events(self):
        for e in self.dockr.events():
            event = json.loads(e)
            if event['Type'] == 'container':
                if event['status'] == 'start':
                    self.container_started(event['id'], event)
                elif event['status'] == 'die':
                    self.container_died(event['id'], event)
                else:
                    pass  # boring...
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
    e = ECSServiceRegistrator(ctx.obj['dns_name'], ctx.obj['hosted_zone_id'], ctx.obj['hostname'])
    e.process_events()

if __name__ == '__main__':
    cli(obj={})

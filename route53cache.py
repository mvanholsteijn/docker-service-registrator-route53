#!/usr/bin/env python 
import sys
import json
import logging
import click
import docker
import boto3
import socket
from botocore.exceptions import ClientError

log = logging.getLogger('Route53Cache')

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

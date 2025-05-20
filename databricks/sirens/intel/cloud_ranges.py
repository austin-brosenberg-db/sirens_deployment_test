import csv
import re

import requests

from databricks.sirens.config_reader import ThreatIntelReader


def requests_get(url, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30
    return requests.get(url, **kwargs)


def get_googlebot_ranges():
    googlebot = requests_get('https://developers.google.com/search/apis/ipranges/googlebot.json').json()['prefixes']
    return [{
        'provider': 'googlebot',
        'type': 6 if 'ipv6Prefix' in rec else 4,
        'ip_prefix': rec['ipv6Prefix'] if 'ipv6Prefix' in rec else rec['ipv4Prefix'],
        'service': 'web_crawler',
        'region': 'n/a'
    } for rec in googlebot]


def get_google_ranges():
    google = requests_get('https://www.gstatic.com/ipranges/goog.json').json()['prefixes']
    return [{
        'provider': 'google',
        'type': 6 if 'ipv6Prefix' in rec else 4,
        'ip_prefix': rec['ipv6Prefix'] if 'ipv6Prefix' in rec else rec['ipv4Prefix'],
        'service': 'google',
        'region': 'n/a'
    } for rec in google]


def get_gcp_ranges():
    return [{
        'provider': 'gcp',
        'type': 6 if 'ipv6Prefix' in rec else 4,
        'ip_prefix': rec['ipv6Prefix'] if 'ipv6Prefix' in rec else rec['ipv4Prefix'],
        'service': rec['service'],
        'region': rec['scope'],
    } for rec in requests_get('https://www.gstatic.com/ipranges/cloud.json').json()['prefixes']]


def get_aws_ranges():
    aws = []
    for rec in requests_get('https://ip-ranges.amazonaws.com/ip-ranges.json').json()['prefixes']:
        aws.append({
            'type': 6 if ':' in rec['ip_prefix'] else 4,
            'ip_prefix': rec['ip_prefix'],
            'provider': 'amazon',
            'service': rec['service'],
            'region': rec['region'],
        })
    return aws


def get_bingbot_ranges():
    return [{
        'provider': 'bingbot',
        'type': 6 if 'ipv6Prefix' in rec else 4,
        'ip_prefix': rec['ipv6Prefix'] if 'ipv6Prefix' in rec else rec['ipv4Prefix'],
        'service': 'web_crawler',
        'region': 'n/a'
    } for rec in requests_get('https://www.bing.com/toolbox/bingbot.json').json()['prefixes']]


def get_github_ranges():
    github = []
    data = requests_get('https://api.github.com/meta').json()
    for service in ['hooks', 'web', 'api', 'git', 'github_enterprise_importer', 'pages', 'importer', 'actions',
                    'dependabot']:
        if service in data:
            for prefix in data[service]:
                github.append({
                    'ip_prefix': prefix,
                    'provider': 'github',
                    'type': 6 if ':' in prefix else 4,
                    'service': service,
                    'region': 'n/a'
                })
    return github


def get_cloudflare_ranges():
    cloudflare = [
        {
            'ip_prefix': prefix,
            'type': 4,
            'provider': 'cloudflare',
            'region': 'n/a',
            'service': 'CDN'
        } for prefix in requests_get('https://www.cloudflare.com/ips-v4').text.split()]

    cloudflare += [
        {
            'ip_prefix': prefix,
            'type': 6,
            'provider': 'cloudflare',
            'region': 'n/a',
            'service': 'CDN'
        } for prefix in requests_get('https://www.cloudflare.com/ips-v6').text.split()]

    return cloudflare


def get_gptbot_ranges():
    return [
        {
            'ip_prefix': prefix,
            'type': 6 if ':' in prefix else 4,
            'provider': 'gptbot',
            'region': 'n/a',
            'service': 'web_crawler'
        } for prefix in requests_get('https://openai.com/gptbot-ranges.txt').text.split()]


def get_icloud_ranges():
    response = requests_get("https://mask-api.icloud.com/egress-ip-ranges.csv")
    lines = response.text.strip().split('\n')
    ranges = []
    for line in lines:
        tokens = line.split(',')
        ranges.append({
            'ip_prefix': tokens[0],
            'type': 6 if ':' in tokens[0] else 4,
            'provider': 'icloud',
            'region': f"{tokens[2]}-{tokens[3]}",
            'service': 'icloud'
        })
    return ranges


def get_telegram_ranges():
    return [
        {
            'ip_prefix': prefix,
            'type': 6 if ':' in prefix else 4,
            'provider': 'telegram',
            'region': 'n/a',
            'service': 'telegram',
        } for prefix in requests_get('https://core.telegram.org/resources/cidr.txt').text.split()]


def get_digital_ocean_ranges():
    do = requests_get('https://www.digitalocean.com/geo/google.csv').text.split('\n')
    digitalocean = []
    for row in csv.DictReader(do, fieldnames=['prefix', 'cc', 'region', 'city', 'postal_code']):
        digitalocean.append({
            'provider': 'digitalocean',
            'service': 'digitalocean',
            'ip_prefix': row['prefix'],
            'type': 6 if ':' in row['prefix'] else 4,
            'region': row['region']
        })
    return digitalocean


def get_linode_ranges():
    data = [line for line in requests_get('https://geoip.linode.com/').text.split('\n') if not line.startswith('#')]
    linode = []
    for row in csv.DictReader(data, fieldnames=['prefix', 'cc', 'region', 'city', 'postal_code']):
        linode.append({
            'provider': 'linode',
            'service': 'linode',
            'ip_prefix': row['prefix'],
            'type': 6 if ':' in row['prefix'] else 4,
            'region': row['region']
        })
    return linode


def get_oracle_ranges():
    data = requests_get('https://docs.oracle.com/en-us/iaas/tools/public_ip_ranges.json').json()['regions']
    oracle = []
    for rec in data:
        region = rec['region']
        for cidr_rec in rec['cidrs']:
            oracle.append({
                'provider': 'oracle',
                'service': ','.join(cidr_rec['tags']),
                'type': 6 if ':' in cidr_rec['cidr'] else 4,
                'ip_prefix': cidr_rec['cidr'],
                'region': 'n/a',
            })
    return oracle


def get_fastly_ranges():
    data = requests_get('https://api.fastly.com/public-ip-list').json()

    fastly = []

    for prefix in data['addresses']:
        fastly.append({
            'type': 4,
            'ip_prefix': prefix,
            'provider': 'fastly',
            'service': 'CDN',
            'region': 'n/a',
        })
    for prefix in data['ipv6_addresses']:
        fastly.append({
            'type': 6,
            'ip_prefix': prefix,
            'provider': 'fastly',
            'service': 'CDN',
            'region': 'n/a',
        })
    return fastly


def get_microsoft_service_tags(id):
    response = requests_get(f'https://www.microsoft.com/en-us/download/confirmation.aspx?id={id}',
                            headers={'User-Agent': 'curl/8.1.2'})

    if response.status_code == 200:
        content = response.text
        url_pattern = r'<a href=["\'](https://.*?ServiceTags_.*?)["\']'
        match = re.search(url_pattern, content)

        if match:
            url = match.group(1)
            response = requests_get(url, timeout=60)
            if response.status_code == 200:
                return response.json()
            else:
                print(f'Failed to download from URL: {url}')
        else:
            print('URL not found in the page')
    else:
        print(f'Failed to fetch confirmation page for id: {id}')
    return {}


def _parse_microsoft_prefixes(parsed_data):
    results = []
    for value in parsed_data['values']:
        properties = value['properties']
        for prefix in properties['addressPrefixes']:
            service = properties['platform'] + " " + properties['systemService']
            region = properties['region']
            if not region:
                region = 'n/a'
            results.append({
                'provider': 'microsoft',
                'ip_prefix': prefix,
                'type': 6 if ':' in prefix else 4,
                'service': service.strip(),
                'region': region
            })
    return results


def get_microsoft_ranges():
    microsoft = []
    microsoft += _parse_microsoft_prefixes(get_microsoft_service_tags('56519'))  # public
    microsoft += _parse_microsoft_prefixes(get_microsoft_service_tags('57063'))  # US gov
    microsoft += _parse_microsoft_prefixes(get_microsoft_service_tags('57064'))  # germany
    microsoft += _parse_microsoft_prefixes(get_microsoft_service_tags('57062'))  # china
    return microsoft


def safe_fetch(func, name):
    try:
        return func()
    except:
        print(f'Error fetching {name}')
    return []


def internal_ranges():
    return ThreatIntelReader().read_named_file(file_name='internal_ip_ranges')


def get_all_ranges():
    ranges = []
    ranges += safe_fetch(get_googlebot_ranges, 'googlebot')
    ranges += safe_fetch(get_google_ranges, 'google')
    ranges += safe_fetch(get_gcp_ranges, 'gcp')
    ranges += safe_fetch(get_aws_ranges, 'aws')
    ranges += safe_fetch(get_bingbot_ranges, 'bingbot')
    ranges += safe_fetch(get_github_ranges, 'github')
    ranges += safe_fetch(get_cloudflare_ranges, 'cloudflare')
    ranges += safe_fetch(get_gptbot_ranges, 'gptbot')
    ranges += safe_fetch(get_telegram_ranges, 'telegram')
    ranges += safe_fetch(get_digital_ocean_ranges, 'digital_ocean')
    ranges += safe_fetch(get_linode_ranges, 'linode')
    ranges += safe_fetch(get_oracle_ranges, 'oracle')
    ranges += safe_fetch(get_fastly_ranges, 'fastly')
    ranges += safe_fetch(get_microsoft_ranges, 'microsoft')
    ranges += safe_fetch(get_icloud_ranges(), 'icloud')
    ranges += internal_ranges()
    return ranges

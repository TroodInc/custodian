#!/usr/bin/python
import base64
import hashlib
import hmac
import os
import glob
import json
import requests
import sys


def get_service_token():
    domain = os.environ.get('SERVICE_DOMAIN')
    secret = os.environ.get('SERVICE_AUTH_SECRET')

    key = hashlib.sha1(b'trood.signer' + secret.encode('utf-8')).digest()

    signature = hmac.new(key, msg=domain.encode('utf-8'), digestmod=hashlib.sha1).digest()
    signature = base64.urlsafe_b64encode(signature).strip(b'=')

    token = str('%s:%s' % (domain, signature))

    return "Service {}".format(token)


if __name__ == "__main__":
    verbose = False

    if len(sys.argv) > 1:
        if '-v' in sys.argv:
            verbose = True

        fixtures_path = sys.argv[1]


        def apply_fixture(name, data):
            response = requests.put(
                f'http://127.0.0.1:8000/custodian/data/bulk/{name}',
                json=data,
                headers={'Authorization': get_service_token()}
            )
            if response.status_code != 200:
                print(f'Fixture {name} not uploaded.')
                if verbose:
                    print(response.status_code)
                    print(json.dumps(response.json(), indent=4))
                    return

            print(f'Fixture: {name} uploaded.')


        fixture_files = sorted(glob.glob(fixtures_path))

        for f in fixture_files:
            object_name = '_'.join(os.path.basename(f).split('.')[1].split('_')[1:])
            fixture_data = json.load(open(f))
            apply_fixture(object_name, fixture_data)



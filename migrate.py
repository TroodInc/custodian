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

        migrations_path = sys.argv[1]

        SKIPPED = 0
        FAILED = 1
        APPLIED = 2

        def apply_migration(migration_data):
            response = requests.post(
                "http://127.0.0.1:8000/custodian/migrations/apply",
                json=migration_data,
                headers={'Authorization': get_service_token()}
            )

            decoded_response = response.json()
            if verbose:
                print(decoded_response)

            if decoded_response['status'] == "OK":
                return APPLIED
            elif decoded_response['status'] == "FAIL" and decoded_response["error"]["Code"] == "migration_already_applied":
                return SKIPPED
            else:
                return FAILED

        print("Applying migrations...")

        migration_files = glob.glob("{}*.json".format(migrations_path))
        migration_files = sorted(migration_files, key=lambda x: int(list(reversed(x.split('/')))[0].split('_')[0]))

        for f in migration_files:
            migration_name = os.path.basename(f)
            migration_data = json.load(open(f))
            application_status = apply_migration(migration_data)
            if application_status == APPLIED:
                print("Migration {} applied.".format(migration_name))
            elif application_status == SKIPPED:
                print("Migration {} is already applied. Skipping.".format(migration_name))
            else:
                print("Failed to apply migration {} .".format(migration_name))

    else:
        print("Migration path not found, set it as first argument: $> migrate.py /path/to/migrations/")

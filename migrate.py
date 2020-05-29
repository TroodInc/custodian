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
    """
    Returns the service token
    """
    domain = os.environ.get('SERVICE_DOMAIN')
    secret = os.environ.get('SERVICE_AUTH_SECRET')

    key = hashlib.sha1(b'trood.signer' + secret.encode('utf-8')).digest()

    signature = hmac.new(key, msg=domain.encode('utf-8'), digestmod=hashlib.sha1).digest()
    signature = base64.urlsafe_b64encode(signature).strip(b'=')

    token = str('%s:%s' % (domain, signature))

    return "Service {}".format(token)


class CustodianMigrator:
    SKIPPED = 0
    FAILED = 1
    APPLIED = 2

    def __init__(self):
        if len(sys.argv) > 1 and '-v' in sys.argv:
            self.verbose = True
        else:
            self.verbose = False
        self.migrations_path = sys.argv[1]
        self.headers = {'Authorization': get_service_token()}
        self.migration_files = self._get_migration_files()

    def migrate_all(self):
        """
        Apply all migrations
        """
        for f in self.migration_files:
            migration_name = os.path.basename(f)
            migration_data = json.load(open(f))
            application_status = self._migrate(migration_data)
            if application_status == self.APPLIED:
                print("Migration {} applied.".format(migration_name))
            elif application_status == self.SKIPPED:
                print("Migration {} is already applied. Skipping.".format(migration_name))
            else:
                print("Failed to apply migration {} .".format(migration_name))

    def _get_migration_files(self):
        """
        Helper to return the migration files
        """
        migration_files = glob.glob("{}*.json".format(self.migrations_path))
        migration_files = sorted(migration_files, key=lambda x: int(list(reversed(x.split('/')))[0].split('_')[0]))
        return migration_files

    def _migrate(self, migration_data):
        """
         Helper to return a specific migration file
        """
        operations = migration_data['operations']
        if operations[0]['type'] == 'createRecords':
            application_status = self._upload_records(migration_data)
        else:
            application_status = self._apply_migration(migration_data)

        return application_status

    def _apply_migration(self, migration_data):
        """
        Helper to apply a specific migration
        """
        response = requests.post(
            "http://127.0.0.1:8000/custodian/migrations/",
            json=migration_data,
            headers={'Authorization': get_service_token()}
        )

        decoded_response = response.json()
        if self.verbose:
            print(decoded_response)

        if decoded_response['status'] == "OK":
            return self.APPLIED
        elif decoded_response['status'] == "FAIL" and decoded_response["error"]["Code"] == "migration_already_applied":
            return self.SKIPPED
        else:
            return self.FAILED

    def _upload_record(self, record, obj_name):
        url = 'http://127.0.0.1:8000/custodian/data/{}'.format(obj_name)
        response = requests.post(
            url,
            json=record,
            headers={'Authorization': get_service_token()}
            )
        if response.status_code == 200:
            print("Record {} uploaded.".format(record))
        elif response.status_code == 400 and response.json()["error"]["Code"] == "duplicated_value_error":
            skiped = True
            print("Record {} is already uploaded.".format(record))
            return skiped
        else:
            print("Failed to upload record {}.".format(record))
            exit(1)

    def _upload_records(self, migration_data):
        obj_name = migration_data['applyTo']
        records = migration_data['operations'][0]['records']

        for record in records:
            is_skiped = self._upload_record(record, obj_name)
        if is_skiped:
            return self.SKIPPED

        return self.APPLIED


def main():
    migrator = CustodianMigrator()
    if migrator.migrations_path:
        migrator.migrate_all()
    else:
        print("Migration path not found, set it as first argument: $> migrate.py /path/to/migrations/")


if __name__ == "__main__":
    main()

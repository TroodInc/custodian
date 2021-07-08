from locust import HttpUser, task, between, tag
import uuid
import random
import logging


def create_object_migration():
    """
    Returns migration data.
    migration_name == migration_id
    """
    migration_id = "o" + str(uuid.uuid4())[:8]
    return {
        "id": migration_id,
        "applyTo": "",
        "dependsOn": [],
        "operations": [
            {
                "type": "createObject",
                "object": {
                    "name": migration_id,
                    "key": "id",
                    "fields": [
                        {
                            "name": "id",
                            "type": "number",
                            "optional": True,
                            "default": {
                                "func": "nextval"
                            }
                        },
                        {
                            "name": "name",
                            "type": "string",
                            "optional": False
                        }
                    ],
                    "cas": False
                }
            }
        ]
    }


def update_object_migration(obj_name):
    """
    Return migration data.
    """
    migration_id = str(uuid.uuid4())

    return {
        "id": migration_id,
        "applyTo": obj_name,
        "dependsOn": [],
        "operations": [
            {
                "type": "updateField",
                "field": {
                            "previosName": "name",
                            "name": "new_name",
                            "type": "string",
                            "optional": False
                        }
                    }
                ]
            }


def remove_object_migration(obj_name):
    """
    Return migration data.
    """
    migration_id = str(uuid.uuid4())

    return {
        "id": migration_id,
        "applyTo": obj_name,
        "dependsOn": [],
        "operations": [
            {
                "type": "deleteObject"
            }
        ]
    }


class CustodianUser(HttpUser):
    wait_time = between(1, 5)

    def on_start(self):
        self.created = dict()

    @tag('migrations')
    @tag('create_obj')
    @task(1)
    def create_simple_obj(self):
        create_obj_migration_data = create_object_migration()
        self.created[create_obj_migration_data["id"]] = list()
        self.client.post(
            "/custodian/migrations",
            json=create_obj_migration_data,
            name="create_simple_object"
            )

    @tag('migrations')
    @tag('update_obj')
    @task(1)
    def update_simple_obj(self):
        if self.created:
            obj = random.choice(list(self.created))
            self.client.post(
                "/custodian/migrations",
                json=update_object_migration(obj_name=obj),
                name="update_simple_object"
                )

    @tag('migrations')
    @tag('delete_obj')
    @task(1)
    def delete_simple_obj(self):
        if self.created:
            obj = random.choice(list(self.created))
            self.client.post(
                "/custodian/migrations",
                json=remove_object_migration(obj_name=obj),
                name="remove_simple_objects"
                )
            del self.created[obj]

    @tag('list_objects')
    @task(10)
    def list_objets(self):
        self.client.get("/custodian/meta", name='list_objects')

    @tag('records')
    @tag('create_record')
    @task(10)
    def create_record(self):
        if self.created:
            obj = random.choice(list(self.created))
            r = self.client.post(
                f"/custodian/data/{obj}",
                json={"name": "test"},
                name="create_simple_record"
                )

            record = r.json().get('data')["id"]
            self.created[obj].append(record)

    @tag('records')
    @tag('update_record')
    @task(10)
    def update_record(self):
        if self.created:
            obj = random.choice(list(self.created))
            if self.created[obj]:
                record_id = random.choice(self.created[obj])
                self.client.patch(
                    f"/custodian/data/{obj}/{record_id}",
                    json={"name": "updated_test"},
                    name="update_simple_record"
                )

    @tag('records')
    @tag('delete_record')
    @task(10)
    def delete_record(self):
        if self.created:
            obj = random.choice(list(self.created))
            if self.created[obj]:
                record_id = random.choice(self.created[obj])
                self.client.delete(
                    f"/custodian/data/{obj}/{record_id}",
                    name="remove_simple_record"
                    )
                self.created[obj].remove(record_id)

    @tag('records')
    @tag('get_record')
    @task(10)
    def retrieve_record(self):
        if self.created:
            obj = random.choice(list(self.created))
            if self.created[obj]:
                record_id = random.choice(self.created[obj])
                self.client.get(
                    f"/custodian/data/{obj}/{record_id}",
                    name="retieve_simple_record"
                    )

    @tag('records')
    @tag('get_records')
    @task(10)
    def retrieve_records(self):
        if self.created:
            obj = random.choice(list(self.created))
            self.client.get(
                f"/custodian/data/{obj}",
                name="retieve_simple_records"
                )

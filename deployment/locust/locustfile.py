from locust import HttpUser, task, between, tag
import uuid
import random


def create_object_migration():
    """
    Returns migration data.
    migration_name == migration_id
    """
    migration_id = str(uuid.uuid4())[:8]
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


class QuickstartUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        # created_objects is a dict where key is object name
        # and value is crreated ids set
        self.created_objects = dict()
        # Create record for crud_simpler_record test
        create_obj_migration_data = create_object_migration()
        self.crud_simpler_record_obj_name = create_obj_migration_data["id"]
        self.client.post("/custodian/migrations", json=create_obj_migration_data, name="create_simple_object")

    @tag('migrations')
    @task(1)
    def crud_simple_meta_object(self):
        create_obj_migration_data = create_object_migration()
        obj_name = create_obj_migration_data["id"]
        self.client.post("/custodian/migrations", json=create_obj_migration_data, name="create_simple_object")
        self.client.post("/custodian/migrations", json=update_object_migration(obj_name=obj_name), name="update_simple_object")
        self.client.get("/custodian/meta", name="get_meta")
        self.client.post("/custodian/migrations", json=remove_object_migration(obj_name=obj_name), name="remove_simple_objects")

    @tag('crud_simple_record')
    @task(5)
    def crud_simple_record(self):
        record_id = random.randint(1, 10**6)
        self.crud_simpler_record_obj_name
        self.client.post(f"/custodian/data/{self.crud_simpler_record_obj_name}", json={"id": record_id, "name": "test"}, name="create_simple_record")
        self.client.patch(f"/custodian/data/{self.crud_simpler_record_obj_name}/{record_id}", json={"name": "updated_test"}, name="update_simple_record")
        self.client.get(f"/custodian/data/{self.crud_simpler_record_obj_name}/{record_id}", name="update_simple_record")
        self.client.delete(f"/custodian/data/{self.crud_simpler_record_obj_name}/{record_id}", name="remove_simple_record")

    @tag('create_simple_object')
    @task(1)
    def create_simple_object(self):
        """
        Creates simple object.
        saves name to self.created_objects
        """
        create_obj_migration_data = create_object_migration()
        obj_name = create_obj_migration_data["id"]

        # save name of created object for this user
        self.created_objects[obj_name] = set()

        self.client.post("/custodian/migrations", json=create_obj_migration_data, name="create_simple_object")
        self.client.get("/custodian/meta", name="get_meta")

    @tag('write_data_to_obj')
    @task(5)
    def write_data_to_object(self):
        # get one of crated objects name
        if self.created_objects:
            obj_name = random.choice(list(self.created_objects))
            record_id = random.randint(1, 10**6)
            if record_id not in self.created_objects[obj_name]:
                self.client.post(f"/custodian/data/{obj_name}", json={"id": record_id, "name": "test"}, name="create_simple_record")

    @tag('delete_data_from_obj')
    @task(1)
    def delete_simple_record(self):
        # get one of crated objects name
        for obj, ids in self.created_objects.items():
            if len(ids) > 0:
                record_to_delete = ids.pop()
                self.client.delete(f"/custodian/data/{obj}/{record_to_delete}", name="remove_simple_record")

                break

    @tag('update_data_from_obj')
    @task(3)
    def update_simple_record(self):
        # get one of crated objects name
        for obj, ids in self.created_objects.items():
            if len(ids) > 0:
                record_to_update = random.choice(list(ids))
                self.client.patch(f"/custodian/data/{obj}/{record_to_update}", json={"name": "updated_test"}, name="update_simple_record")
                break


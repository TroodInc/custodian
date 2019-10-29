Custodian service
=====================



Quickstart
----------

Create network:

.. code-block:: bash

    docker network create custodian

Download and run postgres:

.. code-block:: bash

    docker pull postgres:10.5
    docker run --rm -d --network custodian \
        --env POSTGRES_DB=custodian \
        --env POSTGRES_USER=custodian \
        --env POSTGRES_PASSWORD=custodian \
        --name=database postgres:10.5

Download and start Custodian service container:

.. code-block:: bash

    docker pull registry.tools.trood.ru/custodian:dev
    docker run --rm -d --network custodian \
        -p 127.0.0.1:8000:8000/tcp \
        --env CONFIGURATION=Development \
        --env DATABASE_URL=pgsql://custodian:custodian@database:5432/custodian?sslmode=disable \
        --name=custodian registry.tools.trood.ru/custodian:dev


Create your test object:

.. code-block:: bash

    curl -X POST 'http://127.0.0.1:8000/meta' \
        -H 'Content-Type: application/json' \
        -d '{
                 "name": "test_object",
                 "key": "id",
                 "fields": [
                    {"name": "id", "type": "number", "optional": true, "unique": false, "default": { "func": "nextval" }},
                    {"name": "name", "type": "string", "optional": false, "unique": false}
                 ]
            }'


Insert first record:

.. code-block:: bash

    curl -X POST 'http://127.0.0.1:8000/data/test_object' \
        -H 'Content-Type: application/json' \
        -d '{"name": "My first record!"}'


Check other API methods on documentation:

.. code-block:: bash

    open http://127.0.0.1:8000/swagger/


Contents
--------

.. toctree::
    :maxdepth: 2
    :glob:

    config
    rest-api

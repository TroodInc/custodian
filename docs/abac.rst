ABAC Configuration
==================

Domain
-------

Custodian ABAC configuration should be defined in domain CUSTODIAN

.. _`ABAC domain API`: /troodcore/auth/rest-api.html#abac-domain

For create CUSTODIAN domain use `ABAC domain API`_

Resource
---------

All user created Meta objects can be used as resource by its name

.. tip::

    You can use wildcard ``*`` for making rules for all objects

.. _`ABAC resource API`: /troodcore/auth/rest-api.html#abac-resource

For create resource use `ABAC resource API`_

Action
-------

Now create action for your resource.

.. _`ABAC action API`: /troodcore/auth/rest-api.html#abac-action

For create action use `ABAC action API`_

Object Records actions:
~~~~~~~~~~~~~~~~~~~~~~~~

.. attribute:: data_GET

    Access for getting both *list* or *single* object.

.. attribute:: data_POST

    Access for adding both *list* or *single* object.

.. attribute:: data_PATCH

    Access for editing both *list* or *single* object.

.. attribute:: data_DELETE

    Access for deleting both *list* or *single* object.

Object Meta actions:
~~~~~~~~~~~~~~~~~~~~~~~~~

.. attribute:: meta_GET

    Access for getting object schema.


.. attribute:: meta_PATCH

    Access for altering object schema.


.. attribute:: meta_DELETE

    Access for deleting object schema.


.. tip::

    You can use wildcard actions for making global rules:

    ``data_*``, ``meta_*``, ``*``


Global Mata actions:
~~~~~~~~~~~~~~~~~~~~~

For resource ``meta`` your can create next actions

.. attribute:: GET

    Access for getting *list* of objects schema.

.. attribute:: POST

    Access for Adding new object schema.

Policy
-------

Now you can create policy with rules

.. _`ABAC policy API`: /troodcore/auth/rest-api.html#abac-policy

For create policy use `ABAC policy API`_

Rules can be configured on next attributes:

Subject attributes
~~~~~~~~~~~~~~~~~~~~~

.. attribute:: sbj.id

    System-wide user ID


.. attribute:: sbj.login

    User login string


.. attribute:: sbj.authorized

    Authorization status, can be ``True`` for authorized or ``False`` for anonymous user


.. attribute:: sbj.role

    User role from TroodAuthorization service


.. attribute:: sbj.profile

    Map with additional user profile fields


Context attributes
~~~~~~~~~~~~~~~~~~~~~

.. attribute:: ctx.data

    Map POST json body


.. attribute:: ctx.params

    List of url path chunks


.. attribute:: ctx.query

    Map of GET query params

Object attributes
~~~~~~~~~~~~~~~~~~~~~

.. attribute:: obj.*

    Object attributes contains all fields of Meta object

    .. important::

        **obj** not exist for *POST actions

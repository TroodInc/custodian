ABAC Configuration
==================


Subject attributes
------------------

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
------------------

.. attribute:: ctx.data

    Map POST json body


.. attribute:: ctx.params

    List of url path chunks


.. attribute:: ctx.query

    Map of GET query params


Resources
----------

All user created Meta objects can be used as resource by its name with listed actions

.. tip::

    You can use wildcard resources for making global rules:

    ``<object name>.data_*``, ``<object name>.meta_*``, ``<object name>.*``, ``*.data_GET``, ``*.meta_GET``, ``*.data_*``, ``*.meta_*``, ``*.*``



Object Records access scope:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. attribute:: <object name>.data_GET

    Access for getting both *list* or *single* object.


.. attribute:: <object name>.data_POST

    Access for adding both *list* or *single* object.


.. attribute:: <object name>.data_PATCH

    Access for editing both *list* or *single* object.


.. attribute:: <object name>.data_DELETE

    Access for deleting both *list* or *single* object.

Object Meta access scope:
~~~~~~~~~~~~~~~~~~~~~~~~~

.. attribute:: <object name>.meta_GET

    Access for getting object schema.


.. attribute:: <object name>.meta_PATCH

    Access for altering object schema.


.. attribute:: <object name>.meta_DELETE

    Access for deleting object schema.


Global Scope:
~~~~~~~~~~~~~

.. attribute:: meta.GET

    Access for getting *list* of objects schema.


.. attribute:: meta.POST

    Access for Adding new object schema.

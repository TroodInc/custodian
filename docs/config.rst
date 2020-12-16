Service configuration
=====================

General settings
----------------

.. envvar:: DATABASE_URL

    Service database connection URL


.. envvar:: AUTHENTICATION_TYPE

    Authentication type can be ``NONE`` or ``TROOD``


.. envvar:: TROOD_AUTH_SERVICE_URL

    TroodAut service url, used for ``TROOD`` AUTHENTICATION_TYPE


.. envvar:: SERVICE_DOMAIN

    Service identification used in TroodCore ecosystem, default ``CUSTODIAN``


.. envvar:: SERVICE_AUTH_SECRET

    Random generated string for system token authentication purposes, ``please keep in secret``


Cache settings
--------------

.. envar:: CACHE_TYPE

    Type of cache used, can be ``REDIS`` or ``NONE``


.. envar:: REDIS_URL

    Redis server used for cache



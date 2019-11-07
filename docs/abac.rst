ABAC Configuration
==================


Subject attributes
------------------

.. object:: sbj

    .. attribute:: id

        System-wide user ID


    .. attribute:: login

        User login string


    .. attribute:: authorized

        Authorization status, can be ``True`` for authorizaed or ``False`` for anonimous user


    .. attribute:: role

        User role from TroodAuthorization service


    .. attribute:: profile

        Map with additional user profile fields


Context attributes
------------------

.. object:: ctx

    .. attribute:: data

        Map POST json body


    .. attribute:: params

        List of url path chunks


    .. attribute:: query

        Map of GET query params


Resources
----------


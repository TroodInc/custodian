ABAC Configuration
==================


Subject attributes
------------------

.. attribute:: sbj.id

    System-wide user ID


.. attribute:: sbj.login

    User login string


.. attribute:: sbj.authorized

    Authorization status, can be ``True`` for authorizaed or ``False`` for anonimous user


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


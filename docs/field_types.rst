Field Types
===========

String
------
Basic string type with UTF-8 support and length up to 1GB total size

Field description example:

.. code-block:: json

    {
      "name": "title",
      "type": "string"
    }


Number
------
Numeric type with decimal point.

Field description example:

.. code-block:: json

    {
      "name": "price",
      "type": "number"
    }


Bool
----
Basic boolean type

Field description example:

.. code-block:: json

    {
      "name": "active",
      "type": "bool"
    }


Enum
----
An enum is a string object with a value chosen from a list of permitted values

Field description example:

.. code-block:: json

    {
        "name": "status",
        "type": "enum",
        "choices": ["ACTIVE", "CLOSED"]
    }


Date
----
Date string in YYYY-MM-DD format.

Field description example:

.. code-block:: json

    {
        "name": "birthday",
        "type": "date"
    }



Time
----
Time string in hh:mm:ss format.

Field description example:

.. code-block:: json

    {
        "name": "daily_meeting",
        "type": "time"
    }



Datetime
--------
Date time string with timezone  YYYY-MM-DDThh:mm:ssTZD

Field description example:

.. code-block:: json

    {
        "name": "created",
        "type": "datetime"
    }


Object
------


Generic
-------


Array
-----


Objects
------


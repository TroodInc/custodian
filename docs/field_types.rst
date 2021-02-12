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

Updating enum fields
""""""""""""""""""""
To add choices, you need to send all existing choices + new ones

It is not possible to delete existing choices.
Reducing the number of choices is possible only by changing the type of the enum field to string and back.
The user is responsible for the consistency of the data in this action.

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
Denotes a "one-to-many" relation.

When using any type with some sort of relation you should provide following fields.
"linkMeta" or "linkMetaList" are used to denote the name of the object with which this object is related.
"linkType" is used to denote the type of relation. It can be either "inner" (refers to the internal object, on which this object depends) or "outer" (refers to an external, dependent object).
And if your "linkType" is "outer", you should provide "outerLinkField" field which is used as an attribute of an external object that contains the identifiers of that object.

Field description example:

.. code-block:: json

    {
        "name": "person",
        "type": "object",
        "optional": false,
        "linkMeta": "person",
        "linkType": "inner",
        "onDelete": "cascade"
    }


Generic
-------
Denotes a "one to many" relation, indicating many types of objects with which a relation is established.

Field description example:

.. code-block:: json

    {
        "name": "person",
        "type": "generic",
        "linkType": "inner",
        "optional": false,
        "linkMetaList": ["employee", "client"],
    }


Array
-----
Denotes a "many-to-one" relation.

Field description example:

.. code-block:: json

    {
        "name": "addresses",
        "type": "array",
        "optional": true,
        "linkMeta": "address",
        "outerLinkField": "person",
        "linkType": "outer"
    }

Objects
------
Denotes a "many-to-many" relation.

Field description example:

.. code-block:: json

    {
        "name": "managers",
        "type": "objects",
        "optional": true,
        "linkMeta": "address",
        "linkType": "inner"
    }

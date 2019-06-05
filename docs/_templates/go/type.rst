{{ obj.name }}
{{ "~" * obj.name|length }}

{{ obj.docstring }}

{%- for child in obj.children|sort %}
    {{ child.render() }}
{%- endfor %}


{% for method in obj.methods|sort %}

.. method:: {{obj.name}}.{{ method.name }}
    {%- set argjoin = joiner(', ') -%}
    ({%- for param in method.parameters -%}
        {{ argjoin() }}{{ param.name }} {{ param.type }}
    {%- endfor -%})

{{ method.doc }}
{%- endfor %}


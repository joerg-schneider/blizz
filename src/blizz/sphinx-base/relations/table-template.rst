==========
{name}
==========

{description}

Primary Key
-----------

{primary_key_section}


Schema
------


.. list-table::
    :widths: 10 10 10 50
    :header-rows: 1
    :stub-columns: 1

    * - Field
      - Type
      - Default
      - Description
    {fields_table_rows}

Origin
------

.. code-block:: python
   :linenos:
   :caption: Ingestion code for {name}, as defined in {name}.load

   {load_code}
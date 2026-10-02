.. NOTE FOR REVIEWERS OF THIS REPOSITORY
..
.. This is not a standalone document. It holds the sections to be merged into
.. cinder's own
.. ``doc/source/configuration/block-storage/drivers/hitachi-vsp-driver.rst``
.. when the group-replication work is proposed upstream, kept at the same path
.. here so the move is a copy rather than a rewrite.
..
.. Configuration options are rendered by ``.. config-table::``; extra specs are
.. not, so the section below is written by hand.

Group replication
-----------------

A Cinder generic volume group whose group type sets
``consistent_group_replication_enabled`` (or ``group_replication_enabled``) to
``<is> True`` is replicated as one Universal Replicator copy group, giving the
group's volumes a single consistency point. ``enable_replication``,
``disable_replication`` and ``failover_replication`` act on the copy group as
a whole, so a failover preserves write ordering across every member.

Pairing at create time, or at group join
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A replicated volume is normally paired as it is created. A volume destined for
a replicated group must not be, because the driver pins one mirror unit
(``hitachi_replication_mun``) and the storage system rejects a second pair at
that mirror unit: the volume would be paired twice, and the group join would
fail.

The driver tells the two apart from the volume's group when it has one, and
otherwise from the volume type. A volume created before its group exists has
no ``group_id``, and so no group type to consult -- which is the usual order,
since a group is normally formed from volumes that already exist.

Cinder's group replication API requires every volume type in a replicated
group to set ``replication_enabled='<is> True'``. On the volume type used for
volumes that will join a replicated group, set ``group_replication_enabled``
as well:

.. code-block:: console

   $ openstack volume type set \
     --property replication_enabled='<is> True' \
     --property group_replication_enabled='<is> True' \
     <volume type name>

A volume of that type is created unpaired and reports
``replication_status=disabled`` until ``enable_replication`` places it in the
group's copy group. A volume of a plain ``replication_enabled`` type on the
same backend is unaffected and is still paired at create time, so one backend
serves both.

``group_replication_enabled`` is unscoped, so the scheduler's
``CapabilitiesFilter`` matches it against the pool capability of the same name.
The driver reports that capability only on a backend configured for
replication. A volume of this type can therefore only be placed on a backend
that can do group replication; on any other backend the create fails with
``No valid backend``.

.. note::

   A volume of a type with this extra spec is never paired at create time, nor
   is one created directly into a replicated group. Any other replicated
   volume pairs at create time and cannot afterwards join a copy group.

Allowed values
^^^^^^^^^^^^^^

The extra spec must be exactly ``<is> True``, or absent.

.. list-table::
   :header-rows: 1

   * - Value
     - Scheduler
     - Driver
     - Result
   * - ``<is> True``
     - matches
     - group-replicated
     - Works
   * - absent
     - not evaluated
     - not group-replicated
     - Paired at create time (intended)
   * - ``True``, ``true``
     - no match (compared as a plain string)
     - not group-replicated
     - Unschedulable: ``No valid backend``
   * - ``<is> False``, ``<is> false``
     - requires a backend reporting ``False``; none does
     - not group-replicated
     - Unschedulable on every backend
   * - ``<is> true``
     - matches
     - **not** group-replicated
     - Scheduled, but paired at create time, so it can never join a copy group

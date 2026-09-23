.. NOTE FOR REVIEWERS OF THIS REPOSITORY
..
.. This is not a standalone document. It holds the sections to be merged into
.. cinder's own
.. ``doc/source/configuration/block-storage/drivers/hitachi-vsp-driver.rst``
.. when the group-replication work is proposed upstream, kept at the same path
.. here so the move is a copy rather than a rewrite.
..
.. The two new configuration options
.. (``hitachi_replication_report_pair_status`` and its ``_ttl``) need no text
.. here: that page renders options with ``.. config-table::`` over
.. ``cinder.volume.drivers.hitachi.hbsd_replication``, so their ``help``
.. strings in ``COMMON_REPLICATION_OPTS`` are the documentation. Extra specs
.. are not generated, so the section below is written by hand.

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

The driver can only tell the two apart from the volume type. A volume created
before its group exists has no ``group_id``, and so no group type to consult --
which is the usual order, since a group is normally formed from volumes that
already exist.

Set the following extra spec on the volume type used for volumes that will
join a replicated group:

.. code-block:: console

   $ openstack volume type set --property group_replication_enabled='<is> True' \
     <volume type name>

A volume of that type is created unpaired and reports
``replication_status=disabled`` until ``enable_replication`` places it in the
group's copy group. A volume of a plain ``replication_enabled`` type on the
same backend is unaffected and is still paired at create time, so one backend
serves both.

The key is unscoped, so the scheduler's ``CapabilitiesFilter`` matches it
against the ``group_replication_enabled`` pool capability. The driver reports
that capability only on a backend configured for replication. A volume of this
type can therefore only be placed on a backend that can do group replication;
on any other backend the create fails with ``No valid backend``.

.. note::

   The extra spec is the only way to stop a volume pairing at create time.
   Without it, a replicated volume pairs at create time and cannot afterwards
   join a copy group.

Allowed values
^^^^^^^^^^^^^^

The extra spec must be exactly ``<is> True``, or absent. The scheduler and the
driver both read the value, and they do not parse it the same way. The
scheduler compares ``<is>`` values with ``strutils.bool_from_string``. The
driver accepts only the literal ``<is> True``, after trimming surrounding
whitespace, which is the rule ``volume_utils.is_group_a_type`` applies to group
types.

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

One name, three meanings
^^^^^^^^^^^^^^^^^^^^^^^^

``group_replication_enabled`` names three different things in this driver.
They do not collide, because each lives on a different object:

.. list-table::
   :header-rows: 1

   * - Object
     - Set by
     - Read by
     - Means
   * - Pool capability
     - The driver, in ``update_volume_stats``
     - The scheduler's ``CapabilitiesFilter``
     - This backend can do group replication
   * - Group type ``group_specs``
     - Operator
     - ``Group.is_replicated``
     - This Cinder group is replicated
   * - Volume type ``extra_specs``
     - Operator
     - ``CapabilitiesFilter``, and the driver at volume creation
     - This volume is paired by ``enable_replication``, not at create time

The pool capability and the volume type spec share a name on purpose: that
pairing is what ``CapabilitiesFilter`` enforces. The group spec is Cinder's own
standard key, the sibling of ``consistent_group_replication_enabled``.

Reporting pair state in the pool capabilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With ``hitachi_replication_report_pair_status = True`` the driver publishes
each copy group's pair state as the ``group_replication_pairs`` pool
capability, letting a consumer read pair state from
``GET /scheduler-stats/get_pools?detail=True`` rather than calling the storage
system. It is off by default because building the report costs one REST call
per copy group on every stats poll, against the same Configuration Manager
endpoint that serves pair creation.

``group_replication_pairs_updated_at`` stamps the report, and
``group_replication_pairs_enumerated`` is ``False`` when the driver could not
ask the storage system for the full set -- a consumer that reads this
capability must treat that as "unknown" and fall back, never as "nothing
replicating".

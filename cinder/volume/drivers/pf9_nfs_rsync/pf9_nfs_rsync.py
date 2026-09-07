"""NFS-backed Cinder driver with cross-site group replication.

Adds the standard Cinder group-replication and volume-manage API to an NFS
backend so a disaster-recovery workflow (enable replication, fail over to the
secondary site, adopt the replicated volume, boot from it, fail back) works over
two NFS exports. A background thread replicates the primary export to the
secondary over SSH.

Prototype / lab / CI use, not a supported production driver.

Layout on each site's NFS export:
    <export>/volume-<id>                    volume file
    <export>/cg-<group-id>/                 consistency-group marker + pair table
    <export>/cg-<group-id>/.last_sync.json  last replication time (lag)
"""

from __future__ import annotations

import errno
import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Iterator

from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder import interface
from cinder.volume.drivers import nfs
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)

pf9_replication_opts = [
    cfg.IntOpt("pf9_replication_interval", default=30,
               help="Seconds between replication cycles."),
    cfg.IntOpt("pf9_replication_keep_gens", default=3,
               help="Number of point-in-time copies to keep on the secondary."),
    cfg.StrOpt("pf9_replication_peer", default=None,
               help="SSH target of the secondary NFS server, e.g. "
                    "'user@10.0.0.2'. Leave unset for a single-site deployment."),
    cfg.StrOpt("pf9_replication_peer_incoming", default=None,
               help="Directory on the secondary that receives replicated data, "
                    "e.g. '/export/psr/incoming'."),
    cfg.StrOpt("pf9_replication_ssh_user", default=None,
               help="Deprecated alias; prefer user@host in pf9_replication_peer."),
]
CONF = cfg.CONF
CONF.register_opts(pf9_replication_opts)

REPLICATION_STATE_ENABLED = "enabled"
REPLICATION_STATE_FAILED_OVER = "failed-over"

PAIR_SMPL = "SMPL"
PAIR_COPY = "COPY"
PAIR_PAIR = "PAIR"
PAIR_PSUS = "PSUS"
PAIR_SSWS = "SSWS"

COPYGROUP_FILE = ".copygroup.json"
PSR_META_DIR = ".psr-meta"
LUN_ALLOC_FILE = "lun_alloc.json"
PROMOTED_FILE = "promoted"
PVOL_BASE = 1001
SVOL_BASE = 2001

# Volume-metadata keys carrying the pair's identifiers to PSR.
#
# WHY METADATA. The driver allocates both ids in enable_replication but writes
# them only to its own copygroup JSON, which Cinder never reads — the volume
# model returns replication_status alone, so nothing reaches Cinder. Neither
# provider_location nor replication_driver_data helps: those are internal DB
# columns the volume REST API does not return. `metadata` is the one per-volume,
# driver-writable field exposed on an ordinary volume list.
#
# WHY PSR NEEDS THEM. At failover the recovery site adopts the replica with
# manage_existing(source-name=<S-VOL id>). That id must be known BEFORE the
# primary site is lost, so PSR reads it here, records it on the DiscoveredVolume
# and syncs it to the peer ahead of any disaster.
#
# These names match the pf9_hitachi driver's _MD_* keys deliberately, so both
# backends look identical to PSR and its discovery needs no per-vendor branch.
PSR_PVOL_META_KEY = "psr_pvol_id"        # this site's P-VOL id
PSR_SVOL_META_KEY = "psr_svol_id"        # the peer's S-VOL id
PSR_COPY_GROUP_META_KEY = "psr_copy_group"  # the copy group the pair belongs to


def _psr_metadata_model_update(volume, **kwargs) -> dict:
    """Return a {'metadata': ...} model update fragment, or {}.

    Cinder REPLACES a volume's metadata with what a driver returns rather than
    merging into it, so the whole map has to be sent every time — hence the
    merge onto the volume's current metadata below.

    Which is also why this emits nothing at all when that current metadata
    cannot be read: reading it can go to the database if the attribute was
    never loaded, and sending only our own keys would silently drop whatever
    the volume's owner had set. Nothing here is worth failing a replication
    operation over.

    A None value removes that key — how disable_replication retires the ids.
    """
    try:
        merged = dict(volume.metadata or {})
    except Exception:
        LOG.debug("PF9NFSRsync: not annotating volume %s: its current metadata "
                  "could not be read, and a partial map would discard the "
                  "metadata already on it.", volume.id, exc_info=True)
        return {}
    for key, value in kwargs.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = str(value)
    return {"metadata": merged}

SSH_OPTS = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"]


@interface.volumedriver
class PF9NFSRsyncDriver(nfs.NfsDriver):
    """NFS driver + group-replication contract, backed by rsync.

    Advertises replication so a replicated volume-type / group-type binds here.
    """

    VERSION = "0.1.0-prototype"
    CI_WIKI_NAME = "Platform9_PSR_NFS_Rsync_prototype"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.configuration.append_config_values(pf9_replication_opts)
        self._repl_stop = threading.Event()
        self._repl_thread = None

    def do_setup(self, context) -> None:
        super().do_setup(context)
        self._start_replication_thread()

    def _update_volume_stats(self) -> None:
        super()._update_volume_stats()
        backend = self._secondary_backend_id()
        pools = self._stats.get("pools")
        targets = [pool for pool in (pools or [])]
        for pool in targets:
            pool["replication_enabled"] = True
            pool["replication_type"] = ["async"]
            pool["replication_targets"] = [backend]
        self._stats["replication_enabled"] = True
        self._stats["replication_type"] = ["async"]
        self._stats["replication_targets"] = [backend]

    def _start_replication_thread(self) -> None:
        peer = self.configuration.safe_get("pf9_replication_peer")
        if not peer:
            LOG.info("PF9NFSRsync: pf9_replication_peer unset; replication "
                     "thread idle (single-site / CI).")
            return
        if self._repl_thread and self._repl_thread.is_alive():
            return
        self._repl_stop.clear()
        self._repl_thread = threading.Thread(
            target=self._replication_worker, name="pf9-nfs-repl", daemon=True)
        self._repl_thread.start()
        LOG.info("PF9NFSRsync: replication thread started -> %s every %ss "
                 "(keep %s gens)", peer,
                 self.configuration.safe_get("pf9_replication_interval"),
                 self.configuration.safe_get("pf9_replication_keep_gens"))

    def _replication_worker(self) -> None:
        interval = self.configuration.safe_get("pf9_replication_interval")
        while not self._repl_stop.is_set():
            try:
                self._replicate_once()
            except subprocess.CalledProcessError as e:
                LOG.warning("PF9NFSRsync: replication cycle failed rc=%s: %s",
                            e.returncode,
                            (e.stderr or b"").decode(errors="replace")[:300])
            except Exception as e:
                # Deliberately broad: this is the background thread's top-level
                # guard. A bug in one cycle must be logged, not kill the loop.
                LOG.warning("PF9NFSRsync: replication cycle error: %s", e)
            self._repl_stop.wait(interval)

    def _replicate_once(self) -> None:
        if not self._any_cg_active():
            return
        peer = self.configuration.safe_get("pf9_replication_peer")
        export = self._export().rstrip("/")
        incoming = (self.configuration.safe_get("pf9_replication_peer_incoming")
                    or (export + "/incoming"))
        if self._peer_promoted(peer, incoming):
            LOG.info("PF9NFSRsync: peer %s is promoted; replication paused.", peer)
            return
        keep = self.configuration.safe_get("pf9_replication_keep_gens")

        gen_n = self._next_generation()
        gen = "gen-%d" % gen_n
        start = time.time()

        self._run(SSH_OPTS + [peer, "mkdir -p '%s' && chmod 0777 '%s'" % (incoming, incoming)])
        self._run(["rsync", "-az", "--delete",
                   "-e", " ".join(SSH_OPTS),  # rsync's ssh needs the same opts
                   "--exclude", PSR_META_DIR + "/",
                   "--exclude", "lost+found/",
                   "--exclude", "incoming/",
                   export + "/", "%s:%s/%s/" % (peer, incoming, gen)])
        elapsed = int(time.time() - start)
        # point 'current' at the new generation, then keep the newest N. Prune by
        # generation NUMBER (sort -rn), not mtime: rsync -a copies the source
        # mtime onto every generation, so mtime can't tell them apart. Never
        # delete whatever 'current' points at.
        self._run(SSH_OPTS + [peer,
                  "set -e; cd '%s'; "
                  "ln -sfn '%s' current.tmp && mv -Tf current.tmp current; "
                  "cur=$(readlink current 2>/dev/null); "
                  "ls -1d gen-* 2>/dev/null | sed 's/^gen-//' | sort -rn "
                  "| tail -n +%d | sed 's/^/gen-/' | grep -vx \"$cur\" "
                  "| xargs -r rm -rf" % (incoming, gen, keep + 1)])
        now = int(time.time())
        for cgdir in self._iter_cg_dirs():
            cgn = Path(cgdir).name[len("cg-"):]
            try:
                with (Path(cgdir) / ".last_sync.json").open("w") as f:
                    json.dump({"cg": cgn, "generation": gen_n,
                               "replicated_at": now, "elapsed": elapsed}, f)
            except OSError as e:
                LOG.warning("PF9NFSRsync: could not write .last_sync.json for "
                            "%s (RPO reporting will be stale): %s", cgn, e)
        LOG.info("PF9NFSRsync: shipped %s (consistency_time=%s, %ss)",
                 gen, now, elapsed)

    def _any_cg_active(self) -> bool:
        """True if any group is PAIR/COPY and not reversed. Failed-over (SSWS),
        disabled (SMPL), and reversed groups return False so replication stops.
        """
        for cgdir in self._iter_cg_dirs():
            try:
                with (Path(cgdir) / COPYGROUP_FILE).open() as f:
                    cg = json.load(f)
            except (OSError, ValueError) as e:
                LOG.debug("PF9NFSRsync: skipping %s, unreadable copygroup: %s",
                          cgdir, e)
                continue
            if cg.get("direction") == "reversed":
                continue
            for p in cg.get("pairs", []):
                if p.get("state") in (PAIR_PAIR, PAIR_COPY):
                    return True
        return False

    def _iter_cg_dirs(self) -> Iterator[str]:
        try:
            names = sorted(p.name for p in Path(self._export()).iterdir())
        except OSError as e:
            LOG.debug("PF9NFSRsync: cannot list export %s: %s",
                      self._export(), e)
            return
        for name in names:
            if name.startswith("cg-"):
                yield str(Path(self._export()) / name)

    def _next_generation(self) -> int:
        path = str(Path(self._meta_dir()) / "generation")
        try:
            with Path(path).open() as f:
                g = int((f.read().strip() or "0"))
        except (OSError, ValueError) as e:
            LOG.debug("PF9NFSRsync: no prior generation counter at %s "
                      "(starting at 0): %s", path, e)
            g = 0
        g += 1
        try:
            with Path(path).open("w") as f:
                f.write(str(g))
        except OSError as e:
            LOG.warning("PF9NFSRsync: could not persist generation counter "
                        "to %s: %s", path, e)
        return g

    def _peer_marker_path(self, incoming: str) -> str:
        peer_export = os.path.dirname(incoming.rstrip("/"))
        return os.path.join(peer_export, PSR_META_DIR, PROMOTED_FILE)

    def _peer_promoted(self, peer: str, incoming: str) -> bool:
        # `test -f` exits 1 (not an error) when the marker is absent, so this
        # call must not raise on a non-zero exit -> check=False.
        marker = self._peer_marker_path(incoming)
        try:
            r = self._run(SSH_OPTS + [peer, "test -f '%s'" % marker],
                          check=False, timeout=30)
            return r.returncode == 0
        except (subprocess.SubprocessError, OSError) as e:
            LOG.warning("PF9NFSRsync: could not check promoted marker on %s: %s",
                        peer, e)
            return False

    def _clear_peer_promoted(self, peer: str, incoming: str) -> None:
        """Best-effort: clear the peer's promoted marker so replication can resume
        (used on failback). No-op if the peer is unreachable."""
        marker = self._peer_marker_path(incoming)
        try:
            self._run(SSH_OPTS + [peer, "rm -f '%s'" % marker])
        except (subprocess.SubprocessError, OSError) as e:
            LOG.warning("PF9NFSRsync: could not clear peer promoted marker: %s", e)

    def _run(self, argv: list, check: bool = True, timeout: int = 600) -> subprocess.CompletedProcess:
        return subprocess.run(argv, check=check, capture_output=True,
                              timeout=timeout)

    def _type_is_replicated(self, volume) -> bool:
        # Cinder requires a replication-capable volume to report a
        # replication_status before it will enable replication on it.
        try:
            specs = (volume.volume_type.extra_specs or {})
        except AttributeError as e:
            LOG.debug("PF9NFSRsync: volume %s has no volume_type extra_specs: %s",
                      getattr(volume, "id", "?"), e)
            return False
        val = str(specs.get("replication_enabled", "")).lower()
        return "true" in val

    def create_volume(self, volume) -> dict:
        """Create a volume; seed replication_status for replication-capable types."""
        model = super().create_volume(volume) or {}
        if self._type_is_replicated(volume):
            model["replication_status"] = "disabled"
        return model

    def create_group(self, context, group) -> dict:
        """Create a consistency group."""
        path = self._cg_dir(group.id)
        Path(path).mkdir(parents=True, exist_ok=True)
        LOG.info("PF9NFSRsync: created group dir %s", path)
        return {"status": "available"}

    def delete_group(self, context, group, volumes) -> tuple:
        """Delete a consistency group and its member volume files."""
        for v in (volumes or []):
            try:
                Path(self._vol_path(v.id)).unlink()
            except FileNotFoundError as e:
                LOG.debug("PF9NFSRsync: volume file for %s already gone: %s",
                          v.id, e)
        shutil.rmtree(self._cg_dir(group.id), ignore_errors=True)
        model = {"status": "deleted"}
        return model, [{"id": v.id, "status": "deleted"} for v in (volumes or [])]

    def update_group(self, context, group, add_volumes=None, remove_volumes=None) -> tuple:
        """Add or remove member volumes of a consistency group."""
        for v in add_volumes or []:
            self._link_into_cg(group.id, v.id)
        for v in remove_volumes or []:
            self._unlink_from_cg(group.id, v.id)
        return {"status": "available"}, None, None

    def create_group_snapshot(self, context, group_snapshot, snapshots) -> tuple:
        """Take a crash-consistent snapshot of a group's member volumes."""
        snap_dir = self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id
        Path(snap_dir).mkdir(parents=True, exist_ok=True)
        for s in snapshots:
            self._copy(self._vol_path(s.volume_id), str(Path(snap_dir) / s.volume_id))
        return {"status": "available"}, [{"id": s.id, "status": "available"} for s in snapshots]

    def delete_group_snapshot(self, context, group_snapshot, snapshots) -> tuple:
        """Delete a group snapshot."""
        shutil.rmtree(self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id, ignore_errors=True)
        return {"status": "deleted"}, [{"id": s.id, "status": "deleted"} for s in snapshots]

    def create_group_from_src(self, context, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None) -> tuple:
        """Create a group and its member volumes from a group snapshot or a
        source group, copying each source file into the new volume file.
        """
        Path(self._cg_dir(group.id)).mkdir(parents=True, exist_ok=True)
        pairs = []
        if group_snapshot and snapshots:
            snap_dir = self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id
            snap_by_id = {s.id: s for s in snapshots}
            for v in volumes:
                s = snap_by_id.get(v.snapshot_id)
                if s is None:
                    raise exception.InvalidInput(
                        reason="create_group_from_src: no source snapshot for volume "
                               "%s (snapshot_id=%s)" % (v.id, v.snapshot_id))
                pairs.append((str(Path(snap_dir) / s.volume_id), v))
        elif source_group and source_vols:
            src_by_id = {sv.id: sv for sv in source_vols}
            for v in volumes:
                sv = src_by_id.get(v.source_volid)
                if sv is None:
                    raise exception.InvalidInput(
                        reason="create_group_from_src: no source volume for volume "
                               "%s (source_volid=%s)" % (v.id, v.source_volid))
                pairs.append((self._vol_path(sv.id), v))
        for src, v in pairs:
            self._copy(src, self._vol_path(v.id))
            self._link_into_cg(group.id, v.id)
        vol_models = [{"id": v.id, "status": "available"} for v in volumes]
        return {"status": "available"}, vol_models

    def enable_replication(self, context, group, volumes) -> tuple:
        """Start replicating a group: create a pair per member volume with a
        distinct secondary LUN id and set the pair state to PAIR.
        """
        cg = self._read_copygroup(group.id)
        by_uuid = {p["cinderUuid"]: p for p in cg["pairs"]}
        # Keep each volume's pair so the ids can be stamped onto the volume
        # below. Re-enabling an already-paired group re-stamps rather than
        # re-allocating, so the operation stays idempotent.
        pair_by_uuid = {}
        for v in (volumes or []):
            p = by_uuid.get(v.id)
            if p is None:
                pvol, svol = self._alloc_pair_ids()
                p = {"pvolId": pvol, "svolId": svol,
                     "cinderUuid": v.id, "state": PAIR_PAIR}
                cg["pairs"].append(p)
            else:
                p["state"] = PAIR_PAIR
            pair_by_uuid[v.id] = p
        cg["direction"] = "P_to_S"
        self._write_copygroup(group.id, cg)
        model = {"replication_status": "enabled"}
        copy_group = cg.get("copyGroup") or group.id
        vol_models = []
        for v in (volumes or []):
            update = {"id": v.id, "replication_status": "enabled"}
            pair = pair_by_uuid.get(v.id)
            if pair:
                # Hand the pair's identifiers to PSR. Only on the success path:
                # the copygroup write above has already committed, so a pair
                # advertised here really exists. Publishing ids for a pair that
                # was never created would make a volume look recoverable when it
                # is not — which surfaces at failover, the worst possible time.
                update.update(_psr_metadata_model_update(
                    v, **{PSR_PVOL_META_KEY: pair["pvolId"],
                          PSR_SVOL_META_KEY: pair["svolId"],
                          PSR_COPY_GROUP_META_KEY: copy_group}))
            vol_models.append(update)
        return model, vol_models

    def disable_replication(self, context, group, volumes) -> tuple:
        """Stop replicating a group (pair state -> SMPL)."""
        cg = self._read_copygroup(group.id)
        for p in cg["pairs"]:
            p["state"] = PAIR_SMPL
        cg["direction"] = "none"
        self._write_copygroup(group.id, cg)
        vol_models = []
        for v in (volumes or []):
            update = {"id": v.id, "replication_status": "disabled"}
            # Retire the ids along with the pair. A volume that has stopped
            # replicating still carrying a psr_svol_id would advertise a replica
            # that no longer exists, and PSR would attempt to adopt it at
            # failover — a stale id is worse than an absent one, because absent
            # is reported as "cannot be recovered" while stale fails mid-import.
            update.update(_psr_metadata_model_update(
                v, **{PSR_PVOL_META_KEY: None,
                      PSR_SVOL_META_KEY: None,
                      PSR_COPY_GROUP_META_KEY: None}))
            vol_models.append(update)
        return {"replication_status": "disabled"}, vol_models

    def failover_replication(self, context, group, volumes, secondary_backend_id=None) -> tuple:
        """Fail a group over, or fail it back.

        Failover (secondary_backend_id is the secondary or unset): promote the
        secondary. Pair state -> SSWS, direction reversed, replication_status
        'failed-over'. The data is already on the secondary and is adopted there
        with manage_existing.

        Failback (secondary_backend_id == 'default', Cinder's sentinel for
        "resume the primary"): re-establish primary -> secondary. Pair state ->
        PAIR, replication_status 'enabled'. This is the only transition Cinder
        allows out of the failed-over state.

        Neither direction touches the psr_* metadata, deliberately: the pair
        still exists through a failover, only its direction changed, and the
        same two LUN ids are what a subsequent failback needs. They are retired
        in disable_replication, where the pair is actually torn down.
        """
        cg = self._read_copygroup(group.id)
        if secondary_backend_id == "default":
            for p in cg["pairs"]:
                p["state"] = PAIR_PAIR
            cg["direction"] = "P_to_S"
            cg["target"] = "default"
            self._write_copygroup(group.id, cg)
            peer = self.configuration.safe_get("pf9_replication_peer")
            if peer:
                incoming = (self.configuration.safe_get(
                    "pf9_replication_peer_incoming")
                    or (self._export().rstrip("/") + "/incoming"))
                self._clear_peer_promoted(peer, incoming)
            LOG.info("PF9NFSRsync: failed BACK group %s (%d pairs -> PAIR)",
                     group.id, len(cg["pairs"]))
            model = {"replication_status": "enabled"}
            return model, [{"id": v.id, "replication_status": "enabled"} for v in (volumes or [])]

        target = secondary_backend_id or self._secondary_backend_id()
        for p in cg["pairs"]:
            p["state"] = PAIR_SSWS
        cg["direction"] = "reversed"
        cg["target"] = target
        self._write_copygroup(group.id, cg)
        LOG.info("PF9NFSRsync: failed over group %s to %s (%d pairs -> SSWS)",
                 group.id, target, len(cg["pairs"]))
        model = {"replication_status": "failed-over"}
        return model, [{"id": v.id, "replication_status": "failed-over"} for v in (volumes or [])]

    def list_replication_targets(self, context, group) -> dict:
        """Return the configured DR target(s) for a group."""
        return {"replication_targets": [{"backend_id": self._secondary_backend_id()}]}

    def manage_existing(self, volume, existing_ref: dict) -> dict:
        """Adopt an already-replicated volume on the secondary as a Cinder volume
        so it can be booted after failover.

        existing_ref = {"source-name": "<secondary LUN id>"}; resolved to the
        on-disk file via the group's pair table.
        """
        ref_name = existing_ref.get("source-name") or existing_ref.get("source-id")
        if not ref_name:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason="missing source-name/source-id")
        resolved = self._resolve_svol_ref(ref_name)
        src = self._ref_path({"source-name": resolved})
        if not Path(src).exists():
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason="no file for svol %s (resolved=%s)" % (ref_name, resolved))
        dst = str(Path(self._export()) / ("volume-%s" % volume.id))
        if os.path.abspath(src) != os.path.abspath(dst):
            try:
                os.replace(src, dst)
            except OSError as e:
                if e.errno not in (errno.EXDEV, errno.EPERM, errno.EACCES):
                    raise
                shutil.copy2(src, dst)
                try:
                    Path(src).unlink()
                except OSError as e:
                    LOG.warning("PF9NFSRsync: adopted %s via copy but could not "
                                "remove source %s: %s", dst, src, e)
        try:
            with (Path(self._meta_dir()) / PROMOTED_FILE).open("w") as f:
                f.write(str(int(time.time())))
        except OSError as e:
            LOG.warning("PF9NFSRsync: could not write promoted marker "
                        "(split-brain guard weakened): %s", e)
        LOG.info("PF9NFSRsync: managed svol %s (resolved %s) as volume %s "
                 "(site promoted)", ref_name, resolved, volume.id)
        return {"provider_location": self._nfs_share()}

    def manage_existing_get_size(self, volume, existing_ref: dict) -> int:
        """Return the size (GiB) of the volume being adopted."""
        ref_name = existing_ref.get("source-name") or existing_ref.get("source-id")
        resolved = self._resolve_svol_ref(ref_name) if ref_name else ref_name
        src = self._ref_path({"source-name": resolved})
        return max(1, int(Path(src).stat().st_size / (1024 ** 3)))

    def unmanage(self, volume) -> None:
        """Release Cinder's claim on a volume, leaving the file in place."""
        LOG.info("PF9NFSRsync: unmanaged volume %s (file retained)", volume.id)

    def _secondary_backend_id(self) -> str:
        """Parse the backend_id out of Cinder's replication_device config, which
        looks like 'backend_id:pf9-nfs-secondary,export:/export/psr'. Falls back
        to a sensible default so the prototype works even if unset.
        """
        rd = self.configuration.safe_get("replication_device")
        if isinstance(rd, dict):
            return rd.get("backend_id") or "pf9-nfs-secondary"
        if isinstance(rd, str) and rd:
            for part in rd.split(","):
                k, _, v = part.partition(":")
                if k.strip() == "backend_id" and v.strip():
                    return v.strip()
        return "pf9-nfs-secondary"

    def _export(self) -> str:
        shares = getattr(self, "_mounted_shares", None)
        if shares:
            return self._get_mount_point_for_share(shares[0])
        return self.configuration.safe_get("nfs_shares_config_export") or self._get_mount_point_base()

    def _nfs_share(self) -> str:
        """The NFS share string (host:/export), used as provider_location."""
        shares = getattr(self, "_mounted_shares", None)
        if shares:
            return shares[0]
        cfg = self.configuration.safe_get("nfs_shares_config")
        if cfg and Path(cfg).exists():
            with Path(cfg).open() as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        return line
        return None

    def _cg_dir(self, group_id: str) -> str:
        return str(Path(self._export()) / ("cg-%s" % group_id))

    def _vol_path(self, vol_id: str) -> str:
        d = str(Path(self._export()) / "volumes")
        Path(d).mkdir(parents=True, exist_ok=True)
        return str(Path(d) / vol_id)

    def _ref_path(self, existing_ref: dict) -> str:
        name = existing_ref.get("source-name") or existing_ref.get("source-id")
        if not name:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason="missing source-name/source-id")
        cands = [name] if name.startswith("volume-") else [name, "volume-%s" % name]
        # replicated data lands under incoming/current, so search there first
        for base in ("incoming/current", "incoming", "volumes", ""):
            for cand in cands:
                p = str(Path(self._export()) / base / cand)
                if Path(p).exists():
                    return p
        return str(Path(self._export()) / name)

    def _provider_location(self, vol_id: str) -> str:
        return "%s:volumes/%s" % (self._export(), vol_id)

    def _meta_dir(self) -> str:
        d = str(Path(self._export()) / PSR_META_DIR)
        Path(d).mkdir(parents=True, exist_ok=True)
        return d

    def _alloc_pair_ids(self) -> tuple:
        """Allocate the next distinct (primary, secondary) LUN-id pair, persisted
        so ids stay unique and stable. Returns (pvolId, svolId) as strings.
        """
        path = str(Path(self._meta_dir()) / LUN_ALLOC_FILE)
        try:
            with Path(path).open() as f:
                st = json.load(f)
        except (OSError, ValueError) as e:
            LOG.debug("PF9NFSRsync: no prior LUN allocation at %s "
                      "(seeding from base): %s", path, e)
            st = {"next_pvol": PVOL_BASE, "next_svol": SVOL_BASE}
        pvol, svol = st["next_pvol"], st["next_svol"]
        st["next_pvol"], st["next_svol"] = pvol + 1, svol + 1
        with Path(path).open("w") as f:
            json.dump(st, f)
        return str(pvol), str(svol)

    def _copygroup_path(self, group_id: str) -> str:
        return str(Path(self._cg_dir(group_id)) / COPYGROUP_FILE)

    def _read_copygroup(self, group_id: str) -> dict:
        Path(self._cg_dir(group_id)).mkdir(parents=True, exist_ok=True)
        try:
            with Path(self._copygroup_path(group_id)).open() as f:
                return json.load(f)
        except (OSError, ValueError) as e:
            LOG.debug("PF9NFSRsync: no copygroup for %s yet "
                      "(returning empty): %s", group_id, e)
            return {"copyGroup": group_id, "generation": 0,
                    "consistencyTime": 0, "direction": "P_to_S", "pairs": []}

    def _write_copygroup(self, group_id: str, data: dict) -> None:
        with Path(self._copygroup_path(group_id)).open("w") as f:
            json.dump(data, f)

    def _resolve_svol_ref(self, ref: str) -> str:
        """Map a secondary LUN id to the volume's cinder id by scanning the pair
        tables. Returns ref unchanged if no match.
        """
        export = self._export()
        for base in ("incoming/current", "incoming", "volumes", ""):
            d = str(Path(export) / base)
            try:
                entries = sorted(p.name for p in Path(d).iterdir())
            except OSError as e:
                LOG.debug("PF9NFSRsync: svol scan skipping %s: %s", d, e)
                continue
            for name in entries:
                if not name.startswith("cg-"):
                    continue
                try:
                    with (Path(d) / name / COPYGROUP_FILE).open() as f:
                        cg = json.load(f)
                except (OSError, ValueError) as e:
                    LOG.debug("PF9NFSRsync: svol scan skipping %s, unreadable "
                              "copygroup: %s", name, e)
                    continue
                for p in cg.get("pairs", []):
                    if p.get("svolId") == ref:
                        return p["cinderUuid"]
        return ref

    def _link_into_cg(self, group_id: str, vol_id: str) -> None:
        marker = str(Path(self._cg_dir(group_id)) / vol_id)
        Path(self._cg_dir(group_id)).mkdir(parents=True, exist_ok=True)
        with Path(marker).open("w") as f:
            f.write(vol_id)

    def _unlink_from_cg(self, group_id: str, vol_id: str) -> None:
        try:
            (Path(self._cg_dir(group_id)) / vol_id).unlink()
        except FileNotFoundError as e:
            LOG.debug("PF9NFSRsync: cg %s member marker for %s already gone: %s",
                      group_id, vol_id, e)

    def _copy(self, src: str, dst: str) -> None:
        if Path(src).exists():
            shutil.copy2(src, dst)

    def _write_repl_state(self, group_id: str, state: str, direction: str, target: str = "") -> None:
        Path(self._cg_dir(group_id)).mkdir(parents=True, exist_ok=True)
        payload = {
            "group_id": group_id,
            "replication_state": state,
            "replication_role": direction,
            "target": target,
            "updated_at": time.time(),
        }
        with (Path(self._cg_dir(group_id)) / ".repl_state.json").open("w") as f:
            json.dump(payload, f)

    @staticmethod
    def _volume_utils_ref():
        return volume_utils

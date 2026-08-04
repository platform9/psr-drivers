"""Platform9 NFS+rsync Cinder driver — the PSR prototype "vendor" (ADR-0003 §5, ADR-0008).

Purpose: give an NFS backend the SAME Cinder group-replication API surface a real
array's driver exposes, so PSR's full failover flow (enable → split/failover →
manage_existing → boot) runs end-to-end with NO storage array. rsync between two
NFS exports plays the role of array-to-array replication.

This is the reference the team wants so the prototype demonstrates the *identical*
production flow. Deliberately COMPLETE (no gaps) — unlike Hitachi's hbsd driver,
which is missing H1/H5/H7 in replication-active mode. PSR speaks standard Cinder
to this driver and never knows it's NFS underneath.

Scope: prototype/lab/CI only — not a supported production driver. It subclasses the
upstream NFS driver and adds the group-replication + manage/unmanage methods that
the standard Cinder contract defines (microversion 3.38 group actions + 3.8 manage).

Layout on each site's NFS export:
    <export>/volumes/<cinder-id>            # the volume file (the "LUN")
    <export>/cg-<group-id>/                 # consistency-group marker + members
    <export>/cg-<group-id>/.last_sync.json  # written by the rsync loop; PSR reads lag from here
The rsync loop (pf9_rsync_loop.sh) ships <export> → peer:<incoming> continuously.
"""

from __future__ import annotations

import errno
import json
import os
import shutil
import subprocess
import threading
import time

from oslo_config import cfg  # type: ignore
from oslo_log import log as logging  # type: ignore

from cinder import exception  # type: ignore
from cinder import interface  # type: ignore
from cinder.volume.drivers import nfs  # type: ignore
from cinder.volume import volume_utils  # type: ignore

LOG = logging.getLogger(__name__)

# ── in-driver replication config (v3: loop folded into the driver) ───────────
# Replaces the external pf9_rsync_loop.sh. The driver runs its own background
# replication thread whose lifecycle is tied to the driver (starts in do_setup,
# dies with the process), so there is no separate long-running script to babysit.
pf9_replication_opts = [
    cfg.IntOpt("pf9_replication_interval", default=30,
               help="Seconds between replication cycles (ship + flip)."),
    cfg.IntOpt("pf9_replication_keep_gens", default=3,
               help="Generations (point-in-time copies) to retain on the secondary."),
    cfg.StrOpt("pf9_replication_peer", default=None,
               help="SSH target for the secondary NFS server, e.g. "
                    "'ubuntu@10.10.7.166'. If unset, the replication thread "
                    "stays idle (single-site / CI)."),
    cfg.StrOpt("pf9_replication_peer_incoming", default=None,
               help="Incoming dir on the secondary export, e.g. "
                    "'/export/psr/incoming'. Defaults to '<export>/incoming' "
                    "resolved on the peer."),
    cfg.StrOpt("pf9_replication_ssh_user", default=None,
               help="Deprecated alias; prefer user@host in pf9_replication_peer."),
]
CONF = cfg.CONF
CONF.register_opts(pf9_replication_opts)

REPLICATION_STATE_ENABLED = "enabled"
REPLICATION_STATE_FAILED_OVER = "failed-over"

# ── v2: array-faithful pair model ────────────────────────────────────────────
# Real arrays give the primary and secondary volumes DIFFERENT LUN ids (pvol vs
# svol), and PSR must discover the pair + map A→B. We reproduce that with a
# per-CG pair table (the "copy-group status") + distinct LUN-id allocation, so
# PSR runs the identical discover→map→import path it would against Hitachi.
PAIR_SMPL = "SMPL"    # no pair
PAIR_COPY = "COPY"    # initial sync
PAIR_PAIR = "PAIR"    # replicating normally
PAIR_PSUS = "PSUS"    # split (planned)
PAIR_SSWS = "SSWS"    # secondary promoted / swapped (failed over)

COPYGROUP_FILE = ".copygroup.json"   # per-CG pair table (rides the replicated share)
PSR_META_DIR = ".psr-meta"           # LUN-id allocator lives here
LUN_ALLOC_FILE = "lun_alloc.json"
PROMOTED_FILE = "promoted"           # written on the SECONDARY when it adopts a
                                     # volume; the PRIMARY's replication thread
                                     # checks the peer for it and stops shipping
                                     # (so a B-side failover halts A even if A is
                                     # still alive — no split-brain overwrite).
PVOL_BASE = 1001                     # primary LUN ids start here
SVOL_BASE = 2001                     # secondary LUN ids start here (distinct range)


@interface.volumedriver
class PF9NFSRsyncDriver(nfs.NfsDriver):
    """NFS driver + group-replication contract, backed by rsync.

    Advertises replication so a replicated volume-type / group-type binds here.
    """

    VERSION = "0.1.0-prototype"
    CI_WIKI_NAME = "Platform9_PSR_NFS_Rsync_prototype"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # per-backend replication config + thread handles
        self.configuration.append_config_values(pf9_replication_opts)
        self._repl_stop = threading.Event()
        self._repl_thread = None

    def do_setup(self, context):
        super().do_setup(context)
        # Start the in-driver replication thread (replaces pf9_rsync_loop.sh).
        self._start_replication_thread()

    # ── capability advertisement ────────────────────────────────────────────
    def _update_volume_stats(self):
        super()._update_volume_stats()
        backend = self._secondary_backend_id()
        # Advertise replication on every reported pool (and at the top level, for
        # drivers/schedulers that read either) so a replicated volume-type /
        # group-type with replication_enabled='<is> True' binds here.
        pools = self._stats.get("pools")
        targets = [pool for pool in (pools or [])]
        for pool in targets:
            pool["replication_enabled"] = True
            pool["replication_type"] = ["async"]
            pool["replication_targets"] = [backend]
        self._stats["replication_enabled"] = True
        self._stats["replication_type"] = ["async"]
        self._stats["replication_targets"] = [backend]

    # ── in-driver replication thread (v3 — folds pf9_rsync_loop.sh inward) ────
    # An array replicates continuously in its own microcode, driven by pair
    # state; it is not a cron job an operator starts. So the driver owns the
    # replication loop: a daemon thread that, each interval, checks the pair
    # state and (if any CG is PAIR/COPY & P_to_S) ships a fresh generation to the
    # secondary, atomically flips it live, prunes old gens, and stamps lag.
    # State-gated exactly like the pairs: failover (SSWS) / disable (SMPL) stops
    # it; failback (PAIR) resumes it — no external process to start or stop.
    def _start_replication_thread(self):
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

    def _replication_worker(self):
        interval = int(self.configuration.safe_get(
            "pf9_replication_interval") or 30)
        while not self._repl_stop.is_set():
            try:
                self._replicate_once()
            except subprocess.CalledProcessError as e:
                LOG.warning("PF9NFSRsync: replication cycle failed rc=%s: %s",
                            e.returncode,
                            (e.stderr or b"").decode(errors="replace")[:300])
            except Exception as e:  # never let the thread die on a bad cycle
                LOG.warning("PF9NFSRsync: replication cycle error: %s", e)
            self._repl_stop.wait(interval)

    def _replicate_once(self):
        # STATE GATE: only ship if some CG is actively replicating primary->secondary.
        if not self._any_cg_active():
            return
        peer = self.configuration.safe_get("pf9_replication_peer")
        export = self._export().rstrip("/")
        incoming = (self.configuration.safe_get("pf9_replication_peer_incoming")
                    or (export + "/incoming"))
        # SECONDARY-PROMOTED GATE: if the peer (secondary) has adopted a volume it
        # writes a 'promoted' marker; stop shipping so we never overwrite a site
        # that has already taken over. Lets a failover triggered ON B halt A even
        # while A is alive.
        if self._peer_promoted(peer, incoming):
            LOG.info("PF9NFSRsync: peer %s is promoted; replication paused.", peer)
            return
        keep = int(self.configuration.safe_get(
            "pf9_replication_keep_gens") or 3)

        gen_n = self._next_generation()
        gen = "gen-%d" % gen_n
        start = time.time()

        # 1. ensure incoming/ exists on the peer (rsync won't mkdir the parent).
        self._ssh(peer, "mkdir -p '%s' && chmod 0777 '%s'" % (incoming, incoming))
        # 2. ship the whole export into a FRESH per-generation dir (in-progress
        #    copy never touches the live 'current').
        self._run(["rsync", "-az", "--delete",
                   "--exclude", PSR_META_DIR + "/",
                   "--exclude", "lost+found/",
                   "--exclude", "incoming/",
                   export + "/", "%s:%s/%s/" % (peer, incoming, gen)])
        elapsed = int(time.time() - start)
        # 3. ATOMIC FLIP current -> gen, then prune by generation NUMBER
        #    (not mtime — rsync -a ties mtimes) while protecting current's target.
        self._ssh(peer,
                  "set -e; cd '%s'; "
                  "ln -sfn '%s' current.tmp && mv -Tf current.tmp current; "
                  "cur=$(readlink current 2>/dev/null); "
                  "ls -1d gen-* 2>/dev/null | sed 's/^gen-//' | sort -rn "
                  "| tail -n +%d | sed 's/^/gen-/' | grep -vx \"$cur\" "
                  "| xargs -r rm -rf" % (incoming, gen, keep + 1))
        # 4. stamp per-CG lag (PSR reads RPO from replicated_at).
        now = int(time.time())
        for cgdir in self._iter_cg_dirs():
            cgn = os.path.basename(cgdir)[len("cg-"):]
            try:
                with open(os.path.join(cgdir, ".last_sync.json"), "w") as f:
                    json.dump({"cg": cgn, "generation": gen_n,
                               "replicated_at": now, "elapsed": elapsed}, f)
            except OSError:
                pass
        LOG.info("PF9NFSRsync: shipped %s (consistency_time=%s, %ss)",
                 gen, now, elapsed)

    def _any_cg_active(self) -> bool:
        """True if any CG is PAIR/COPY with direction not reversed — the same
        gate the old bash loop used. SSWS (failed over) / SMPL (disabled) /
        reversed all read False, so replication stops without overwriting a
        promoted secondary.
        """
        for cgdir in self._iter_cg_dirs():
            try:
                with open(os.path.join(cgdir, COPYGROUP_FILE)) as f:
                    cg = json.load(f)
            except (OSError, ValueError):
                continue
            if cg.get("direction") == "reversed":
                continue
            for p in cg.get("pairs", []):
                if p.get("state") in (PAIR_PAIR, PAIR_COPY):
                    return True
        return False

    def _iter_cg_dirs(self):
        try:
            names = os.listdir(self._export())
        except OSError:
            return
        for name in names:
            if name.startswith("cg-"):
                yield os.path.join(self._export(), name)

    def _next_generation(self) -> int:
        path = os.path.join(self._meta_dir(), "generation")
        try:
            with open(path) as f:
                g = int((f.read().strip() or "0"))
        except (OSError, ValueError):
            g = 0
        g += 1
        try:
            with open(path, "w") as f:
                f.write(str(g))
        except OSError:
            pass
        return g

    def _peer_marker_path(self, incoming: str) -> str:
        # The peer's export is the parent of its incoming/ dir; its .psr-meta sits
        # beside incoming/. e.g. incoming=/export/psr/incoming -> marker at
        # /export/psr/.psr-meta/promoted.
        peer_export = os.path.dirname(incoming.rstrip("/"))
        return os.path.join(peer_export, PSR_META_DIR, PROMOTED_FILE)

    def _peer_promoted(self, peer: str, incoming: str) -> bool:
        marker = self._peer_marker_path(incoming)
        try:
            r = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no",
                 "-o", "ConnectTimeout=10", peer, "test -f '%s'" % marker],
                capture_output=True, timeout=30)
            return r.returncode == 0
        except Exception:
            # Peer unreachable: don't assume promoted; let the ship attempt run
            # (it'll fail its own way if the peer is truly down).
            return False

    def _clear_peer_promoted(self, peer: str, incoming: str) -> None:
        """Best-effort: clear the peer's promoted marker so replication can resume
        (used on failback). No-op if the peer is unreachable."""
        marker = self._peer_marker_path(incoming)
        try:
            self._ssh(peer, "rm -f '%s'" % marker)
        except Exception as e:
            LOG.warning("PF9NFSRsync: could not clear peer promoted marker: %s", e)

    def _ssh(self, peer: str, remote_cmd: str) -> None:
        self._run(["ssh", "-o", "StrictHostKeyChecking=no",
                   "-o", "ConnectTimeout=10", peer, remote_cmd])

    def _run(self, argv) -> None:
        subprocess.run(argv, check=True, capture_output=True, timeout=600)

    # ── volume create: seed replication_status ───────────────────────────────
    def _type_is_replicated(self, volume):
        # A volume-type carrying replication_enabled='<is> True' is replication
        # CAPABLE. Real replication drivers report such a volume as
        # replication_status='disabled' at create time (capable, not yet paired);
        # Cinder REQUIRES that before group-enable-replication will accept it.
        try:
            specs = (volume.volume_type.extra_specs or {})
        except Exception:
            return False
        val = str(specs.get("replication_enabled", "")).lower()
        return "true" in val

    def create_volume(self, volume):
        model = super().create_volume(volume) or {}
        if self._type_is_replicated(volume):
            model["replication_status"] = "disabled"
        return model

    # ── consistency groups (generic volume groups) ──────────────────────────
    def create_group(self, context, group):
        path = self._cg_dir(group.id)
        os.makedirs(path, exist_ok=True)
        LOG.info("PF9NFSRsync: created group dir %s", path)
        return {"status": "available"}

    def delete_group(self, context, group, volumes):
        # Remove the member volume files too — otherwise they leak on the export
        # while Cinder marks the volumes deleted.
        for v in (volumes or []):
            try:
                os.remove(self._vol_path(v.id))
            except FileNotFoundError:
                pass
        shutil.rmtree(self._cg_dir(group.id), ignore_errors=True)
        model = {"status": "deleted"}
        return model, [{"id": v.id, "status": "deleted"} for v in (volumes or [])]

    def update_group(self, context, group, add_volumes=None, remove_volumes=None):
        # This is Hitachi gap H3 (fails in replication-active mode). Here it just works.
        for v in add_volumes or []:
            self._link_into_cg(group.id, v.id)
        for v in remove_volumes or []:
            self._unlink_from_cg(group.id, v.id)
        return {"status": "available"}, None, None

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        # Hitachi gap H4. Here: reflink/copy each member into a snapshot dir.
        snap_dir = self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id
        os.makedirs(snap_dir, exist_ok=True)
        for s in snapshots:
            self._copy(self._vol_path(s.volume_id), os.path.join(snap_dir, s.volume_id))
        return {"status": "available"}, [{"id": s.id, "status": "available"} for s in snapshots]

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        shutil.rmtree(self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id, ignore_errors=True)
        return {"status": "deleted"}, [{"id": s.id, "status": "deleted"} for s in snapshots]

    # ── group snapshot clone (test recovery / create-from-src) ───────────────
    def create_group_from_src(self, context, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        """Create a writable group + member volumes from a group snapshot (or a
        source group) — the test-recovery 'clone the CG' path PSR drives via
        Cinder create-from-src. Copies each source file into the new volume file.
        """
        os.makedirs(self._cg_dir(group.id), exist_ok=True)
        pairs = []
        if group_snapshot and snapshots:
            snap_dir = self._cg_dir(group_snapshot.group_id) + "/.snap-" + group_snapshot.id
            # Map each new volume to ITS source snapshot via snapshot_id. Cinder does
            # NOT guarantee snapshots/volumes arrive in the same order, so zip() would
            # copy the wrong disk into the wrong volume (silent data corruption).
            snap_by_id = {s.id: s for s in snapshots}
            for v in volumes:
                s = snap_by_id.get(v.snapshot_id)
                if s is None:
                    raise exception.InvalidInput(
                        reason="create_group_from_src: no source snapshot for volume "
                               "%s (snapshot_id=%s)" % (v.id, v.snapshot_id))
                pairs.append((os.path.join(snap_dir, s.volume_id), v))
        elif source_group and source_vols:
            # Map by source_volid, not position.
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

    # ── group replication (microversion 3.38) ───────────────────────────────
    def enable_replication(self, context, group, volumes):
        """createHURPair equivalent (Hitachi gap H5). Creates a real pair per
        member volume, allocating a DISTINCT svol LUN id (!= the pvol id) — like
        an array's create-pair. State -> PAIR. The pair table (.copygroup.json)
        is the 'copy-group status' PSR discovers to map A->B.
        """
        cg = self._read_copygroup(group.id)
        by_uuid = {p["cinderUuid"]: p for p in cg["pairs"]}
        for v in (volumes or []):
            p = by_uuid.get(v.id)
            if p is None:
                pvol, svol = self._alloc_pair_ids()
                cg["pairs"].append({"pvolId": pvol, "svolId": svol,
                                    "cinderUuid": v.id, "state": PAIR_PAIR})
            else:
                p["state"] = PAIR_PAIR
        cg["direction"] = "P_to_S"
        self._write_copygroup(group.id, cg)
        model = {"replication_status": "enabled"}
        vol_models = [{"id": v.id, "replication_status": "enabled"} for v in (volumes or [])]
        return model, vol_models

    def disable_replication(self, context, group, volumes):
        """Hitachi gap H6 — tear the pairs down (state -> SMPL)."""
        cg = self._read_copygroup(group.id)
        for p in cg["pairs"]:
            p["state"] = PAIR_SMPL
        cg["direction"] = "none"
        self._write_copygroup(group.id, cg)
        return {"replication_status": "disabled"}, [{"id": v.id, "replication_status": "disabled"} for v in (volumes or [])]

    def failover_replication(self, context, group, volumes, secondary_backend_id=None):
        """splitHURPair/promote equivalent (Hitachi gap H7).

        Two directions, like a real array:
          * FAILOVER (target = the secondary, or unset): split + promote secondary.
            state -> SSWS, direction reversed, group replication_status 'failed-over'.
            Data is already on the secondary (rsync); manage_existing adopts by svol id.
          * FAILBACK (target == 'default'): Cinder's sentinel for "resume the primary."
            Re-establish the pair primary->secondary. state -> PAIR, direction P_to_S,
            group replication_status 'enabled'. This is the ONLY exit Cinder allows
            from the failed-over state (it rejects enable/disable from there).
        """
        cg = self._read_copygroup(group.id)
        if secondary_backend_id == "default":
            for p in cg["pairs"]:
                p["state"] = PAIR_PAIR
            cg["direction"] = "P_to_S"
            cg["target"] = "default"
            self._write_copygroup(group.id, cg)
            # Clear the peer's promoted marker so the replication thread resumes
            # shipping to it (failback re-establishes primary -> secondary).
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

    def list_replication_targets(self, context, group):
        """Hitachi gap H8."""
        return {"replication_targets": [{"backend_id": self._secondary_backend_id()}]}

    # ── manage / unmanage (microversion 3.8) ─────────────────────────────────
    def manage_existing(self, volume, existing_ref):
        """THE step Hitachi blocks (H1) for a still-paired LUN. Here it just works:
        adopt an existing file (the replicated/promoted 'S-VOL') as a Cinder volume.

        existing_ref = {"source-name": "<file-name-on-export>"} — this is what PSR
        passes as DiscoveredVolume.spec.secondaryLunId at failover.
        """
        # v2: source-name is the SECONDARY LUN id (svolId = DiscoveredVolume
        # .secondaryLunId). Resolve it to the on-disk file via the pair table.
        ref_name = existing_ref.get("source-name") or existing_ref.get("source-id")
        if not ref_name:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason="missing source-name/source-id")
        resolved = self._resolve_svol_ref(ref_name)
        src = self._ref_path({"source-name": resolved})
        if not os.path.exists(src):
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason="no file for svol %s (resolved=%s)" % (ref_name, resolved))
        # Place the adopted file where the stock NfsDriver expects it —
        # "<mount-root>/volume-<id>" — NOT our volumes/ subdir. Nova's attach path
        # calls NfsDriver.initialize_connection, which derives the on-disk name as
        # volume-<id> at the share root and the connection 'export' from
        # provider_location=<share>. A custom layout makes attach fail with
        # "[Errno 2] No such file or directory".
        dst = os.path.join(self._export(), "volume-%s" % volume.id)
        if os.path.abspath(src) != os.path.abspath(dst):
            try:
                os.replace(src, dst)  # fast path: rename within the same filesystem
            except OSError as e:
                if e.errno not in (errno.EXDEV, errno.EPERM, errno.EACCES):
                    raise
                # Can't rename the source into place:
                #  - EXDEV: incoming/ and volumes/ on different mounts;
                #  - EPERM/EACCES: the rsync loop delivers gen dirs with the
                #    source's sticky bit (1777) and files owned by the ssh user,
                #    so cinder (user 'pf9') can't rename a file it doesn't own out
                #    of a sticky dir.
                # Copy into place instead; the leftover source copy is harmless
                # (it lives under incoming/ and gets pruned/overwritten later).
                shutil.copy2(src, dst)
                try:
                    os.remove(src)
                except OSError:
                    pass
        # Adopting a replicated volume == promoting this site. Drop a 'promoted'
        # marker so the PRIMARY's replication thread (which checks the peer for it)
        # stops shipping — this is what makes a failover triggered ON B halt A,
        # with no external script and no call back to A.
        try:
            with open(os.path.join(self._meta_dir(), PROMOTED_FILE), "w") as f:
                f.write(str(int(time.time())))
        except OSError:
            pass
        LOG.info("PF9NFSRsync: managed svol %s (resolved %s) as volume %s "
                 "(site promoted)", ref_name, resolved, volume.id)
        # provider_location MUST be the NFS share string (host:/export), the same
        # value NfsDriver.create_volume stores, so initialize_connection can hand
        # Nova a mountable export. NOT our "<mount>:volumes/<id>" form.
        return {"provider_location": self._nfs_share()}

    def manage_existing_get_size(self, volume, existing_ref):
        ref_name = existing_ref.get("source-name") or existing_ref.get("source-id")
        resolved = self._resolve_svol_ref(ref_name) if ref_name else ref_name
        src = self._ref_path({"source-name": resolved})
        return max(1, int(os.path.getsize(src) / (1024 ** 3)))  # bytes -> GiB, min 1

    def unmanage(self, volume):
        """Hitachi gap H2 (fails in replication). Here: drop Cinder's claim, keep the file."""
        LOG.info("PF9NFSRsync: unmanaged volume %s (file retained)", volume.id)

    # ── helpers ──────────────────────────────────────────────────────────────
    def _secondary_backend_id(self) -> str:
        """Parse the backend_id out of Cinder's replication_device config, which
        looks like 'backend_id:pf9-nfs-secondary,export:/export/psr'. Falls back
        to a sensible default so the prototype works even if unset.
        """
        rd = self.configuration.safe_get("replication_device")
        # replication_device may arrive as a dict (parsed by oslo) or a raw string.
        if isinstance(rd, dict):
            return rd.get("backend_id") or "pf9-nfs-secondary"
        if isinstance(rd, str) and rd:
            for part in rd.split(","):
                k, _, v = part.partition(":")
                if k.strip() == "backend_id" and v.strip():
                    return v.strip()
        return "pf9-nfs-secondary"

    def _export(self) -> str:
        # Return the ACTUAL mounted share dir, not the mount base. NfsDriver mounts
        # each share at <base>/<hash>/, so the base is NOT the share — writing cg/
        # volume/manage files to the base means they never land on the NFS export
        # that rsync replicates. Use the first mounted share's mount point.
        shares = getattr(self, "_mounted_shares", None)
        if shares:
            return self._get_mount_point_for_share(shares[0])
        return self.configuration.safe_get("nfs_shares_config_export") or self._get_mount_point_base()

    def _nfs_share(self) -> str:
        """The NFS share string (host:/export) — what NfsDriver stores as
        provider_location so initialize_connection can build a mountable export.
        """
        shares = getattr(self, "_mounted_shares", None)
        if shares:
            return shares[0]
        cfg = self.configuration.safe_get("nfs_shares_config")
        if cfg and os.path.exists(cfg):
            with open(cfg) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        return line
        return None

    def _cg_dir(self, group_id: str) -> str:
        return os.path.join(self._export(), "cg-%s" % group_id)

    def _vol_path(self, vol_id: str) -> str:
        d = os.path.join(self._export(), "volumes")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, vol_id)

    def _ref_path(self, existing_ref) -> str:
        name = existing_ref.get("source-name") or existing_ref.get("source-id")
        if not name:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason="missing source-name/source-id")
        # NfsDriver stores volumes as "volume-<id>"; also accept a bare name.
        cands = [name] if name.startswith("volume-") else [name, "volume-%s" % name]
        # search where the v2 rsync loop delivers (incoming/current/) first, then
        # older layouts, then the active export.
        for base in ("incoming/current", "incoming", "volumes", ""):
            for cand in cands:
                p = os.path.join(self._export(), base, cand)
                if os.path.exists(p):
                    return p
        return os.path.join(self._export(), name)

    def _provider_location(self, vol_id: str) -> str:
        return "%s:volumes/%s" % (self._export(), vol_id)

    # ── v2 pair-table + LUN-id helpers ───────────────────────────────────────
    def _meta_dir(self) -> str:
        d = os.path.join(self._export(), PSR_META_DIR)
        os.makedirs(d, exist_ok=True)
        return d

    def _alloc_pair_ids(self):
        """Allocate the next DISTINCT (pvol, svol) LUN-id pair, persisted on the
        export so ids stay unique + stable. Mirrors an array giving the primary
        and secondary different LDEV numbers. Returns (pvolId, svolId) strings.
        """
        path = os.path.join(self._meta_dir(), LUN_ALLOC_FILE)
        try:
            with open(path) as f:
                st = json.load(f)
        except (OSError, ValueError):
            st = {"next_pvol": PVOL_BASE, "next_svol": SVOL_BASE}
        pvol, svol = st["next_pvol"], st["next_svol"]
        st["next_pvol"], st["next_svol"] = pvol + 1, svol + 1
        with open(path, "w") as f:
            json.dump(st, f)
        return str(pvol), str(svol)

    def _copygroup_path(self, group_id: str) -> str:
        return os.path.join(self._cg_dir(group_id), COPYGROUP_FILE)

    def _read_copygroup(self, group_id: str) -> dict:
        os.makedirs(self._cg_dir(group_id), exist_ok=True)
        try:
            with open(self._copygroup_path(group_id)) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {"copyGroup": group_id, "generation": 0,
                    "consistencyTime": 0, "direction": "P_to_S", "pairs": []}

    def _write_copygroup(self, group_id: str, data: dict) -> None:
        with open(self._copygroup_path(group_id), "w") as f:
            json.dump(data, f)

    def _resolve_svol_ref(self, ref: str) -> str:
        """Map a secondary LUN id (svolId — what PSR passes as source-name at
        failover) to the volume's cinder id, by scanning the CG pair tables.
        Falls back to returning ref unchanged (v1 raw-name compat).
        """
        export = self._export()
        # On the SECONDARY the pair table arrives under incoming/current/cg-*
        # (where the rsync loop delivers it), NOT at the export root. Scan the
        # same locations _ref_path uses, incoming/current first.
        for base in ("incoming/current", "incoming", "volumes", ""):
            d = os.path.join(export, base)
            try:
                entries = os.listdir(d)
            except OSError:
                continue
            for name in entries:
                if not name.startswith("cg-"):
                    continue
                try:
                    with open(os.path.join(d, name, COPYGROUP_FILE)) as f:
                        cg = json.load(f)
                except (OSError, ValueError):
                    continue
                for p in cg.get("pairs", []):
                    if p.get("svolId") == ref:
                        return p["cinderUuid"]
        return ref

    def _link_into_cg(self, group_id: str, vol_id: str) -> None:
        marker = os.path.join(self._cg_dir(group_id), vol_id)
        os.makedirs(self._cg_dir(group_id), exist_ok=True)
        with open(marker, "w") as f:
            f.write(vol_id)

    def _unlink_from_cg(self, group_id: str, vol_id: str) -> None:
        try:
            os.remove(os.path.join(self._cg_dir(group_id), vol_id))
        except FileNotFoundError:
            pass

    def _copy(self, src: str, dst: str) -> None:
        if os.path.exists(src):
            shutil.copy2(src, dst)

    def _write_repl_state(self, group_id: str, state: str, direction: str, target: str = "") -> None:
        os.makedirs(self._cg_dir(group_id), exist_ok=True)
        payload = {
            "group_id": group_id,
            "replication_state": state,
            "replication_role": direction,
            "target": target,
            "updated_at": time.time(),
        }
        with open(os.path.join(self._cg_dir(group_id), ".repl_state.json"), "w") as f:
            json.dump(payload, f)

    @staticmethod
    def _volume_utils_ref():  # kept to show upstream helper availability
        return volume_utils

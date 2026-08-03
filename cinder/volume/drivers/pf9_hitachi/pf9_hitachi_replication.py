"""
Platform9 Hitachi VSP Extended Replication Driver.

Implements Cinder group-replication actions (microversion 3.38):
- H1: manage_existing() — import promoted S-VOLs post-failover
- H5: enable_replication() — activate UR replication on a CG
- H7: failover_replication() — split and promote S-VOLs
- H2: unmanage() — release Cinder claim without affecting pair
- H6: disable_replication() — delete UR pairs
- H4: create_group_snapshot() — snapshot S-VOLs for test recovery
- H3: update_group() — add/remove volumes from live CG
- H8: list_replication_targets() — return secondary backend_id
"""

from __future__ import annotations

import json
import logging
import time

import requests

from cinder import exception, interface
from cinder.volume.drivers.pf9_hitachi import hbsd_fc, hbsd_iscsi

LOG = logging.getLogger(__name__)

# Pair states that indicate active replication
HEALTHY_PAIR_STATUS = ("PAIR", "COPY")
SWAPPED_PAIR_STATUS = ("SSWS",)  # Secondary split, writable


class HBSDRestClientError(exception.VolumeDriverException):
    """Configuration Manager REST API error."""

    message = "Hitachi CM REST error: %(msg)s"


class HBSDConfigurationManagerRestClient:
    """
    Hitachi Configuration Manager REST client.

    Handles:
    - Session authentication (username/password -> sessionId/token)
    - Synchronous and asynchronous job contracts (Response-Max-Wait header)
    - Job polling for long-running operations
    - Standard error handling and retries
    """

    def __init__(
        self,
        rest_api_ip: str,
        rest_api_port: int = 443,
        username: str = "system",
        password: str = "",
        storage_device_id: str = "",
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        """
        Initialize CM REST client.

        Args:
            rest_api_ip: Configuration Manager hostname or IP
            rest_api_port: CM REST API port (default 443)
            username: REST API login username
            password: REST API login password
            storage_device_id: Storage array ID (for pair operations)
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        self._base = f"https://{rest_api_ip}:{rest_api_port}/ConfigurationManager"
        self._username = username
        self._password = password
        self.storage_device_id = storage_device_id
        self._verify_ssl = verify_ssl
        self._timeout = timeout
        self._session_id: int | None = None
        self._token: str | None = None

    def login(self) -> None:
        """Authenticate with Configuration Manager and obtain session token."""
        resp = requests.post(
            f"{self._base}/v1/objects/sessions",
            auth=(self._username, self._password),
            verify=self._verify_ssl,
            timeout=self._timeout,
        )
        self._raise_for_status(resp)
        body = resp.json()
        self._session_id = body.get("sessionId")
        self._token = body.get("token")
        if not self._session_id:
            raise HBSDRestClientError(msg="No sessionId in auth response")
        LOG.info("CM REST authenticated (session %s)", str(self._session_id)[:16])

    def logout(self) -> None:
        """Close the session."""
        if self._session_id is None:
            return
        try:
            requests.delete(
                f"{self._base}/v1/objects/sessions/{self._session_id}",
                headers=self._headers(),
                verify=self._verify_ssl,
                timeout=self._timeout,
            )
        except requests.HTTPError as e:
            LOG.warning("Error closing CM REST session: %s", e)
        finally:
            self._session_id = None
            self._token = None

    def _headers(self, extra: dict | None = None) -> dict:
        """Build HTTP headers for authenticated requests."""
        headers = {
            "Authorization": f"Session {self._token}",
            "Content-Type": "application/json",
            "Response-Max-Wait": "60",
        }
        if extra:
            headers.update(extra)
        return headers

    def _raise_for_status(self, resp: requests.Response) -> None:
        """Raise exception on HTTP error."""
        if resp.status_code >= 400:
            try:
                detail = resp.json()
            except ValueError:
                detail = resp.text[:200]
            raise HBSDRestClientError(msg=f"{resp.status_code} {detail}")

    def request(
        self,
        method: str,
        path: str,
        json_body: dict | None = None,
        extra_headers: dict | None = None,
    ) -> dict | None:
        """
        Execute a REST request with automatic session management.

        Args:
            method: HTTP method (GET, POST, DELETE)
            path: API path (e.g., /v1/objects/remote-mirror-copypairs)
            json_body: Request body (for POST/DELETE)
            extra_headers: Additional headers to add

        Returns:
            Parsed JSON response, or None if response had no content

        Raises:
            HBSDRestClientError: On HTTP error or auth failure
        """
        if self._token is None:
            self.login()
        resp = requests.request(
            method,
            f"{self._base}{path}",
            headers=self._headers(extra_headers),
            json=json_body,
            verify=self._verify_ssl,
            timeout=self._timeout,
        )
        self._raise_for_status(resp)

        # Handle async jobs (202 Accepted)
        if resp.status_code == 202:
            job = resp.json() if resp.content else {}
            job_id = job.get("jobId") or (
                job.get("affectedResources", [None])[0]
                if job.get("affectedResources")
                else None
            )
            return self._wait_for_job(job_id) if job_id else job

        # Return parsed JSON or None
        return resp.json() if resp.content else None

    def _wait_for_job(
        self, job_id: str, interval: int = 3, timeout: int = 86400
    ) -> dict:
        """
        Poll a job until completion.

        Jobs are represented by jobId and live at /v1/objects/jobs/{jobId}.
        Polling continues until status != "Running".

        Args:
            job_id: Job ID to poll
            interval: Polling interval in seconds
            timeout: Max total wait time in seconds

        Returns:
            Final job object

        Raises:
            HBSDRestClientError: If job fails or times out
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.request("GET", f"/v1/objects/jobs/{job_id}")
            if job and job.get("status") != "Running":
                if job.get("state") == "Failed":
                    raise HBSDRestClientError(
                        msg=f"Job {job_id} failed: {job.get('error')}"
                    )
                return job
            time.sleep(interval)

        raise HBSDRestClientError(msg=f"Job {job_id} timed out after {timeout}s")


class HBSDGroupReplicationMixin:
    """
    Group-replication contract backed by Hitachi UR copy groups.

    This mixin supplies the methods Cinder's group-replication (3.38),
    generic-volume-group, and manage/unmanage actions call. It communicates
    with Configuration Manager REST only, never with array front-end ports
    (FC/iSCSI). This allows the same logic to sit under either transport
    driver without modification.
    """

    def __init__(self, *args, **kwargs):
        """Initialize mixin and underlying transport driver."""
        super().__init__(*args, **kwargs)
        self._cm_rest_client: HBSDConfigurationManagerRestClient | None = None
        self._remote_cm_rest_client: HBSDConfigurationManagerRestClient | None = None

    def _local_cm_client(self) -> HBSDConfigurationManagerRestClient:
        """Get or create CM REST client for the local (primary) array."""
        if self._cm_rest_client is None:
            cfg = self.configuration
            self._cm_rest_client = HBSDConfigurationManagerRestClient(
                rest_api_ip=cfg.safe_get("san_ip") or "127.0.0.1",
                rest_api_port=int(cfg.safe_get("san_api_port") or 443),
                username=cfg.safe_get("san_login") or "system",
                password=cfg.safe_get("san_password") or "",
                storage_device_id=cfg.safe_get("hitachi_storage_id") or "",
                verify_ssl=cfg.safe_get("driver_ssl_cert_verify") or False,
            )
        return self._cm_rest_client

    def _remote_cm_client(self) -> HBSDConfigurationManagerRestClient:
        """Get or create CM REST client for the remote (secondary) array."""
        if self._remote_cm_rest_client is None:
            rd = self._replication_device()
            self._remote_cm_rest_client = HBSDConfigurationManagerRestClient(
                rest_api_ip=rd.get("san_ip") or "127.0.0.1",
                rest_api_port=int(rd.get("san_api_port") or 443),
                username=rd.get("san_login") or "system",
                password=rd.get("san_password") or "",
                storage_device_id=rd.get("storage_id") or "",
                verify_ssl=rd.get("driver_ssl_cert_verify") or False,
            )
        return self._remote_cm_rest_client

    def _replication_device(self) -> dict:
        """Parse replication_device config from cinder.conf."""
        rd = self.configuration.safe_get("replication_device")
        if isinstance(rd, list):
            return rd[0] if rd else {}
        if isinstance(rd, dict):
            return rd
        if isinstance(rd, str):
            # Parse comma-separated key:value pairs
            result = {}
            for part in rd.split(","):
                k, _, v = part.partition(":")
                result[k.strip()] = v.strip()
            return result
        return {}

    def _copy_pair_key(self, group_id: str, volume_id: str) -> tuple:
        """
        Generate the 5-part key CM REST uses to address a copy pair.

        Format: (remoteStorageDeviceId, copyGroupName, localDeviceGroupName,
                 remoteDeviceGroupName, copyPairName)
        """
        remote_storage_id = self._replication_device().get("storage_id", "")
        local_dg = f"{group_id}_local"[:31]  # Max 31 chars
        remote_dg = f"{group_id}_remote"[:31]
        copy_pair_name = volume_id[:31]  # Max 31 chars, case sensitive
        return (remote_storage_id, group_id[:31], local_dg, remote_dg, copy_pair_name)

    def _update_volume_stats(self):
        """Advertise replication and consistency group support."""
        super()._update_volume_stats()
        if self.configuration.safe_get("replication_device"):
            pools = self._stats.get("pools") or []
            for pool in pools:
                pool["consistencygroup_support"] = True
                pool["group_replication_enabled"] = True
                pool["replication_enabled"] = True
                pool["replication_type"] = ["async"]

    def enable_replication(self, context, group, volumes):
        """
        Enable UR replication for a consistency group.

        Creates one copy pair (copyPairName) per member volume in the CG.
        First volume triggers CG creation (isNewGroupCreation=True);
        subsequent volumes join the same CG.

        Args:
            context: Cinder request context
            group: Group object
            volumes: List of volumes in the group

        Returns:
            Tuple (group_model, volume_models) with replication_status
        """
        client = self._local_cm_client()
        rd = self._replication_device()
        vol_models = []

        for idx, volume in enumerate(volumes or []):
            try:
                # Generate the copy pair key and individual pair name
                _, _, local_dg, remote_dg, copy_pair_name = self._copy_pair_key(
                    group.id, volume.id
                )

                # Build the create-pair request body
                body = {
                    "replicationType": "UR",
                    "copyGroupName": group.id[:31],
                    "copyPairName": copy_pair_name,
                    "localDeviceGroupName": local_dg,
                    "remoteDeviceGroupName": remote_dg,
                    "remoteStorageDeviceId": rd.get("storage_id", ""),
                    "pvolLdevId": self._ldev_id_for(volume),
                    "svolLdevId": self._svol_ldev_id_for(volume, rd),
                    "isNewGroupCreation": idx == 0,  # Only first pair creates CG
                    "doInitialCopy": True,
                    "fenceLevel": "ASYNC",
                    "muNumber": int(
                        self.configuration.safe_get("hitachi_replication_mun") or 1
                    ),
                }
                LOG.info("Creating UR pair %s (new_cg=%s)", copy_pair_name, idx == 0)
                client.request(
                    "POST", "/v1/objects/remote-mirror-copypairs", json_body=body
                )
                vol_models.append({"id": volume.id, "replication_status": "enabled"})
            except HBSDRestClientError as e:
                LOG.error(
                    "Failed to enable replication for volume %s: %s", volume.id, e
                )
                vol_models.append({"id": volume.id, "replication_status": "error"})
        return {"status": "available"}, vol_models

    def disable_replication(self, context, group, volumes):
        """
        Disable UR replication for a consistency group.

        Deletes each member volume's copy pair (but leaves the CG itself).

        Args:
            context: Cinder request context
            group: Group object
            volumes: List of volumes to remove from replication

        Returns:
            Tuple (group_model, volume_models) with replication_status=disabled
        """
        client = self._local_cm_client()
        vol_models = []

        for volume in volumes or []:
            try:
                key = self._copy_pair_key(group.id, volume.id)
                path = "/v1/objects/remote-mirror-copypairs/" + ",".join(
                    str(k) for k in key
                )
                LOG.info("Deleting UR pair for volume %s", volume.id)
                client.request("DELETE", path)
                vol_models.append({"id": volume.id, "replication_status": "disabled"})
            except HBSDRestClientError as e:
                LOG.error(
                    "Failed to disable replication for volume %s: %s", volume.id, e
                )
                vol_models.append({"id": volume.id, "replication_status": "error"})
        return {"status": "available"}, vol_models

    def failover_replication(
        self,
        context,
        group,
        volumes,
        secondary_backend_id=None,
        allow_attached_volume=False,
    ):
        """
        Failover a consistency group to the secondary site.

        Correct sequence:
        1. Split: POST .../actions/split/invoke on LOCAL array
           -> Primary suspended, secondary split
        2. Takeover: POST .../actions/takeover/invoke on REMOTE array
           -> Secondary S-VOL becomes writable

        Args:
            context: Cinder request context
            group: Group object
            volumes: List of volumes to failover
            secondary_backend_id: Target secondary backend ID
            allow_attached_volume: If True, use force=true for emergency failover (primary unreachable)

        Returns:
            Tuple (secondary_backend_id, volume_models) with replication_status=failed-over
        """
        rd = self._replication_device()
        target_backend_id = secondary_backend_id or rd.get("backend_id", "")

        local_client = self._local_cm_client()
        remote_client = self._remote_cm_client()
        vol_models = []

        for volume in volumes or []:
            try:
                storage_id, copy_group, local_dg, remote_dg, copy_pair_name = (
                    self._copy_pair_key(group.id, volume.id)
                )
                pair_key = ",".join(
                    str(k)
                    for k in (
                        storage_id,
                        copy_group,
                        local_dg,
                        remote_dg,
                        copy_pair_name,
                    )
                )

                # Phase 1: Split the pair on the primary (local) array
                LOG.info(
                    "Splitting pair %s on primary (force=%s)",
                    copy_pair_name,
                    allow_attached_volume,
                )
                split_params = {"replicationType": "UR"}
                if allow_attached_volume:
                    # Emergency failover: force split even if primary is unreachable
                    split_params["force"] = True
                    LOG.warning(
                        "Emergency failover for pair %s: forcing split (data loss possible)",
                        copy_pair_name,
                    )
                local_client.request(
                    "POST",
                    f"/v1/objects/remote-mirror-copypairs/{pair_key}/actions/split/invoke",
                    json_body={"parameters": split_params},
                )

                # Phase 2: Takeover on the secondary (remote) array
                takeover_key = ",".join(
                    str(k)
                    for k in (
                        "NotSpecified",
                        copy_group,
                        local_dg,
                        "NotSpecified",
                        copy_pair_name,
                    )
                )
                failover_timeout = int(
                    self.configuration.safe_get("hitachi_failover_timeout") or 7200
                )
                LOG.info(
                    "Taking over pair %s on secondary (timeout=%ds)",
                    copy_pair_name,
                    failover_timeout,
                )
                remote_client.request(
                    "POST",
                    f"/v1/objects/remote-mirror-copypairs/{takeover_key}/actions/takeover/invoke",
                    json_body={
                        "parameters": {"mode": "auto", "timeout": failover_timeout}
                    },
                )
                vol_models.append(
                    {"id": volume.id, "replication_status": "failed-over"}
                )
            except HBSDRestClientError as e:
                LOG.error("H7: Failover failed for volume %s: %s", volume.id, e)
                vol_models.append(
                    {"id": volume.id, "replication_status": "failover-error"}
                )
        return target_backend_id, vol_models

    def failback_replication(self, context, group, volumes, secondary_backend_id=None):
        """
        Failback a consistency group to the primary site.

        Reverses a previous failover by resyncing and swapping roles.
        This allows recovery to the original primary site after failover.

        Sequence:
        1. Resync: POST .../actions/resync/invoke on SECONDARY array
           with replicationType: swap to reverse direction
        2. Wait for resync to complete: Poll pair status until PAIR
        3. Optional swap: Can toggle roles back to original direction

        Args:
            context: Cinder request context
            group: Group object
            volumes: List of volumes to failback
            secondary_backend_id: Secondary backend ID (unused for failback)

        Returns:
            Tuple (primary_backend_id, volume_models) with replication_status=enabled
        """
        self._replication_device()
        primary_backend_id = self.configuration.safe_get("volume_backend_name", "")

        local_client = self._local_cm_client()
        remote_client = self._remote_cm_client()
        vol_models = []

        resync_timeout = int(
            self.configuration.safe_get("hitachi_resync_timeout") or 3600
        )

        for volume in volumes or []:
            try:
                _, copy_group, local_dg, _, copy_pair_name = self._copy_pair_key(
                    group.id, volume.id
                )
                resync_key = ",".join(
                    str(k)
                    for k in (
                        "NotSpecified",
                        copy_group,
                        local_dg,
                        "NotSpecified",
                        copy_pair_name,
                    )
                )
                LOG.info(
                    "Starting resync for pair %s (failback to primary)", copy_pair_name
                )
                remote_client.request(
                    "POST",
                    f"/v1/objects/remote-mirror-copypairs/{resync_key}/actions/resync/invoke",
                    json_body={"parameters": {"replicationType": "swap"}},
                )
                LOG.info("Waiting for pair %s to reach PAIR status", copy_pair_name)
                self._wait_for_pair_status(
                    local_client,
                    copy_group,
                    copy_pair_name,
                    expected_status="PAIR",
                    timeout=resync_timeout,
                )
                vol_models.append({"id": volume.id, "replication_status": "enabled"})
            except HBSDRestClientError as e:
                LOG.error("Failback resync failed for volume %s: %s", volume.id, e)
                vol_models.append({"id": volume.id, "replication_status": "error"})
        return primary_backend_id, vol_models

    def _wait_for_pair_status(
        self, client, copy_group, copy_pair_name, expected_status, timeout=3600
    ):
        """
        Poll a UR pair until it reaches expected status.

        Args:
            client: REST client for local or remote array
            copy_group: Copy group name
            copy_pair_name: Copy pair name
            expected_status: Status to wait for (e.g., "PAIR")
            timeout: Maximum wait time in seconds

        Raises:
            HBSDRestClientError: If timeout or status check fails
        """
        deadline = time.time() + timeout
        interval = 5

        while time.time() < deadline:
            try:
                result = client.request(
                    "GET",
                    f"/v1/objects/remote-mirror-copypairs?copyGroupName={copy_group}&copyPairName={copy_pair_name}",
                )
                pairs = (result or {}).get("data", [])
                if pairs:
                    pair_status = pairs[0].get("pvolStatus")
                    if pair_status == expected_status:
                        LOG.info(
                            "Pair %s reached status %s", copy_pair_name, expected_status
                        )
                        return
                    LOG.debug(
                        "Pair %s status: %s (waiting for %s)",
                        copy_pair_name,
                        pair_status,
                        expected_status,
                    )
                time.sleep(interval)
            except HBSDRestClientError as e:
                LOG.warning("Error checking pair status: %s", e)
                time.sleep(interval)

        raise exception.VolumeDriverException(
            reason=f"Pair {copy_pair_name} did not reach {expected_status} "
            f"status within {timeout}s"
        )

    def list_replication_targets(self, context, group):
        """
        List replication targets (secondary backends) for a group.

        Queries copy groups matching this group ID and extracts the
        remoteStorageDeviceId from each. Filters by replication state.

        Args:
            context: Cinder request context
            group: Group object

        Returns:
            Dict with 'replication_targets' list
        """
        try:
            client = self._local_cm_client()
            result = client.request("GET", "/v1/objects/remote-mirror-copygroups")
            data = (result or {}).get("data", [])

            targets = []
            valid_pair_states = ("PAIR", "COPY", "SSWS", "PSUS", "PSUE")

            for item in data:
                cg_name = item.get("copyGroupName")
                if cg_name != group.id[:31]:
                    continue

                remote_id = item.get("remoteStorageDeviceId")
                if not remote_id:
                    LOG.debug("Copy group %s has no remote storage ID", cg_name)
                    continue

                pair_status = item.get("pvolStatus", "")
                if pair_status not in valid_pair_states:
                    LOG.debug(
                        "Skipping target with invalid pair status %s", pair_status
                    )
                    continue

                is_replicating = item.get("isReplicationEnabled", False)
                if not is_replicating:
                    LOG.debug("Skipping target with replication disabled")
                    continue

                targets.append({"backend_id": remote_id, "pair_status": pair_status})
                LOG.info(
                    "Added replication target %s (status=%s)", remote_id, pair_status
                )

            if not targets:
                LOG.warning("No valid replication targets found for group %s", group.id)

            LOG.info(
                "Found %d replication target(s) for group %s", len(targets), group.id
            )
            return {"replication_targets": targets}

        except HBSDRestClientError as e:
            LOG.error(
                "Failed to list replication targets for group %s: %s", group.id, e
            )
            raise exception.VolumeDriverException(
                reason=f"Failed to query replication targets: {e}"
            )

    def get_replication_lag(self, context, group):
        """
        Query the replication lag (consistency time) for a group.

        Returns the time in seconds since the last successful replication
        point on the pair. This is used by PSR's ReplicationMonitor to
        track RPO (Recovery Point Objective) and alert on drift.

        Args:
            context: Cinder request context
            group: Group object

        Returns:
            Dict with 'lag_seconds' (int) and 'pair_status' or error dict
            Example: {"lag_seconds": 42, "pair_status": "PAIR"}
        """
        try:
            client = self._local_cm_client()
            result = client.request(
                "GET",
                f"/v1/objects/remote-mirror-copygroups?copyGroupName={group.id[:31]}",
            )
            data = (result or {}).get("data", [])

            if not data:
                LOG.warning(
                    "Replication lag query: no copy group found for %s", group.id
                )
                return {"lag_seconds": None, "error": "Copy group not found"}

            group_info = data[0]
            # consistencyTime is time in seconds since last successful replication
            lag_seconds = group_info.get("consistencyTime")
            pair_status = group_info.get("pvolStatus", "unknown")
            is_replicating = group_info.get("isReplicationEnabled", False)

            if lag_seconds is None:
                LOG.warning(
                    "Replication lag: consistencyTime not available for group %s "
                    "(status=%s, replicating=%s)",
                    group.id,
                    pair_status,
                    is_replicating,
                )
                return {
                    "lag_seconds": None,
                    "pair_status": pair_status,
                    "replication_enabled": is_replicating,
                }

            LOG.debug(
                "Replication lag for group %s: %d seconds (status=%s)",
                group.id,
                lag_seconds,
                pair_status,
            )
            return {
                "lag_seconds": lag_seconds,
                "pair_status": pair_status,
                "replication_enabled": is_replicating,
            }

        except HBSDRestClientError as e:
            LOG.error("Failed to query replication lag for group %s: %s", group.id, e)
            return {"lag_seconds": None, "error": str(e)}

    def update_group(self, context, group, add_volumes=None, remove_volumes=None):
        """
        Add or remove volumes from an existing consistency group live.

        Adds new copy pairs without recreating the CG (isNewGroupCreation=False),
        and removes individual pairs without tearing down the CG.

        Args:
            context: Cinder request context
            group: Group object
            add_volumes: List of volumes to add to the group
            remove_volumes: List of volumes to remove from the group

        Returns:
            Tuple (group_model, updated_vol_models, updated_snapshot_models)
        """
        client = self._local_cm_client()
        rd = self._replication_device()

        if add_volumes or remove_volumes:
            existing_cg = client.request(
                "GET",
                f"/v1/objects/remote-mirror-copygroups?copyGroupName={group.id[:31]}",
            )
            if not existing_cg or not existing_cg.get("data"):
                raise exception.VolumeDriverException(
                    reason=f"Consistency group {group.id} does not exist on array. "
                    f"Use enable_replication to create the group first."
                )

        for volume in add_volumes or []:
            try:
                _, _, local_dg, remote_dg, copy_pair_name = self._copy_pair_key(
                    group.id, volume.id
                )

                body = {
                    "replicationType": "UR",
                    "copyGroupName": group.id[:31],
                    "copyPairName": copy_pair_name,
                    "localDeviceGroupName": local_dg,
                    "remoteDeviceGroupName": remote_dg,
                    "remoteStorageDeviceId": rd.get("storage_id", ""),
                    "pvolLdevId": self._ldev_id_for(volume),
                    "svolLdevId": self._svol_ldev_id_for(volume, rd),
                    "isNewGroupCreation": False,  # Join existing CG
                    "doInitialCopy": True,
                    "fenceLevel": "ASYNC",
                }

                LOG.info("Adding volume %s to group %s", volume.id, group.id)
                client.request(
                    "POST", "/v1/objects/remote-mirror-copypairs", json_body=body
                )
            except HBSDRestClientError as e:
                LOG.error("Failed to add volume %s: %s", volume.id, e)
                raise exception.VolumeDriverException(reason=str(e))

        # Remove volumes from the CG
        for volume in remove_volumes or []:
            try:
                key = self._copy_pair_key(group.id, volume.id)
                path = "/v1/objects/remote-mirror-copypairs/" + ",".join(
                    str(k) for k in key
                )

                LOG.info("Removing volume %s from group %s", volume.id, group.id)
                client.request("DELETE", path)
            except HBSDRestClientError as e:
                LOG.error("Failed to remove volume %s: %s", volume.id, e)
                raise exception.VolumeDriverException(reason=str(e))

        return {"status": "available"}, None, None

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        """
        Create a crash-consistent snapshot of all S-VOLs in a group.

        Uses Hitachi Thin Image to create per-LDEV snapshots without
        disrupting the UR pair state. Snapshots are taken on the SECONDARY
        array (S-VOL) to avoid impacting replication.

        Args:
            context: Cinder request context
            group_snapshot: GroupSnapshot object
            snapshots: List of Snapshot objects

        Returns:
            Tuple (snapshot_model, snapshot_volume_models)
        """
        # Use secondary array client for test-recovery snapshots
        client = self._remote_cm_client()
        snap_models = []
        rd = self._replication_device()

        auto_split = self.configuration.safe_get("hitachi_snapshot_auto_split", True)
        group_name = group_snapshot.id[:32]

        try:
            existing_snap = client.request(
                "GET", f"/v1/objects/snapshots?snapshotGroupName={group_name}"
            )
            if existing_snap and existing_snap.get("data"):
                raise exception.VolumeDriverException(
                    reason=f"Snapshot group {group_name} already exists on secondary"
                )
        except HBSDRestClientError as e:
            LOG.warning("Could not check for existing snapshot group: %s", e)

        for snap in snapshots or []:
            try:
                # Get the secondary (S-VOL) LDEV ID for test recovery snapshots
                svol_ldev = self._svol_ldev_id_for(snap.volume, rd)

                body = {
                    "pvolLdevId": svol_ldev,  # Snapshot the secondary S-VOL, not primary P-VOL
                    "snapshotGroupName": group_name,
                    "autoSplit": auto_split,
                }

                LOG.info(
                    "Creating snapshot for secondary S-VOL LDEV %d (volume %s, autoSplit=%s)",
                    svol_ldev,
                    snap.volume.id,
                    auto_split,
                )
                client.request("POST", "/v1/objects/snapshots", json_body=body)
                snap_models.append({"id": snap.id, "status": "available"})
            except HBSDRestClientError as e:
                LOG.error("Failed to create snapshot for %s: %s", snap.id, e)
                snap_models.append({"id": snap.id, "status": "error"})
            except exception.VolumeDriverException as e:
                LOG.error(
                    "Failed to allocate secondary LDEV for snapshot of %s: %s",
                    snap.volume.id,
                    e,
                )
                snap_models.append({"id": snap.id, "status": "error"})

        return {"status": "available"}, snap_models

    def manage_existing(self, volume, existing_ref):
        """
        Manage an existing LDEV as a Cinder volume.

        Allow adoption of promoted S-VOL (still paired, now writable) on
        the secondary/recovery site. Validates the LDEV and presents it to the
        recovery host group. Does NOT modify the UR pair (pair remains active).

        Args:
            volume: Volume object (from Cinder)
            existing_ref: Dict with 'source-name' or 'source-id' = LDEV ID

        Returns:
            Dict with 'provider_location' and 'replication_driver_data'
        """
        client = self._remote_cm_client()
        ldev_id = existing_ref.get("source-name") or existing_ref.get("source-id")

        if not ldev_id:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason="source-name or source-id required"
            )

        try:
            rd = self._replication_device()

            ldev = client.request("GET", f"/v1/objects/ldevs/{ldev_id}")
            if not ldev:
                raise exception.ManageExistingInvalidReference(
                    existing_ref=existing_ref,
                    reason=f"LDEV {ldev_id} not found on secondary array",
                )

            if ldev.get("status") == "BLK":
                raise exception.ManageExistingInvalidReference(
                    existing_ref=existing_ref,
                    reason=f"LDEV {ldev_id} is blocked on secondary array",
                )

            target_ports = rd.get("target_ports") or []
            remote_hg = int(rd.get("target_number") or 0)

            client.request(
                "POST",
                "/v1/objects/luns",
                json_body={
                    "ldevId": int(ldev_id),
                    "hostGroupNumber": remote_hg,
                    "portIds": target_ports[:6] if target_ports else None,
                },
            )

            LOG.info(
                "Managed promoted S-VOL LDEV %s as volume %s on recovery site",
                ldev_id,
                volume.id,
            )

            replication_data = {
                "copy_group": ldev.get("copyGroupName", ""),
                "ldev_id": ldev_id,
                "pair_status": ldev.get("pairStatus", "unknown"),
                "timestamp": time.time(),
                "array": "secondary",
            }

            return {
                "provider_location": str(ldev_id),
                "replication_driver_data": json.dumps(replication_data),
            }
        except HBSDRestClientError as e:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason=f"Failed to manage LDEV {ldev_id} on secondary array: {e}",
            )

    def manage_existing_get_size(self, volume, existing_ref):
        """
        Get the size of an existing LDEV to be imported.

        Args:
            volume: Volume object
            existing_ref: Dict with 'source-name' or 'source-id'

        Returns:
            Size in GiB (minimum 1)
        """
        client = self._local_cm_client()
        ldev_id = existing_ref.get("source-name") or existing_ref.get("source-id")

        try:
            ldev = client.request("GET", f"/v1/objects/ldevs/{ldev_id}")
            blocks = ldev.get("blockCapacity", 0)
            gib = (blocks * 512) / (1024**3)
            return max(1, int(gib + 0.999))  # Round up, min 1 GiB
        except HBSDRestClientError as e:
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref,
                reason=f"Could not get LDEV {ldev_id} size: {e}",
            )

    def unmanage(self, volume):
        """
        Unmanage a volume (remove from Cinder DB only).

        Release Cinder's claim on a volume without modifying the
        LDEV or its UR pair. Used during failback to unpair from primary
        before re-enabling replication in the opposite direction.

        Args:
            volume: Volume object

        Returns:
            None (Cinder DB deletion is handled by the base class)
        """
        LOG.info("Unmanaged volume %s (LDEV and UR pair untouched)", volume.id)

    def _ldev_id_for(self, volume) -> int:
        """
        Extract LDEV ID from volume's provider_location.

        The upstream hbsd_* driver sets provider_location to the LDEV ID
        by convention during create_volume or manage_existing.
        """
        try:
            return int(volume.provider_location)
        except (TypeError, ValueError):
            raise exception.VolumeDriverException(
                reason=f"Invalid provider_location: {volume.provider_location}"
            )

    def _svol_ldev_id_for(self, volume, replication_device: dict) -> int:
        """
        Allocate or derive a secondary LDEV ID for the given volume.

        Queries available LDEVs from the secondary array's configured range,
        matches size with the primary volume, and returns an allocated LDEV ID.

        Args:
            volume: Volume object (has size, id, provider_location)
            replication_device: Replication device config dict with 'ldev_range' and 'pool'

        Returns:
            Allocated secondary LDEV ID (integer)

        Raises:
            VolumeDriverException: If no available LDEV found matching size/range
        """
        client = self._remote_cm_client()
        ldev_range = replication_device.get("ldev_range", "5000-6000")
        pool_id = replication_device.get("pool", "0")

        try:
            start, end = map(int, ldev_range.split("-"))
        except (ValueError, AttributeError) as e:
            raise exception.VolumeDriverException(
                reason=f"Invalid ldev_range format '{ldev_range}': {e}"
            )

        volume_blocks = int(volume.size * (1024**3) / 512)

        LOG.debug(
            "Searching for available LDEV in range %d-%d matching %d blocks",
            start,
            end,
            volume_blocks,
        )

        try:
            result = (
                client.request(
                    "GET",
                    f"/v1/objects/ldevs?poolId={pool_id}&status=NML&blockCapacity={volume_blocks}",
                )
                or {}
            )

            available_ldevs = result.get("data", [])
            for ldev in available_ldevs:
                ldev_id = ldev.get("ldevId")
                if start <= ldev_id <= end:
                    LOG.info(
                        "Allocated LDEV %d for secondary volume %s", ldev_id, volume.id
                    )
                    return int(ldev_id)

            raise exception.VolumeDriverException(
                reason=f"No available LDEV in range {ldev_range} with {volume.size}GB "
                f"in pool {pool_id}"
            )

        except HBSDRestClientError as e:
            raise exception.VolumeDriverException(
                reason=f"Failed to query available LDEVs for secondary allocation: {e}"
            )


@interface.volumedriver
class HBSDGroupReplicationFCDriver(HBSDGroupReplicationMixin, hbsd_fc.HBSDFCDriver):
    """
    Hitachi VSP Remote Replication Driver for Fibre Channel.

    Implements Cinder group-replication actions for UR replication.

    Use this driver if your Hitachi backend is configured for FC transport.
    MRO puts the mixin first, so its __init__ and _update_volume_stats run
    and cooperatively call super() into HBSDFCDriver for FC setup and stats.

    Transport-specific methods (initialize_connection, terminate_connection, etc.)
    are inherited directly from HBSDFCDriver and work without modification.

    Configuration (cinder.conf):
        [hitachi_vsp_fc]
        volume_driver = cinder.volume.drivers.pf9_hitachi.pf9_hitachi_rsync.HBSDGroupReplicationFCDriver
        san_ip = <primary-cm-ip>
        san_login = system
        san_password = <password>
        replication_device = backend_id:hitachi-vsp-secondary,san_ip:<secondary-cm-ip>,...
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_FC"


@interface.volumedriver
class HBSDGroupReplicationISCSIDriver(
    HBSDGroupReplicationMixin, hbsd_iscsi.HBSDISCSIDriver
):
    """
    Hitachi VSP Remote Replication Driver for iSCSI.

    Identical group-replication behavior to the FC variant above — same
    mixin, same Configuration Manager REST calls. Transport-specific methods
    (iSCSI target/portal handling) come from HBSDISCSIDriver instead.

    Use this driver if your Hitachi backend is configured for iSCSI transport.

    Configuration (cinder.conf):
        [hitachi_vsp_iscsi]
        volume_driver = cinder.volume.drivers.pf9_hitachi.pf9_hitachi_rsync.HBSDGroupReplicationISCSIDriver
        san_ip = <primary-cm-ip>
        san_login = system
        san_password = <password>
        replication_device = backend_id:hitachi-vsp-secondary,san_ip:<secondary-cm-ip>,...
    """

    VERSION = "1.0.0"
    CI_WIKI_NAME = "Hitachi_VSP_Extended_Replication_ISCSI"

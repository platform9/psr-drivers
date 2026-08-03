"""
Secondary LDEV Allocator for Hitachi VSP Arrays

This module handles allocation of secondary LDEVs from a configured pool range
on the secondary (target) Hitachi VSP array during replication pair creation.
"""

from cinder import exception


class SecondaryLDEVAllocator:
    """
    Allocate secondary LDEVs from a reserved pool range.

    This is a stub implementation. Customize based on your array pool management:
    - Query available LDEVs in the configured range
    - Match size with primary LDEV
    - Track allocations per volume
    - Handle exhausted pools
    """

    def __init__(self, cm_rest_client, pool_id: str, ldev_range: str):
        """
        Initialize the allocator.

        Args:
            cm_rest_client: Hitachi Configuration Manager REST client
            pool_id: Logical pool ID on secondary array
            ldev_range: Range string like "5000-6000"
        """
        self.client = cm_rest_client
        self.pool_id = pool_id
        self.ldev_range = self._parse_range(ldev_range)
        self._allocated = set()

    def _parse_range(self, range_str: str):
        """Parse '5000-6000' to Python range(5000, 6001)."""
        try:
            start, end = range_str.split("-")
            return range(int(start), int(end) + 1)
        except ValueError:
            raise ValueError(f"Invalid LDEV range format: {range_str}. Use 'start-end'")

    def allocate(self, size_gb: int) -> int:
        """
        Find and allocate a free LDEV matching the requested size.

        Args:
            size_gb: Requested size in GiB

        Returns:
            Allocated LDEV ID

        Raises:
            Exception: If no free LDEVs available in range
        """
        volume_blocks = int(size_gb * (1024**3) / 512)

        result = (
            self.client.request(
                "GET",
                f"/v1/objects/ldevs?poolId={self.pool_id}&status=NML&blockCapacity={volume_blocks}",
            )
            or {}
        )

        available_ldevs = result.get("data", [])
        for ldev in available_ldevs:
            ldev_id = ldev.get("ldevId")
            if ldev_id not in self._allocated and ldev_id in self.ldev_range:
                self._allocated.add(ldev_id)
                return int(ldev_id)

        raise exception.VolumeDriverException(
            reason=f"No available LDEV in range {self.ldev_range.start}-{self.ldev_range.stop - 1} "
            f"with {size_gb}GB in pool {self.pool_id}"
        )

# psr-drivers
PSR Cinder custom drivers to be extended from PF9's Cinder drivers

# Hitachi driver

Details of Hitachi custom drivers are documented [here](/psr-drivers/pf9_hitachi/README.md).

# NFS + rsync driver

An NFS-backed driver that emulates a replication array (rsync between two exports)
so PSR's full DR flow runs with no storage hardware. Documented
[here](/psr-drivers/pf9_nfs_rsync/README.md).

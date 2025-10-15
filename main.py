#!/usr/bin/env python3
"""
CUPCAKE Agent - Node-local upgrade execution
Runs on each node to perform local upgrade operations
"""
import os
import sys
import time
import json
import logging
import subprocess
import shutil
from datetime import datetime, timezone
from pathlib import Path
from kubernetes import client, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
NODE_NAME = os.getenv('NODE_NAME')
NAMESPACE = os.getenv('NAMESPACE', 'kube-system')
HOSTPATH_ROOT = os.getenv('HOSTPATH_ROOT', '/var/lib/cupcake')
BACKUP_STORE_ENABLED = os.getenv('BACKUP_STORE_ENABLED', 'false').lower() == 'true'
RECONCILE_INTERVAL = int(os.getenv('RECONCILE_INTERVAL', '30').rstrip('s'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'info').upper()

# Set log level
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


class UpdateAgent:
    """Main agent class"""
    
    def __init__(self):
        self.node_name = NODE_NAME
        self.namespace = NAMESPACE
        self.hostpath_root = Path(HOSTPATH_ROOT)
        self.current_operation = None
        
        # Ensure hostpath exists
        self.hostpath_root.mkdir(parents=True, exist_ok=True)
        
        # Load Kubernetes config
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded kubeconfig configuration")
            except config.ConfigException:
                logger.error("Could not load Kubernetes configuration")
                sys.exit(1)
        
        self.v1 = client.CoreV1Api()
        
        logger.info(f"Agent initialized for node: {self.node_name}")
        logger.info(f"HostPath root: {self.hostpath_root}")
    
    def run(self):
        """Main agent loop"""
        logger.info("Agent starting main loop")
        
        # Resume any incomplete operations on startup
        self.resume_operations()
        
        # Main reconciliation loop
        while True:
            try:
                self.reconcile()
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {e}", exc_info=True)
            
            time.sleep(RECONCILE_INTERVAL)
    
    def resume_operations(self):
        """Resume any incomplete operations found in hostpath"""
        logger.info("Checking for incomplete operations to resume")
        
        for entry in self.hostpath_root.iterdir():
            if entry.is_dir() and entry.name.startswith('operation-'):
                operation_id = entry.name.replace('operation-', '')
                logger.info(f"Found operation directory: {operation_id}")
                
                # Check if operation is incomplete
                if not (entry / 'completed').exists() and not (entry / 'failed').exists():
                    logger.info(f"Resuming incomplete operation: {operation_id}")
                    self.process_operation(operation_id, resume=True)
    
    def reconcile(self):
        """Check node annotations for new work"""
        try:
            # Read node annotations
            node = self.v1.read_node(self.node_name)
            annotations = node.metadata.annotations or {}
            
            operation_id = annotations.get('cupcake.ricardomolendijk.com/operation-id')
            status = annotations.get('cupcake.ricardomolendijk.com/status', 'pending')
            
            if operation_id and status == 'pending':
                logger.info(f"Found pending operation: {operation_id}")
                
                # Extract upgrade details
                target_version = annotations.get('cupcake.ricardomolendijk.com/target-version')
                components = annotations.get('cupcake.ricardomolendijk.com/components', '').split(',')
                
                # Process the operation
                self.process_operation(operation_id, target_version, components)
            
            # Check for backup requests
            self.check_backup_requests()
            
        except Exception as e:
            logger.error(f"Error in reconcile: {e}")
    
    def process_operation(self, operation_id, target_version=None, components=None, resume=False):
        """Process an upgrade operation"""
        operation_dir = self.hostpath_root / f'operation-{operation_id}'
        operation_dir.mkdir(exist_ok=True)
        
        logs_dir = operation_dir / 'logs'
        logs_dir.mkdir(exist_ok=True)
        
        # Save operation metadata
        if not resume:
            metadata = {
                'operation_id': operation_id,
                'target_version': target_version,
                'components': components,
                'node_name': self.node_name,
                'started_at': datetime.now(timezone.utc).isoformat()
            }
            
            with open(operation_dir / 'metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)
        else:
            # Load existing metadata
            with open(operation_dir / 'metadata.json', 'r') as f:
                metadata = json.load(f)
            target_version = metadata.get('target_version')
            components = metadata.get('components', [])
        
        logger.info(f"Processing operation {operation_id} for node {self.node_name}")
        logger.info(f"Target version: {target_version}, Components: {components}")
        
        # Define upgrade steps
        steps = self.get_upgrade_steps(target_version, components)
        
        # Execute steps
        for i, step in enumerate(steps, start=1):
            step_name = step['name']
            step_file = operation_dir / f'step-{i:02d}-{step_name}.done'
            
            if step_file.exists():
                logger.info(f"Step {i} ({step_name}) already completed, skipping")
                continue
            
            # Mark step as in progress
            in_progress_file = operation_dir / f'step-{i:02d}-{step_name}.inprogress'
            with open(in_progress_file, 'w') as f:
                json.dump({
                    'step': i,
                    'name': step_name,
                    'started_at': datetime.now(timezone.utc).isoformat()
                }, f)
            
            logger.info(f"Executing step {i}: {step_name}")
            
            try:
                # Execute step function
                step_func = step['func']
                step_func(operation_dir, logs_dir, metadata)
                
                # Mark step as done
                in_progress_file.rename(step_file)
                
                logger.info(f"Step {i} ({step_name}) completed successfully")
                
            except Exception as e:
                logger.error(f"Step {i} ({step_name}) failed: {e}")
                
                # Mark operation as failed
                with open(operation_dir / 'failed', 'w') as f:
                    json.dump({
                        'step': i,
                        'name': step_name,
                        'error': str(e),
                        'failed_at': datetime.now(timezone.utc).isoformat()
                    }, f)
                
                # Update node annotation
                self.update_node_annotation('cupcake.ricardomolendijk.com/status', 'failed')
                return
        
        # Mark operation as completed
        with open(operation_dir / 'completed', 'w') as f:
            json.dump({
                'completed_at': datetime.now(timezone.utc).isoformat(),
                'node_name': self.node_name
            }, f)
        
        # Update node annotation
        self.update_node_annotation('cupcake.ricardomolendijk.com/status', 'completed')
        
        logger.info(f"Operation {operation_id} completed successfully")
    
    def get_upgrade_steps(self, target_version, components):
        """Define the sequence of upgrade steps"""
        steps = []
        
        # Check if this is a control-plane node
        is_control_plane = self.is_control_plane_node()
        
        if is_control_plane:
            steps.extend([
                {'name': 'download-packages', 'func': self.step_download_packages},
                {'name': 'upgrade-kubeadm', 'func': self.step_upgrade_kubeadm},
                {'name': 'kubeadm-upgrade', 'func': self.step_kubeadm_upgrade},
                {'name': 'upgrade-kubelet', 'func': self.step_upgrade_kubelet}
            ])
            
            # Add containerd upgrade if requested
            if 'containerd' in components:
                steps.append({'name': 'upgrade-containerd', 'func': self.step_upgrade_containerd})
            
            steps.extend([
                {'name': 'restart-kubelet', 'func': self.step_restart_kubelet},
                {'name': 'verify-node', 'func': self.step_verify_node}
            ])
        else:
            # Worker node steps
            steps.extend([
                {'name': 'download-packages', 'func': self.step_download_packages},
                {'name': 'drain-node', 'func': self.step_drain_node},
                {'name': 'upgrade-kubeadm', 'func': self.step_upgrade_kubeadm},
                {'name': 'kubeadm-upgrade-node', 'func': self.step_kubeadm_upgrade_node},
                {'name': 'upgrade-kubelet', 'func': self.step_upgrade_kubelet}
            ])
            
            # Add containerd upgrade if requested
            if 'containerd' in components:
                steps.append({'name': 'upgrade-containerd', 'func': self.step_upgrade_containerd})
            
            steps.extend([
                {'name': 'restart-kubelet', 'func': self.step_restart_kubelet},
                {'name': 'verify-node', 'func': self.step_verify_node},
                {'name': 'uncordon-node', 'func': self.step_uncordon_node}
            ])
        
        return steps
    
    def is_control_plane_node(self):
        """Check if current node is a control-plane node"""
        try:
            node = self.v1.read_node(self.node_name)
            labels = node.metadata.labels or {}
            
            return (
                labels.get('node-role.kubernetes.io/control-plane') is not None or
                labels.get('node-role.kubernetes.io/master') is not None
            )
        except Exception as e:
            logger.error(f"Failed to check node role: {e}")
            return False
    
    def step_download_packages(self, operation_dir, logs_dir, metadata):
        """Download/prepare upgrade packages"""
        target_version = metadata.get('target_version')
        logger.info(f"Downloading packages for Kubernetes {target_version}")
        
        log_file = logs_dir / 'download-packages.log'
        
        try:
            # Detect package manager
            if shutil.which('apt-get'):
                self._download_packages_apt(target_version, log_file)
            elif shutil.which('yum'):
                self._download_packages_yum(target_version, log_file)
            else:
                raise Exception("No supported package manager found (apt-get or yum)")
                
        except Exception as e:
            logger.error(f"Failed to download packages: {e}")
            raise
    
    def _download_packages_apt(self, target_version, log_file):
        """Download packages using apt (Debian/Ubuntu)"""
        logger.info("Using apt package manager")
        
        # Update package cache
        cmd = ['apt-get', 'update']
        with open(log_file, 'w') as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
        
        # Download packages without installing
        version_suffix = f"={target_version}-00"
        packages = [
            f"kubeadm{version_suffix}",
            f"kubelet{version_suffix}",
            f"kubectl{version_suffix}"
        ]
        
        cmd = ['apt-get', 'download'] + packages
        with open(log_file, 'a') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
            
        logger.info("Packages downloaded successfully")
    
    def _download_packages_yum(self, target_version, log_file):
        """Download packages using yum (RHEL/CentOS)"""
        logger.info("Using yum package manager")
        
        version_suffix = f"-{target_version}-0"
        packages = [
            f"kubeadm{version_suffix}",
            f"kubelet{version_suffix}",
            f"kubectl{version_suffix}"
        ]
        
        cmd = ['yum', 'install', '--downloadonly', '-y'] + packages
        with open(log_file, 'w') as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
        logger.info("Packages downloaded successfully")
    
    def step_upgrade_kubeadm(self, operation_dir, logs_dir, metadata):
        """Upgrade kubeadm binary"""
        target_version = metadata.get('target_version')
        logger.info(f"Upgrading kubeadm to {target_version}")
        
        log_file = logs_dir / 'upgrade-kubeadm.log'
        
        try:
            if shutil.which('apt-get'):
                cmd = ['apt-get', 'install', '-y', '--allow-change-held-packages',
                       f"kubeadm={target_version}-00"]
            elif shutil.which('yum'):
                cmd = ['yum', 'install', '-y', f"kubeadm-{target_version}-0"]
            else:
                raise Exception("No supported package manager found")
            
            with open(log_file, 'w') as f:
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
            # Verify kubeadm version
            result = subprocess.run(['kubeadm', 'version', '-o', 'short'],
                                  capture_output=True, text=True, check=True)
            logger.info(f"Kubeadm upgraded: {result.stdout.strip()}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upgrade kubeadm: {e}")
            raise
    
    def step_kubeadm_upgrade(self, operation_dir, logs_dir, metadata):
        """Run kubeadm upgrade apply (control-plane)"""
        target_version = metadata.get('target_version')
        logger.info(f"Running kubeadm upgrade apply v{target_version}")
        
        log_file = logs_dir / 'kubeadm-upgrade.log'
        
        try:
            # First, check current plan
            cmd = ['kubeadm', 'upgrade', 'plan', f"v{target_version}"]
            with open(log_file, 'w') as f:
                f.write(f"=== Upgrade Plan ===\n")
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
            
            # Apply the upgrade
            cmd = ['kubeadm', 'upgrade', 'apply', f"v{target_version}", '-y', '--force']
            with open(log_file, 'a') as f:
                f.write(f"\n=== Applying Upgrade ===\n")
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
            logger.info(f"Kubeadm upgrade apply completed successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to run kubeadm upgrade apply: {e}")
            raise
    
    def step_kubeadm_upgrade_node(self, operation_dir, logs_dir, metadata):
        """Run kubeadm upgrade node (worker)"""
        logger.info("Running kubeadm upgrade node")
        
        log_file = logs_dir / 'kubeadm-upgrade-node.log'
        
        try:
            cmd = ['kubeadm', 'upgrade', 'node']
            with open(log_file, 'w') as f:
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
            logger.info("Kubeadm upgrade node completed successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to run kubeadm upgrade node: {e}")
            raise
    
    def step_upgrade_kubelet(self, operation_dir, logs_dir, metadata):
        """Upgrade kubelet and kubectl"""
        target_version = metadata.get('target_version')
        logger.info(f"Upgrading kubelet and kubectl to {target_version}")
        
        log_file = logs_dir / 'upgrade-kubelet.log'
        
        try:
            if shutil.which('apt-get'):
                cmd = ['apt-get', 'install', '-y', '--allow-change-held-packages',
                       f"kubelet={target_version}-00",
                       f"kubectl={target_version}-00"]
            elif shutil.which('yum'):
                cmd = ['yum', 'install', '-y',
                       f"kubelet-{target_version}-0",
                       f"kubectl-{target_version}-0"]
            else:
                raise Exception("No supported package manager found")
            
            with open(log_file, 'w') as f:
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
            # Verify versions
            result = subprocess.run(['kubelet', '--version'],
                                  capture_output=True, text=True, check=True)
            logger.info(f"Kubelet upgraded: {result.stdout.strip()}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upgrade kubelet: {e}")
            raise
    
    def step_restart_kubelet(self, operation_dir, logs_dir, metadata):
        """Restart kubelet service"""
        logger.info("Restarting kubelet")
        
        log_file = logs_dir / 'restart-kubelet.log'
        
        try:
            with open(log_file, 'w') as f:
                # Reload systemd
                subprocess.run(['systemctl', 'daemon-reload'], 
                             stdout=f, stderr=subprocess.STDOUT, check=True)
                
                # Restart kubelet
                subprocess.run(['systemctl', 'restart', 'kubelet'], 
                             stdout=f, stderr=subprocess.STDOUT, check=True)
            
            # Wait for kubelet to stabilize
            time.sleep(15)
            
            # Check kubelet status
            result = subprocess.run(['systemctl', 'is-active', 'kubelet'],
                                  capture_output=True, text=True)
            
            if result.stdout.strip() == 'active':
                logger.info("Kubelet restarted successfully")
            else:
                raise Exception(f"Kubelet not active: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restart kubelet: {e}")
            raise
    
    def step_upgrade_containerd(self, operation_dir, logs_dir, metadata):
        """Upgrade containerd runtime"""
        logger.info("Upgrading containerd")
        
        log_file = logs_dir / 'upgrade-containerd.log'
        
        try:
            if shutil.which('apt-get'):
                # Debian/Ubuntu
                cmd = ['apt-get', 'install', '-y', '--allow-change-held-packages', 'containerd.io']
            elif shutil.which('yum'):
                # RHEL/CentOS
                cmd = ['yum', 'update', '-y', 'containerd.io']
            else:
                logger.warning("Unknown package manager, skipping containerd upgrade")
                return
            
            with open(log_file, 'w') as f:
                subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
            
            # Restart containerd
            subprocess.run(['systemctl', 'restart', 'containerd'], check=True)
            time.sleep(5)
            
            # Verify containerd is running
            result = subprocess.run(['systemctl', 'is-active', 'containerd'],
                                  capture_output=True, text=True, check=True)
            
            logger.info(f"Containerd upgraded and restarted: {result.stdout.strip()}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upgrade containerd: {e}")
            raise
    
    def step_drain_node(self, operation_dir, logs_dir, metadata):
        """Drain the node"""
        logger.info(f"Draining node {self.node_name}")
        
        try:
            # Use kubectl to drain
            cmd = [
                'kubectl', 'drain', self.node_name,
                '--ignore-daemonsets',
                '--delete-emptydir-data',
                '--timeout=300s'
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Node drained successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to drain node: {e.stderr}")
            raise
    
    def step_uncordon_node(self, operation_dir, logs_dir, metadata):
        """Uncordon the node"""
        logger.info(f"Uncordoning node {self.node_name}")
        
        try:
            cmd = ['kubectl', 'uncordon', self.node_name]
            
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            logger.info(f"Node uncordoned successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to uncordon node: {e.stderr}")
            raise
    
    def step_verify_node(self, operation_dir, logs_dir, metadata):
        """Verify node is ready and healthy"""
        logger.info(f"Verifying node {self.node_name}")
        
        max_wait = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                node = self.v1.read_node(self.node_name)
                
                # Check node conditions
                for condition in node.status.conditions:
                    if condition.type == 'Ready' and condition.status == 'True':
                        logger.info("Node is Ready")
                        return
                
                logger.debug("Node not ready yet, waiting...")
                time.sleep(10)
                
            except Exception as e:
                logger.warning(f"Error checking node status: {e}")
                time.sleep(10)
        
        raise Exception(f"Node did not become Ready within {max_wait} seconds")
    
    def check_backup_requests(self):
        """Check for backup requests via ConfigMaps"""
        if not BACKUP_STORE_ENABLED:
            return
        
        try:
            # List ConfigMaps with backup label for this node
            label_selector = f'cupcake.ricardomolendijk.com/backup=true'
            
            cms = self.v1.list_namespaced_config_map(
                self.namespace,
                label_selector=label_selector
            )
            
            for cm in cms.items:
                # Check if this backup is for our node
                node_name = cm.data.get('node_name')
                if node_name != self.node_name:
                    continue
                
                operation_id = cm.data.get('operation_id')
                snapshot_name = cm.data.get('snapshot_name')
                
                logger.info(f"Processing backup request: {snapshot_name}")
                
                # Perform backup
                self.perform_etcd_backup(operation_id, snapshot_name)
                
                # Delete the request ConfigMap
                self.v1.delete_namespaced_config_map(cm.metadata.name, self.namespace)
                
        except Exception as e:
            logger.error(f"Error checking backup requests: {e}")
    
    def perform_etcd_backup(self, operation_id, snapshot_name):
        """Perform etcd snapshot and upload to backup store"""
        logger.info(f"Taking etcd snapshot: {snapshot_name}")
        
        snapshot_path = self.hostpath_root / f'{snapshot_name}.db'
        
        try:
            # Set etcdctl API version
            env = os.environ.copy()
            env['ETCDCTL_API'] = '3'
            
            # Determine etcd endpoints and certificates
            etcd_endpoints = self._get_etcd_endpoints()
            cert_paths = self._get_etcd_cert_paths()
            
            # Take etcd snapshot
            cmd = [
                'etcdctl', 'snapshot', 'save', str(snapshot_path),
                f'--endpoints={etcd_endpoints}',
                f'--cacert={cert_paths["ca"]}',
                f'--cert={cert_paths["cert"]}',
                f'--key={cert_paths["key"]}'
            ]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            logger.info(f"Snapshot saved: {result.stdout}")
            
            # Verify snapshot
            cmd_verify = [
                'etcdctl', 'snapshot', 'status', str(snapshot_path),
                '--write-out=table'
            ]
            result = subprocess.run(cmd_verify, env=env, capture_output=True, text=True, check=True)
            logger.info(f"Snapshot verified: {result.stdout}")
            
            # Upload to backup store if enabled
            if BACKUP_STORE_ENABLED:
                self.upload_backup(snapshot_path, snapshot_name)
            
            # Create status ConfigMap
            self.create_backup_status(operation_id, snapshot_name, True, str(snapshot_path))
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            self.create_backup_status(operation_id, snapshot_name, False, str(e))
            raise
    
    def _get_etcd_endpoints(self):
        """Get etcd endpoints"""
        # Check for external etcd or stacked etcd
        if os.path.exists('/etc/kubernetes/manifests/etcd.yaml'):
            # Stacked etcd
            return 'https://127.0.0.1:2379'
        else:
            # Try to read from kubeadm config
            return 'https://127.0.0.1:2379'
    
    def _get_etcd_cert_paths(self):
        """Get etcd certificate paths"""
        # Standard kubeadm paths
        base_path = Path('/etc/kubernetes/pki/etcd')
        
        if base_path.exists():
            return {
                'ca': str(base_path / 'ca.crt'),
                'cert': str(base_path / 'server.crt'),
                'key': str(base_path / 'server.key')
            }
        else:
            # Fallback to healthcheck client certs
            base_path = Path('/etc/kubernetes/pki')
            return {
                'ca': str(base_path / 'etcd/ca.crt'),
                'cert': str(base_path / 'apiserver-etcd-client.crt'),
                'key': str(base_path / 'apiserver-etcd-client.key')
            }
    
    def upload_backup(self, local_path, snapshot_name):
        """Upload backup to external store"""
        backup_type = os.getenv('BACKUP_STORE_TYPE', 's3')
        
        if backup_type == 's3':
            self.upload_to_s3(local_path, snapshot_name)
        elif backup_type == 'gcs':
            self.upload_to_gcs(local_path, snapshot_name)
        else:
            logger.warning(f"Unknown backup type: {backup_type}")
    
    def upload_to_s3(self, local_path, snapshot_name):
        """Upload to S3/MinIO"""
        import boto3
        
        bucket = os.getenv('BACKUP_STORE_BUCKET')
        endpoint = os.getenv('BACKUP_STORE_ENDPOINT')
        
        logger.info(f"Uploading to S3 bucket: {bucket}")
        
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint if endpoint else None
        )
        
        s3_client.upload_file(
            str(local_path),
            bucket,
            f'etcd-snapshots/{snapshot_name}.db'
        )
        
        logger.info(f"Upload to S3 completed: {snapshot_name}")
    
    def upload_to_gcs(self, local_path, snapshot_name):
        """Upload to Google Cloud Storage"""
        from google.cloud import storage
        
        bucket_name = os.getenv('BACKUP_STORE_BUCKET')
        
        logger.info(f"Uploading to GCS bucket: {bucket_name}")
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f'etcd-snapshots/{snapshot_name}.db')
        
        blob.upload_from_filename(str(local_path))
        
        logger.info(f"Upload to GCS completed: {snapshot_name}")
    
    def create_backup_status(self, operation_id, snapshot_name, success, message):
        """Create a ConfigMap with backup status"""
        cm_name = f"backup-status-{operation_id}-{self.node_name}".replace('.', '-')
        
        config_map = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=cm_name,
                namespace=self.namespace
            ),
            data={
                'completed': 'true',
                'success': 'true' if success else 'false',
                'message': message,
                'snapshot_name': snapshot_name,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        )
        
        try:
            self.v1.create_namespaced_config_map(self.namespace, config_map)
            logger.info(f"Created backup status ConfigMap: {cm_name}")
        except Exception as e:
            logger.error(f"Failed to create status ConfigMap: {e}")
    
    def update_node_annotation(self, key, value):
        """Update node annotation"""
        try:
            body = {
                "metadata": {
                    "annotations": {
                        key: value
                    }
                }
            }
            
            self.v1.patch_node(self.node_name, body)
            logger.debug(f"Updated node annotation: {key}={value}")
            
        except Exception as e:
            logger.error(f"Failed to update node annotation: {e}")


def main():
    """Main entry point"""
    if not NODE_NAME:
        logger.error("NODE_NAME environment variable not set")
        sys.exit(1)
    
    logger.info(f"Starting CUPCAKE agent for node: {NODE_NAME}")
    
    agent = UpdateAgent()
    agent.run()


if __name__ == '__main__':
    main()

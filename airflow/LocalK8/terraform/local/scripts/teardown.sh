#!/bin/bash
# Graceful teardown script for Airflow on Kind

set -e

echo " Starting graceful teardown of Airflow cluster..."

# Step 1: Remove Helm release first (while cluster is still running)
echo " Removing Helm release..."
if helm list -n airflow | grep -q airflow; then
    helm uninstall airflow -n airflow --wait
    echo "Helm release removed"
else
    echo "ℹ No Helm release found"
fi

# Step 2: Remove Kubernetes resources from Terraform state
echo "  Removing Kubernetes resources from Terraform state..."
terraform state rm kubernetes_namespace.airflow || echo "  Namespace not in state"
terraform state rm helm_release.airflow || echo " Helm release not in state"

# Step 3: Destroy the Kind cluster
echo "  Destroying Kind cluster..."
terraform destroy -auto-approve

# Step 4: Clean up any remaining Kind clusters
echo "🧹 Cleaning up any remaining Kind clusters..."
kind delete cluster --name airflow-local || echo " Cluster already deleted"

echo "Teardown complete!"
echo ""
echo " To deploy again, run:"
echo "   terraform apply -auto-approve" 
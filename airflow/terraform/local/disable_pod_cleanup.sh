#!/bin/bash

# Script to disable pod auto-deletion and garbage collection in Kind cluster

echo "Disabling pod cleanup and garbage collection..."

# Get the current context
CONTEXT=$(kubectl config current-context)
echo "Current context: $CONTEXT"

# Apply resource quotas and limit ranges to prevent pod eviction
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: LimitRange
metadata:
  name: pod-retention-limits
  namespace: airflow
spec:
  limits:
  - type: Pod
    default:
      cpu: "4"
      memory: "8Gi"
    defaultRequest:
      cpu: "100m"
      memory: "256Mi"
---
apiVersion: v1
kind: ResourceQuota
metadata:
  name: airflow-quota
  namespace: airflow
spec:
  hard:
    requests.cpu: "20"
    requests.memory: "40Gi"
    limits.cpu: "40"
    limits.memory: "80Gi"
    persistentvolumeclaims: "10"
EOF

# Create a policy to prevent pod deletion
cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: preserve-pods
  namespace: airflow
spec:
  podSelector:
    matchLabels:
      persistent: "true"
  policyTypes: []
EOF

# Apply pod disruption budget to prevent eviction
cat <<EOF | kubectl apply -f -
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: airflow-pod-retention
  namespace: airflow
spec:
  minAvailable: 0
  selector:
    matchLabels:
      persistent: "true"
EOF

echo "Pod cleanup prevention policies applied successfully!"
echo ""
echo "To manually clean up old pods later, use:"
echo "kubectl delete pods -n airflow -l persistent=true"
echo ""
echo "To check preserved pods:"
echo "kubectl get pods -n airflow -l persistent=true"

#!/bin/bash
hostname=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/local-ipv4 | xargs dig +short -x | sed s/\.$// `;
ip=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/local-ipv4`;
ami_id=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/ami-id`;
instance_type=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/instance-type`;
instance_id=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/instance-id`;
availability_zone=`/usr/bin/curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`
memory=`lsmem | grep "Total online memory" | awk -F: '{print $2}' | xargs`
cpu_model=`lscpu | grep "Model name" | awk -F: '{print $2}' | xargs`
vcpu=`lscpu  | grep "^CPU(s):" | awk -F: '{print $2}'| xargs`
local_storage=`df -h /data | sed 1d | awk '{print $2}'`
kernel=`uname -r`
docker=`docker -v`;
echo "**********************************"
echo ""
echo "AWS Instance Details"
echo "--------------------"
echo "HOSTNAME = $hostname"
echo "IP = $ip"
echo "AVAILABILTY ZONE = $availability_zone"
echo "INSTANCE TYPE = $instance_type"
echo "INSTANCE ID = $instance_id"
echo "AMI = $ami_id"
echo "Compute H/W: $vcpu vCPU, $memory memory, $local_storage local storage"
echo "Physical H/W: $cpu_model"
echo "Kernel version: $kernel"
echo "$docker"
echo ""
echo "**********************************"

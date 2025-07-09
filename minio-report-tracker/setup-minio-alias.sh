#!/bin/bash

echo "Configuring MinIO Alias..."
mc alias set cellbox21 https://minio.cellbox21.mosip.net:9000 admin kEKKnxDnAO --api S3v2
mc alias ls

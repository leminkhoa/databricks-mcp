#!/bin/bash

# Define the image name and tag
IMAGE_NAME="databricks-mcp"
TAG="latest"

# Build the Docker image
docker build -t ${IMAGE_NAME}:${TAG} .

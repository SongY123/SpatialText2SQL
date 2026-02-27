#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKERFILE_PATH="${ROOT_DIR}/docker/Dockerfile"

IMAGE_NAME="${IMAGE_NAME:-spatialtext2sql}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-spatialtext2sql}"
HOST_PORT="${HOST_PORT:-8888}"

IMAGE_REF="${IMAGE_NAME}:${IMAGE_TAG}"

usage() {
  cat <<EOF
Usage: ./docker/docker_service.sh <command>

Commands:
  build   Build Docker image
  run     Run Docker container (remove old container with same name first)
  up      Build image then run container
  stop    Stop and remove container
  logs    Follow container logs

Environment variables:
  IMAGE_NAME      Default: spatialtext2sql
  IMAGE_TAG       Default: latest
  CONTAINER_NAME  Default: spatialtext2sql
  HOST_PORT       Default: 8888
EOF
}

ensure_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker command not found."
    exit 1
  fi
}

build_image() {
  docker build -f "${DOCKERFILE_PATH}" -t "${IMAGE_REF}" "${ROOT_DIR}"
}

stop_container() {
  if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    docker rm "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}

run_container() {
  stop_container
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:8888" \
    "${IMAGE_REF}"
}

show_logs() {
  docker logs -f "${CONTAINER_NAME}"
}

main() {
  ensure_docker

  case "${1:-}" in
    build)
      build_image
      ;;
    run)
      run_container
      ;;
    up)
      build_image
      run_container
      ;;
    stop)
      stop_container
      ;;
    logs)
      show_logs
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "${1:-}"

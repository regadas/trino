#!/bin/bash
set -eox pipefail

get_commit_sha() {
    if [[ -n "${COMMIT_SHA}" ]] ; then
        echo >&2 using COMMIT_SHA variable
        echo "${COMMIT_SHA}"
        return 0
    fi
    if [[ -f .git/refs/heads/master ]]; then
        echo >&2 using .git/refs/heads/master
        head -n1 .git/refs/heads/master
        return 0
    elif [[ -f .git/HEAD ]]; then
        echo >&2 using .git/HEAD
        cat .git/HEAD
        return 0
    else
        echo >&2 "Error: Expected a git repo but couldn't find .git/refs/heads/master"
        exit 2
    fi
}

## Based on https://ghe.spotify.net/action-containers/docker-builder/blob/2c374304be91e1cebcd1778bd0452cf3d2a28f0a/containerfs/usr/bin/entrypoint.sh
## but skipping the build part as that it is done on
SOURCE_DIR="../.."
GCP_PROJECT_ID="spotify-trino"
ORIGINAL_IMAGE_NAME="trino"
NEW_IMAGE_NAME="custom-trino"

## Optional vars
if [ -z "$TRINO_VERSION" ]; then
    TRINO_VERSION=$("${SOURCE_DIR}/mvnw" -f "${SOURCE_DIR}/pom.xml" --quiet help:evaluate -Dexpression=project.version -DforceStdout)
fi

arch=${ARCH:-amd64}
TAG_PREFIX="${TRINO_VERSION}"
BUILT_IMAGE_TAG="${TAG_PREFIX}-$arch"

if [[ -z "${EXPANDED_IMAGE_TAG}" ]]; then
    sha=$(get_commit_sha | cut -c 1-8)
    timestamp=$(date +%Y%m%d%H%M%S)
    EXPANDED_IMAGE_TAG="${BUILT_IMAGE_TAG}-${timestamp}-${sha}"
fi

full_image_name="${IMAGE_REGISTRY:-"eu.gcr.io"}/${GCP_PROJECT_ID}/${NEW_IMAGE_NAME}"
full_image_name_with_tag="${full_image_name}:${EXPANDED_IMAGE_TAG}"

echo "🏷️ Retagging: ${ORIGINAL_IMAGE_NAME}:${BUILT_IMAGE_TAG} as ${full_image_name_with_tag}"
docker tag ${ORIGINAL_IMAGE_NAME}:${BUILT_IMAGE_TAG} ${full_image_name_with_tag}

echo "🚀 Pusing: ${full_image_name_with_tag}"
docker push "${full_image_name_with_tag}"

docker rmi "${full_image_name_with_tag}"


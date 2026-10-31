# Live-release overlay (master prompt section 30). The ONLY place a live-release manifest enters an image.
#
# Never used by CI, docker compose, deploy/staging/*.sh or the systemd units (tests/deploy static test). Every default
# image (deploy/containers/backend.Dockerfile) carries no manifest and is therefore NOT live-capable:
# shared_lib.core.live_build answers LIVE_NOT_PERMITTED_IN_THIS_BUILD / LIVE_RELEASE_MANIFEST_ABSENT.
#
# Used only after scripts/release/live_release_manifest.py has validated the manifest and two reviewers have pinned
# its SHA-256 in shared_lib/core/live_release_allowlist.py (a reviewed source change). The base image must have been
# built with --build-arg BUILD_REVISION=<the manifest's git_commit>. The build context is a directory holding only
# live-release/manifest.json:
#
#   docker build -f deploy/containers/live-release.Dockerfile \
#     --build-arg BASE_IMAGE=cosmicforge/bot-backend:<release_id>-base -t cosmicforge/bot-backend:<release_id> <ctx>
#
# A live-capable image authorizes nothing by itself: the go-live gate's other 16 conditions, the operator's
# LIVE_ORDER_SUBMISSION_ENABLED and every transport gate still apply.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY live-release/manifest.json /etc/cosmicforge/live-release/manifest.json
RUN chown -R root:root /etc/cosmicforge/live-release && chmod 0555 /etc/cosmicforge/live-release \
 && chmod 0444 /etc/cosmicforge/live-release/manifest.json
USER cosmicforge

# CI failure drill only (never deployed anywhere else): the real bot-backend image of this commit, serving a process
# that never becomes ready. deploy.sh must reject it and roll the stack back to the previous tag on its own.
#
#   docker build -f deploy/staging/drill/unready-release.Dockerfile \
#     --build-arg BASE_IMAGE=cosmicforge/bot-backend:<tag> -t cosmicforge/bot-backend:<drill-tag> deploy/staging/drill
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY unready_server.py /srv/cosmicforge/drill/unready_server.py
CMD ["python", "/srv/cosmicforge/drill/unready_server.py"]

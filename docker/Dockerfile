# The same shape as strata_exporter's and rooter's Dockerfiles: the Go binary is compiled by the CI
# lane (`go build -o ./entrypoint`) and this file only copies the artifact in. Two reasons, both real:
# the image then ships the exact bytes the tests ran against rather than a second, untested compile,
# and it carries no toolchain, which on a 4-node dev cluster is most of the image.
#
# ubuntu:26.04 rather than scratch/distroless because the container needs a shell for a kubectl-style
# debug session (`kubectl exec -it ... -- sh`) at 03:00 when the harvester has stopped writing rows and
# nobody yet knows why. The cost is a few MB.
FROM ubuntu:26.04

# ca-certificates is not in the base image, and a Go binary built with CGO_ENABLED=0 has no certificate
# roots of its own - it reads the system pool. Measured, not guessed: this image pointed at
# https://rooter.dev.initialed85.cc failed with "certificate signed by unknown authority" until this
# line existed. In cluster everything is plaintext http, so the failure would have waited for the first
# time someone pointed the harvester at a URL over TLS, which is the worst possible time to discover a
# missing CA bundle.
#
# Baked in rather than installed at runtime because the pod runs with readOnlyRootFilesystem.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY ./entrypoint /srv/entrypoint
STOPSIGNAL SIGTERM
CMD ["/srv/entrypoint"]

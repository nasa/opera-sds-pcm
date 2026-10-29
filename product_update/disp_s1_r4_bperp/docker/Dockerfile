ARG BASE=ghcr.io/opera-adt/disp-s1:0.5.14

# The base image is amd64-only. The "update" env is created for linux-64 in a
# stage that runs on the build host's own platform and is then only copied into
# the final image, so the image also builds on an arm64 (Graviton) mozart
# without emulating amd64. /opt/conda is the base image's MAMBA_ROOT_PREFIX.
FROM --platform=$BUILDPLATFORM ghcr.io/mamba-org/micromamba:2.3.3 AS update-env

RUN micromamba create --yes --platform linux-64 --channel conda-forge -p /opt/conda/envs/update python yamale h5py lxml && \
    micromamba clean --all --yes

FROM $BASE

COPY --from=update-env --chown=$MAMBA_USER:$MAMBA_USER /opt/conda/envs/update /opt/conda/envs/update

WORKDIR /update

COPY --chown=$MAMBA_USER:$MAMBA_USER update_runconfig_schema.yaml .
COPY --chown=$MAMBA_USER:$MAMBA_USER entrypoint.sh .
COPY --chown=$MAMBA_USER:$MAMBA_USER disp-s1-bperp-update.py .

ENTRYPOINT ["/bin/bash", "/update/entrypoint.sh"]
CMD ["--help"]

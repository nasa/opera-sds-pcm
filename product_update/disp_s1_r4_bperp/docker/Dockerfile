ARG BASE=ghcr.io/opera-adt/disp-s1:develop

FROM $BASE

RUN micromamba create --yes --channel conda-forge -n update python yamale h5py lxml && \
    micromamba clean --all --yes

WORKDIR /update

COPY --chown=$MAMBA_USER:$MAMBA_USER update_runconfig_schema.yaml .
COPY --chown=$MAMBA_USER:$MAMBA_USER entrypoint.sh .
COPY --chown=$MAMBA_USER:$MAMBA_USER disp-s1-bperp-update.py .

ENTRYPOINT ["/bin/bash", "/update/entrypoint.sh"]
CMD ["--help"]

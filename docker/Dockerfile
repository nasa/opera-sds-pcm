ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

# Use bash for RUN (default is /bin/sh). Required because conda's gdal package
# drops /opt/conda/share/bash-completion/completions/gdalinfo with bash-specific
# syntax (process substitution `<()`) that /bin/sh chokes on when .bash_profile
# is sourced. Build #3 on gmanipon-0082 hit:
#   /opt/conda/share/bash-completion/completions/gdalinfo: line 49: syntax error
#   near unexpected token `<'
SHELL ["/bin/bash", "-c"]

ARG GIT_OAUTH_TOKEN
ARG PCM_COMMONS_BRANCH
ARG PRODUCT_DELIVERY_BRANCH

# create ops user and group
RUN useradd -M -u 10005 ops

# softlink /home/ops
RUN ln -s /root /home/ops

# Make sure $HOME is set when we run this container
ENV HOME=/home/ops

# provision
USER root
RUN \
<<-EOT
  set -ex
  source /home/ops/.bash_profile

  # Match nisar-pcm/conda_requirements.txt exactly: bare package list, no
  # -c conda-forge (use base image's pinned channel config), no cffi, no
  # eccodes version pin. The `-c conda-forge` + `eccodes==2.31.0` + `cffi`
  # we previously had caused a conda solver conflict on python=3.12 (pinned
  # in the base image) -> libxml2/libgdal-core unsatisfiable -- the
  # combination diverged from the curated base-image channel state.
  ###
  # Restrict scipy version as v1.18 does not support numpy<2
  sudo /opt/conda/bin/conda install -y conda poppler gdal eccodes pandas s1reader 'scipy<1.18' && \
  sudo /opt/conda/bin/conda clean -afy
EOT

RUN \
<<-EOT
  set -ex
  source /home/ops/.bash_profile

  cd /home/ops/verdi/ops
  git clone https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git
  cd pcm_commons
  git checkout ${PCM_COMMONS_BRANCH}
  pip install -e .
EOT

RUN \
<<-EOT
  set -ex
  source /home/ops/.bash_profile

  cd /home/ops/verdi/ops
  git clone https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git
  cd CNM_product_delivery
  git checkout ${PRODUCT_DELIVERY_BRANCH}
  pip install -e .
EOT

COPY . /home/ops/verdi/ops/opera-pcm

RUN \
<<-EOT
  set -ex
  source /home/ops/.bash_profile

  sudo chown -R ops:ops /home/ops/verdi/ops/opera-pcm
  # Drop -U: hysds_commons pins `numpy<2.0.0` (elastic/elasticsearch-py#2646);
  # `pip install -U` would upgrade numpy past that pin, breaking the
  # `import elasticsearch` at runtime via `np.float_` removal in numpy 2.x.
  # NISAR's Dockerfile uses bare `pip install -e .` for the same reason.
  pip install -e '/home/ops/verdi/ops/opera-pcm[docker]'

  cd /home/ops/verdi/ops/opera-pcm
  ./docker/run_tests.sh
EOT

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

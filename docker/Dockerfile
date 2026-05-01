ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

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
  sudo /opt/conda/bin/conda install -y conda poppler gdal eccodes pandas
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
  pip install -U -e '/home/ops/verdi/ops/opera-pcm[docker]'

  cd /home/ops/verdi/ops/opera-pcm
  ./docker/run_tests.sh
EOT

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

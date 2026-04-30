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

  # gdal: no version pin (matches nisar-pcm conda_requirements.txt). An earlier
  # `gdal<3.12.0` pin caused a conda solver conflict during build_ci_job on
  # gmanipon-0082 (transitive libxml2 conflicts), with no actual ABI issue
  # observed once dropped.
  sudo /opt/conda/bin/conda install -y -c conda-forge conda gdal cffi poppler eccodes==2.31.0 pandas
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

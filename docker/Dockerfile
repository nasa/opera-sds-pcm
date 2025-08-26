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

  # Update with HySDS v5.5.0
  sudo /opt/conda/bin/conda install -y conda gdal cffi poppler eccodes==2.31.0 pandas

  # FIX: Patch the GDAL bash completion script to prevent syntax errors with /bin/sh.
  # This addresses a bug in the conda-forge gdal package where an sh script
  # tries to source a bash-only completion file.
  if [ -f "/opt/conda/etc/conda/activate.d/gdal-activate.sh" ]; then
    sudo cp /opt/conda/etc/conda/activate.d/gdal-activate.sh /opt/conda/etc/conda/activate.d/gdal-activate.sh.bak
    sudo sed -i 's|^#!/bin/sh|#!/bin/bash|' /opt/conda/etc/conda/activate.d/gdal-activate.sh
    diff /opt/conda/etc/conda/activate.d/gdal-activate.sh.bak /opt/conda/etc/conda/activate.d/gdal-activate.sh
  fi

  # TODO: remove the following kludge once this issue has been resolved:
  # https://stackoverflow.com/questions/71904252/gdalinfo-error-while-loading-shared-libraries-libtiledb-so-2-2-cannot-open-sh
  sudo ln -sf /opt/conda/lib/libtiledb.so /opt/conda/lib/libtiledb.so.2.2
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

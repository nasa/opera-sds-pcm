ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

ARG GIT_OAUTH_TOKEN
ARG PCM_COMMONS_BRANCH

# provision
USER ops
COPY . /home/ops/verdi/ops/opera-pcm
RUN set -ex \
 && source /home/ops/.bash_profile \
 && sudo /opt/conda/bin/conda install -y gdal cffi \
 # TODO: remove the following kludge once this issue has been resolved:
 # https://stackoverflow.com/questions/71904252/gdalinfo-error-while-loading-shared-libraries-libtiledb-so-2-2-cannot-open-sh
 && sudo ln -sf /opt/conda/lib/libtiledb.so /opt/conda/lib/libtiledb.so.2.2 \
 && sudo chown -R ops:ops /home/ops/verdi/ops/opera-pcm \
 && pip install -U -e '/home/ops/verdi/ops/opera-pcm[docker]' \
 && cd /home/ops/verdi/ops \
 && git clone https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git \
 && cd pcm_commons \
 && git checkout ${PCM_COMMONS_BRANCH} \
 && pip install -e . \
 && cd /home/ops/verdi/ops/opera-pcm \
 && ./docker/run_tests.sh

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

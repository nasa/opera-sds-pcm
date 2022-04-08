ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

ARG GIT_OAUTH_TOKEN
ARG PCM_COMMONS_BRANCH

# provision
USER ops
COPY . /home/ops/verdi/ops/opera-pcm
RUN set -ex \
 && source /home/ops/.bash_profile \
 && pip install '.[dockertest]' \
 && sudo chown -R ops:ops /home/ops/verdi/ops/opera-pcm \
 && pip install pcm-commons@git+https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git@${PCM_COMMONS_BRANCH} \
 && cd /home/ops/verdi/ops/opera-pcm \
 && ./docker/run_tests.sh

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

ARG GIT_OAUTH_TOKEN
ARG PCM_COMMONS_BRANCH

# provision
USER ops
COPY . /home/ops/verdi/ops/opera-pcm
RUN set -ex \
 && source /home/ops/.bash_profile \
 && pip install -U more-itertools \
 && pip install -U pytest scripttest mock mockito flake8 pytest-cov flake8-junit-report flake8-string-format xmltodict yamale==3.0.6 ruamel.yaml elasticmock geopandas smart_open \
 && pip install -U pytest-asyncio elasticsearch[async] \
 && sudo chown -R ops:ops /home/ops/verdi/ops/opera-pcm \
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

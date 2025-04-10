ARG PGE_BASE_VERSION
FROM hysds/pge-base:${PGE_BASE_VERSION}

ARG GIT_OAUTH_TOKEN
ARG PCM_COMMONS_BRANCH
ARG PRODUCT_DELIVERY_BRANCH

# provision
USER ops
COPY . /home/ops/verdi/ops/opera-pcm
RUN set -ex \
 && source /home/ops/.bash_profile \
 # TODO: remove pin of conda and gdal once updated gdal (3.7.*) has been pushed to the main channel
 && sudo /opt/conda/bin/conda install -y -c conda-forge conda gdal==3.6.4 cffi poppler eccodes==2.31.0 \
# && sudo /opt/conda/bin/conda install -y -c conda-forge conda==22.11.1 gdal==3.6.2 cffi poppler==22.12.0 \
 # install eccodes with binary tar file for ECMWF merger
# && curl -o /tmp/eccodes-2.31.0-1.el8.x86_64.rpm https://artifactory-fn.jpl.nasa.gov:443/artifactory/general-develop/gov/nasa/jpl/opera/sds/pcm/eccodes/eccodes-2.31.0-1.el8.x86_64.rpm \
# && sudo dnf install -y /tmp/eccodes-2.31.0-1.el8.x86_64.rpm \
# && export ECCODES_DIR=/opt/eccodes && pip install eccodes==2.31.0 --no-binary eccodes \
# && rm -rf /tmp/eccodes-2.31.0-1.el8.x86_64.rpm \
# && ln -sf /opt/eccodes/bin/grib_to_netcdf /home/ops/verdi/bin/ \
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
 && cd /home/ops/verdi/ops \
 && git clone https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git \
 && cd CNM_product_delivery \
 && git checkout ${PRODUCT_DELIVERY_BRANCH} \
 && pip install -e . \
 && cd /home/ops/verdi/ops/opera-pcm \
 && ./docker/run_tests.sh

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

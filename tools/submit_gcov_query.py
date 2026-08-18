import argparse
import logging
import re
from datetime import datetime
from os.path import isfile
from typing import Optional, Union, Tuple, Iterable

from dateutil.parser import parse as datetime_parse
from shapely import from_wkt, from_geojson
from shapely.geometry import Polygon, MultiPolygon

from util.conf_util import SettingsConf
try:
    from util.job_submitter import try_submit_mozart_job
except Exception as e:
    __IMPORT_ERROR = e
    def try_submit_mozart_job(*args, **kwargs):
        raise NotImplementedError(f'Unable to import mozart job submitter. '
                                  f'You may not be running on an active cluster: {__IMPORT_ERROR}')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


GCOV_PATTERN = re.compile(r"(?P<id>(?P<project>NISAR)_(?P<instrument>L)(?P<level>2)_(?P<processing_type>PR|UR|OD)_"
                          r"(?P<product_type>GCOV)_(?P<cycle>\d{3})_(?P<track>\d{3})_(?P<orbit_dir>[AD])_"
                          r"(?P<frame_id>\d{3})_(?P<mode>\d{4})_(?P<pol>(SH|SV|DH|DV|CL|CR|QP|NA){2})_(?P<source>[AM])_"
                          r"(?P<sensing_start_date_time>\d{8}T\d{6})_(?P<sensing_end_date_time>\d{8}T\d{6})_"
                          r"(?P<crid>[A-Za-z0-9]{6})_(?P<accuracy>[PMNF])_(?P<coverage>[FP])_(?P<sds_location>[JN])_"
                          r"(?P<counter>\d{3}))$")


def submit_catalog_ingest_job(
        start_date: datetime,
        end_date: datetime,
        use_revision: bool,
        spatial: Optional[str],
        native_id: Optional[str],
        mgrs_sets: Optional[Iterable[str]],
        job_type: str,
        job_release: str
) -> str:
    params = [
        {
            "name": "start_date",
            "from": "value",
            "type": "text",
            "value": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        {
            "name": "end_date",
            "from": "value",
            "type": "text",
            "value": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        {
            "name": "use_temporal",
            "from": "value",
            "type": "boolean",
            "value": str(use_revision),
        },
    ]

    if spatial is not None:
        params.append({
            "name": "spatial",
            "from": "value",
            "type": "",
            "value": spatial,
        })

    if native_id is not None:
        params.append({
            "name": "native_id",
            "from": "value",
            "type": "text",
            "value": native_id,
        })

    if mgrs_sets is not None:
        params.append({
            "name": "mgrs_sets",
            "from": "value",
            "type": "text",
            "value": ','.join(mgrs_sets),
        })

    try:
        job_id = try_submit_mozart_job(
            product={},
            job_queue='opera-job_worker-gcov_catalog_ingest',
            rule_name=f'trigger-{job_type}',
            params=params,
            job_spec=f'{job_type}:{job_release}',
            job_name=f'job-GCOV_catalog_ingest'
        )
        logger.info(f'Submitted HySDS job {job_id}')
        return job_id
    except Exception as e:
        logger.error(f"Failed to submit job: {str(e)}")
        raise


def _parse_wkt(wkt_str_or_path: str) -> Union[Polygon, MultiPolygon]:
    if isfile(wkt_str_or_path):
        with open(wkt_str_or_path) as f:
            wkt_str = f.read().strip()
    else:
        wkt_str = wkt_str_or_path

    geom = from_wkt(wkt_str)

    if not isinstance(geom, (Polygon, MultiPolygon)):
        raise argparse.ArgumentTypeError(f'Unexpected geometry type: {type(geom)}')

    return geom


def _parse_geojson(geojson_str_or_path: str) -> Union[Polygon, MultiPolygon]:
    if isfile(geojson_str_or_path):
        with open(geojson_str_or_path) as f:
            geojson_str = f.read().strip()
    else:
        geojson_str = geojson_str_or_path

    geom = from_geojson(geojson_str)

    if not isinstance(geom, (Polygon, MultiPolygon)):
        raise argparse.ArgumentTypeError(f'Unexpected geometry type: {type(geom)}')

    return geom


def _mgrs_set(set_id: str) -> str:
    mgrs_set_pattern = re.compile(r'MS_\d+_\d+')

    if not mgrs_set_pattern.fullmatch(set_id):
        raise argparse.ArgumentTypeError(f'Provided value {set_id} does not match the expected MGRS tile set '
                                         f'ID pattern {mgrs_set_pattern.pattern}')

    return set_id


def parse_args():
    parser = argparse.ArgumentParser(description="Manually submit GCOV catalog-ingest job for historical/reproc")

    parser.add_argument(
        '-s', '--start-date',
        type=datetime_parse,
        default=None,
        help='Start time for the temporal query (YYYY-MM-DDThh:mm:ss). '
             'Must use with --end-date and not with --native-id'
    )

    parser.add_argument(
        '-e', '--end-date',
        type=datetime_parse,
        default=None,
        help='End time for the temporal query (YYYY-MM-DDThh:mm:ss). '
             'Must use with --start-date and not with --native-id'
    )

    parser.add_argument(
        '--use-revision',
        action='store_true',
        help='For temporal query, use revision time rather than acquisition time'
    )

    spatial_group = parser.add_mutually_exclusive_group()

    spatial_group.add_argument(
        '-b', '--bbox',
        nargs=4,
        dest='bbox',
        metavar=('MIN_LON', 'MIN_LAT', 'MAX_LON', 'MAX_LAT'),
        type=float,
        default=None,
        help='Bounding box: min_lon min_lat max_lon max_lat'
    )

    spatial_group.add_argument(
        '-w', '--wkt',
        default=None,
        type=_parse_wkt,
        help='WKT representation or path to a file containing WKT representation of an AOI to query. '
             'Must be either POLYGON or MULTIPOLYGON'
    )

    spatial_group.add_argument(
        '-g', '--geojson',
        default=None,
        type=_parse_geojson,
        help='GeoJSON representation or path to a file containing GeoJSON representation of an AOI to query. '
             'Must be either POLYGON or MULTIPOLYGON'
    )

    parser.add_argument(
        '-i', '--native-id',
        default=None,
        help='Native ID to query. Will ignore any spatial parameters. GCOVs queried will expand to all GCOVs within '
             'all MGRS tile sets for which this GCOV is a member. Native ID must be a *FULL* GCOV ID, wildcards and '
             'partial IDs are not supported'
    )

    parser.add_argument(
        '-m', '--mgrs-sets',
        default=None,
        type=_mgrs_set,
        nargs='+',
        help='MGRS sets to restrict cataloging to'
    )

    args = parser.parse_args()

    if all([v is None for v in {args.start_date, args.end_date, args.native_id}]):
        raise ValueError(f'Must provide --start-date and --end-date or --native-id')

    if args.native_id is None:
        if args.start_date is None or args.end_date is None:
            raise ValueError('Must provide --start-date and --end-date together')

        if args.start_date >= args.end_date:
            raise ValueError('Start date must be before end date')
    else:
        if args.start_date is not None or args.end_date is not None:
            raise ValueError('Must not provide --start-date or --end-date with --native-id')

        if not GCOV_PATTERN.fullmatch(args.native_id):
            raise ValueError(f'Native ID {args.native_id} does not match expected pattern {GCOV_PATTERN.pattern}')

        # use dummy values for start/end time as the HySDS job requires them (but ignores them)
        args.start_date = datetime(1970, 1, 1)
        args.end_date = datetime(3000, 1, 1)

        args.bbox = None
        args.wkt = None
        args.geojson = None

    if args.bbox is not None:
        min_lon, min_lat, max_lon, max_lat = args.bbox

        errs = []

        if min_lat >= max_lat:
            errs.append(ValueError(f'Minimum latitude cannot be >= maximum latitude'))

        if min_lon >= max_lon:
            errs.append(ValueError(f'Minimum longitude cannot be >= maximum longitude'))

        if any(not (-180 <= c <= 180) for c in {min_lon, max_lon}):
            errs.append(ValueError(f'Longitudes must be between -180 and 180'))

        if any(not (-90 <= c <= 90) for c in {min_lat, max_lat}):
            errs.append(ValueError(f'Latitudes must be between -90 and 90'))

        if len(errs) > 0:
            raise ExceptionGroup('Provided bbox is invalid', errs)

        args.bbox = ','.join(str(c) for c in args.bbox)

    if args.wkt is not None or args.geojson is not None:
        args.polygon = args.wkt if args.wkt is not None else args.geojson
    else:
        args.polygon = None
    del args.wkt, args.geojson

    return args


def main():
    args = parse_args()

    settings = SettingsConf().cfg
    job_type = 'job-gcov_catalog_ingest'
    job_release = settings['RELEASE_VERSION']

    if args.bbox is not None:
        spatial = args.bbox,
    elif args.polygon is not None:
        spatial = args.polygon.wkt
    else:
        spatial = None

    submit_catalog_ingest_job(
        args.start_date,
        args.end_date,
        args.use_revision,
        spatial,
        args.native_id,
        args.mgrs_sets,
        job_type,
        job_release
    )


if __name__ == '__main__':
    main()

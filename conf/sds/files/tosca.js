// -----------------------------------------------------------------------------
// OPERA override of hysds_ui's tosca.template.js
//
// sdscli's send_hysds_ui_conf() prefers ~/.sds/files/tosca.js over the
// hysds_ui checkout's tosca.template.js, and mozart.tf installs this repo's
// conf/sds/ as ~/.sds/, so this file is what a deployed cluster builds against.
//
// PROVENANCE -- copied verbatim from hysds_ui v1.4.0 (86f2c1d), then modified
// only where marked "OPERA:". Because this file REPLACES the upstream template
// rather than extending it, facets added upstream will NOT appear on our
// clusters until this file is re-synced. Re-sync it whenever hysds_release
// changes, and diff it against the template shipped with that release:
//
//   diff <(sed -n '/exports.FILTERS/,/^];/p' ~/mozart/ops/hysds_ui/src/config/tosca.template.js) \
//        <(sed -n '/exports.FILTERS/,/^];/p' conf/sds/files/tosca.js)
//
// NOTE -- this file is rendered through Jinja by sdscli (use_jinja=True), so a
// doubled opening brace, or an opening brace followed by a percent sign, is a
// Jinja delimiter and will be interpreted rather than passed through. In
// practice that rules out JSX inline styles, whose value is written with two
// opening braces; assign the style object to a named constant and reference it
// by name instead. This comment is deliberately worded to avoid the sequences
// it describes.
// -----------------------------------------------------------------------------

const React = require("react"); // lgtm [js/unused-local-variable]

// DEFINING THE OPTIONS FOR THE LEAFLET MAP
exports.DISPLAY_MAP = true;

// all leaflet styles: https://leaflet-extras.github.io/leaflet-providers/preview/
// NOTE: CARTO's free raster basemaps (basemaps.cartocdn.com) now require an API
// key and render an "API KEY REQUIRED" watermark without one, so default to
// OpenStreetMap's keyless standard tiles
exports.LEAFLET_TILELAYER = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
exports.LEAFLET_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

exports.BBOX_COLOR = "#f06eaa";
exports.BBOX_WEIGHT = 5;
exports.BBOX_OPACITY = 0.3;

// reactivesearch retrieves data from each component by its componentId
// custom Reactivesearch component
exports.ID_COMPONENT = "_id";
exports.QUERY_SEARCH_COMPONENT_ID = "query_string";
exports.MAP_COMPONENT_ID = "polygon";

// built in Reactivesearch component id
exports.RESULTS_LIST_COMPONENT_ID = "results";

// fields returned by Elasticsearch (less fields = faster UI)
exports.FIELDS = [
  "id",
  "starttime",
  "endtime",
  "location",
  "center",
  "urls",
  "browse_urls",
  "datasets",
  "metadata.Files.sensor",
  "daac_delivery_status",
  "daac_CNM_S_status",
  "metadata.sensoroperationalmode",
  "metadata.polarisationmode",
  "metadata.user_tags",
  "@timestamp",
  "dataset",
];

exports.GRQ_TABLE_VIEW_DEFAULT = true;

exports.FILTERS = [
  {
    componentId: "dataset",
    dataField: "dataset.keyword",
    title: "Dataset",
    type: "single",
    size: 1000,
  },
  {
    componentId: "dataset_type",
    dataField: "dataset_type.keyword",
    title: "Dataset Type",
    type: "single",
    size: 1000,
  },
  {
    componentId: "dataset_level",
    dataField: "dataset_level.keyword",
    title: "Dataset Level",
    type: "single",
    size: 1000,
  },
  {
    componentId: "system_version",
    dataField: "system_version.keyword",
    title: "System Version",
    type: "single",
    size: 1000,
  },
  {
    componentId: "creation_timestamp",
    dataField: "creation_timestamp",
    // OPERA: see the date-filter note at the bottom of this file.
    title: "Created At (local dates, UTC data, end date excluded)",
    type: "date",
  },
  {
    // OPERA: upstream points this at metadata.platform, which nothing populates,
    // so the facet rendered as nothing at all -- reactivesearch returns null from
    // a list with zero buckets, removing the control entirely. The value does
    // exist: PCM's filename regex captures it as `sensor` and product2dataset
    // leaves it per-file. Values seen: S1A/S1B/S1C/S1D, S2A/S2B/L8/L9 (HLS),
    // LSAR (NI). Note DISP-S1 has no platform in its filename and so has none here.
    componentId: "platform",
    dataField: "metadata.Files.sensor.keyword",
    title: "Platform",
    type: "single",
  },
  {
    // OPERA: replaces upstream's "Continent" facet, which pointed at a field
    // OPERA never populates and therefore never rendered.
    componentId: "daac_delivery_status",
    dataField: "daac_delivery_status.keyword",
    title: "DAAC Delivery Status",
    type: "single",
  },
  {
    componentId: "tags",
    dataField: "metadata.tags.keyword",
    title: "Tags",
    type: "multi",
    size: 1000,
  },
  {
    componentId: "starttime",
    dataField: "starttime",
    // OPERA: see the date-filter note at the bottom of this file.
    title: "Start Time (local dates, UTC data, end date excluded)",
    type: "date",
  },
  {
    componentId: "endtime",
    dataField: "endtime",
    // OPERA: see the date-filter note at the bottom of this file.
    title: "End Time (local dates, UTC data, end date excluded)",
    type: "date",
  },
  {
    // OPERA: replaces upstream's "State" facet, likewise unpopulated here.
    componentId: "daac_cnm_s_status",
    dataField: "daac_CNM_S_status.keyword",
    title: "CNM-S Status",
    type: "single",
    size: 1000,
  },
  // OPERA: upstream's "Exists In Object Store" facet removed -- it pointed at
  // metadata.exists_in_object_store, which OPERA does not populate, so it never
  // rendered. Nothing equivalent to put in its place.
];

exports.QUERY_LOGIC = {
  and: [
    "dataset",
    "dataset_level",
    "dataset_type",
    "system_version",
    "creation_timestamp",
    "starttime",
    "endtime",
    "platform",
    "daac_delivery_status",
    "daac_cnm_s_status",
    "tags",
    this.ID_COMPONENT,
    this.MAP_COMPONENT_ID,
    this.QUERY_SEARCH_COMPONENT_ID,
  ],
};

// NOTE: add "keyword: true" to the column if the field is sorted by keyword, ex. id.keyword
// check your Elasticsearch mapping
exports.GRQ_DISPLAY_COLUMNS = [
  { Header: "ID", accessor: "id", width: 400, keyword: true },
  {
    Header: "Dataset",
    accessor: "dataset",
    className: "keyword",
    keyword: true,
  },
  { Header: "last_modified", accessor: "@timestamp", width: 200 },
  { Header: "start_time", accessor: "starttime" },
  { Header: "end_time", accessor: "endtime" },
  {
    id: "browse",
    sortable: false,
    width: 100,
    resizable: false,
    Cell: (state) =>
      state.original.urls && state.original.urls.length > 0 ? (
        <a
          target="_blank"
          href={state.original.urls[0]}
          rel="noopener noreferrer"
        >
          Browse
        </a>
      ) : null,
  },
];

// -----------------------------------------------------------------------------
// OPERA: why the date facet titles carry a warning
//
// The "date" filters render reactivesearch's DateRange (pinned at 3.2.4). Two
// behaviours of that component surprise users on this cluster:
//
//   1. Both ends of the range are serialized as the selected day's MIDNIGHT,
//      not 00:00:00 -> 23:59:59. So "Sep 1 - Sep 8" excludes all of Sep 8 after
//      midnight, and "Sep 8 - Sep 8" produces gte === lte and matches almost
//      nothing.
//
//   2. That midnight is the VIEWER'S LOCAL midnight, serialized to an absolute
//      instant. The picker hands back a local Date; XDate re-parses the
//      "YYYY-MM-DD" string as local midnight (unlike native Date, which parses
//      it as UTC); queryFormat defaults to "epoch_millis". A viewer at UTC-7
//      selecting Sep 8 sends 2026-09-08T07:00:00Z. Every timestamp in the
//      results table, meanwhile, is the raw UTC value from elasticsearch.
//
// Neither is configurable today: hysds_ui's SidebarFilters destructures a fixed
// set of prop names and silently drops everything else, so queryFormat and
// customQuery never reach the widget. Labelling the controls is the only
// mitigation available from this file alone.
//
// Once the upstream pass-through lands and reaches our hysds_release, adding
// the line below to each date filter fixes both behaviours -- it emits
// "YYYY-MM-DD" and lets elasticsearch round the range to whole UTC days -- and
// these titles should go back to plain "Created At" / "Timestamp" etc.
//
//     queryFormat: "date",
//
// Setting it before that lands is harmless but has no effect.
// -----------------------------------------------------------------------------

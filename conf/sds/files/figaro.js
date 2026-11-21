// -----------------------------------------------------------------------------
// OPERA override of hysds_ui's figaro.template.js
//
// sdscli's send_hysds_ui_conf() prefers ~/.sds/files/figaro.js over the
// hysds_ui checkout's figaro.template.js, and mozart.tf installs this repo's
// conf/sds/ as ~/.sds/, so this file is what a deployed cluster builds against.
//
// PROVENANCE -- copied verbatim from hysds_ui v1.4.0 (86f2c1d), then modified
// only where marked "OPERA:". Because this file REPLACES the upstream template
// rather than extending it, facets added upstream will NOT appear on our
// clusters until this file is re-synced. Re-sync it whenever hysds_release
// changes, and diff it against the template shipped with that release:
//
//   diff <(sed -n '/exports.FILTERS/,/^];/p' ~/mozart/ops/hysds_ui/src/config/figaro.template.js) \
//        <(sed -n '/exports.FILTERS/,/^];/p' conf/sds/files/figaro.js)
//
// NOTE -- this file is rendered through Jinja by sdscli (use_jinja=True), so a
// doubled opening brace, or an opening brace followed by a percent sign, is a
// Jinja delimiter and will be interpreted rather than passed through. In
// practice that rules out JSX inline styles, whose value is written with two
// opening braces; assign the style object to a named constant and reference it
// by name instead. This comment is deliberately worded to avoid the sequences
// it describes.
// -----------------------------------------------------------------------------

// NOTE: add "keyword: true" to the column if the field is sorted by keyword, ex. id.keyword
// check your Elasticsearch mapping
exports.FIGARO_DISPLAY_COLUMNS = [
  { Header: "status", accessor: "status" },
  { Header: "job name", accessor: "job.name" },
  { Header: "job type", accessor: "job.type" },
  { Header: "queue", accessor: "job.job_info.job_queue" },
  { Header: "node", accessor: "job.job_info.execute_node" },
  { Header: "timestamp", accessor: "@timestamp", width: 200 },
  { Header: "duration (sec)", accessor: "job.job_info.duration" },
];

exports.FILTERS = [
  {
    componentId: "resource",
    dataField: "resource",
    title: "Resource",
    type: "single",
    defaultValue: "job",
  },
  {
    componentId: "status",
    dataField: "status",
    title: "Status",
    type: "single",
    sortBy: "asc",
  },
  {
    componentId: "redelivered",
    dataField: "job.delivery_info.redelivered",
    title: "Redelivered",
    type: "boolean",
  },
  {
    componentId: "tags",
    dataField: "tags.keyword",
    title: "Tags",
    type: "multi",
    size: 1000,
  },
  {
    componentId: "timestamp",
    dataField: "@timestamp",
    // OPERA: see the date-filter note at the bottom of this file.
    title: "Timestamp (local dates, UTC data, end date excluded)",
    type: "date",
  },
  {
    componentId: "job_type",
    dataField: "job.type",
    title: "Job Type",
    type: "single",
    size: 1000,
  },
  {
    componentId: "queue",
    dataField: "job.job_info.job_queue",
    title: "Job Queue",
    type: "single",
  },
  {
    componentId: "username",
    dataField: "job.username",
    title: "Username",
    type: "single",
  },
  {
    componentId: "node",
    dataField: "job.job_info.execute_node",
    title: "Node",
    type: "single",
    size: 1000,
  },
  {
    componentId: "priority",
    dataField: "job.priority",
    title: "Priority",
    type: "multi",
    sortBy: "desc",
  },
  {
    componentId: "short_error",
    dataField: "short_error.keyword",
    title: "Short Error",
    type: "single",
    size: 1000,
  },
  {
    componentId: "job_detail",
    dataField: "msg",
    title: "Job Detail",
    type: "single",
  },
  {
    componentId: "container_image",
    dataField: "job.container_image_name",
    title: "Container Image",
    type: "single",
  },
  {
    componentId: "instance_type",
    dataField: "job.job_info.facts.ec2_instance_type",
    title: "Instance Type",
    type: "single",
  },
  {
    componentId: "retry_count",
    dataField: "job.retry_count",
    title: "Retry Count",
    type: "single",
  },
];

// TODO: TRY ADDING .KEYWORD TO COMPONENTID
exports.QUERY_LOGIC = {
  and: [
    "_id",
    "tags",
    "status",
    "short_error",
    "job_detail",
    "resource",
    "job_type",
    "queue",
    "username",
    "node",
    "priority",
    "container_image",
    "instance_type",
    "retry_count",
    "query_string",
    "payload_id",
    "timestamp",
    "redelivered",
  ],
};

exports.FIELDS = [
  "_index",
  "_id",
  "status",
  "resource",
  "payload_id",
  "@timestamp",
  "short_error",
  "error",
  "traceback",
  "msg_details",
  "dedup_msg",
  "tags",
  "job.name",
  "job.priority",
  "job.retry_count",
  "job.type",
  "job.job_info.execute_node",
  "job.job_info.facts.ec2_instance_type",
  "job.job_info.job_queue",
  "job.job_info.duration",
  "job.job_info.job_url",
  "job.job_info.time_queued",
  "job.job_info.time_start",
  "job.job_info.time_end",
  "job.job_info.metrics.products_staged.id",
  "job.delivery_info.redelivered",
  "event.traceback",
  "user_tags",
  "dedup_job",
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

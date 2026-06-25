"""
Output formatting for the DIST-S1 input tool.

Handles JSON, text, and IDs output formats for single query, batch, and
temporal window modes.
"""

import json
import logging

logger = logging.getLogger(__name__)


def _calculate_lookback_window(time, years_back, window_size):
    """Deferred import to avoid circular dependency with dist_s1_input_tool."""
    from tools.dist_s1_input_tool import calculate_lookback_window
    return calculate_lookback_window(time, years_back, window_size)


def format_baseline_product_json(product: dict, time, window_size: int) -> dict:
    """Format a single baseline product for JSON output."""
    windows = {}
    for years_back, window_name in [(1, "w1"), (2, "w2"), (3, "w3")]:
        windows[window_name] = {
            "years_back": years_back,
            "window": _calculate_lookback_window(time, years_back, window_size).to_dict(),
            "granules": [g.to_dict() for g in product[window_name]],
            "count": len(product[window_name]),
        }

    total_files = len(product["t0"]) + len(product["w1"]) + len(product["w2"]) + len(product["w3"])
    return {
        "burst_id": product["burst_id"],
        "subswath": product["subswath"],
        "t0": {
            "description": "RTC granules at acquisition time",
            "granules": [g.to_dict() for g in product["t0"]],
            "count": len(product["t0"]),
        },
        "windows": windows,
        "total_granules": total_files,
    }


def format_json_output(results: list[dict], args) -> dict:
    """Format results as JSON output."""
    if len(results) == 1:
        # Single query mode
        result = results[0]
        tile_id = result["tile_id"]
        time = result["reference_time"]
        baseline_products = result["baseline_products"]

        output = {
            "query": {
                "tile_id": tile_id,
                "reference_time": time.isoformat(),
                "window_size_days": args.window_size,
                "max_files": list(args.max_files),
            },
            "baseline_products": {},
            "summary": {
                "total_baselines": len(baseline_products),
                "total_granules": 0,
            },
        }

        for baseline_id, product in baseline_products.items():
            formatted = format_baseline_product_json(product, time, args.window_size)
            output["baseline_products"][baseline_id] = formatted
            output["summary"]["total_granules"] += formatted["total_granules"]
    else:
        # Batch mode
        output = {
            "query": {"window_size_days": args.window_size, "max_files": list(args.max_files)},
            "results": [],
        }
        total_baselines = total_granules = 0
        for result in results:
            baseline_products = result["baseline_products"]
            result_entry = {
                "native_id": result.get("native_id"),
                "tile_id": result["tile_id"],
                "reference_time": result["reference_time"].isoformat(),
                "baseline_products": {},
            }
            result_total_granules = 0
            for baseline_id, product in baseline_products.items():
                formatted = format_baseline_product_json(product, result["reference_time"], args.window_size)
                result_entry["baseline_products"][baseline_id] = formatted
                result_total_granules += formatted["total_granules"]
            result_entry["total_granules"] = result_total_granules
            result_entry["total_baselines"] = len(baseline_products)
            output["results"].append(result_entry)
            total_baselines += len(baseline_products)
            total_granules += result_total_granules
        output["summary"] = {
            "total_queries": len(results),
            "total_baselines": total_baselines,
            "total_granules": total_granules,
        }

        # Add list of products needing retriggering (successful queries with sufficient inputs)
        output["products_to_retrigger"] = []
        for result in results:
            tile_id = result["tile_id"]
            # Add acquisition group if present
            if "acq_group" in result and result["acq_group"]:
                formatted_tile_id = f"{tile_id}_{result['acq_group']}"
            else:
                formatted_tile_id = tile_id

            output["products_to_retrigger"].append(
                {
                    "tile_id": tile_id,
                    "acq_group": result.get("acq_group"),
                    "acquisition_time": result["reference_time"].isoformat(),
                    "formatted": f"{formatted_tile_id},{result['reference_time'].strftime('%Y%m%dT%H%M%SZ')}",
                }
            )

    return output


def format_ids_output(results: list[dict]) -> str:
    """Format results as granule IDs only (one per line)."""
    ids = []
    for result in results:
        for product in result["baseline_products"].values():
            for window_name in ["t0", "w1", "w2", "w3"]:
                ids.extend(g.granule_id for g in product[window_name])
    return "\n".join(ids)


def format_temporal_window_json(results: dict, args) -> dict:
    """Format temporal window results for JSON serialization."""
    formatted_details = []
    for detail in results["details"]:
        formatted_detail = {
            "tile_id": detail["tile_id"],
            "acquisition_time": detail["acquisition_time"].isoformat(),
            "is_sufficient": detail["is_sufficient"],
            "baseline_count": detail["baseline_count"],
            "reason": detail.get("reason", ""),
        }

        # Include diagnostics if present
        if "diagnostics" in detail and detail["diagnostics"]:
            formatted_detail["diagnostics"] = detail["diagnostics"]

        # Format baseline_products if present
        if "baseline_products" in detail and detail["baseline_products"]:
            formatted_baselines = {}
            for baseline_id, product in detail["baseline_products"].items():
                formatted_baselines[baseline_id] = format_baseline_product_json(
                    product, detail["acquisition_time"], args.window_size
                )
            formatted_detail["baseline_products"] = formatted_baselines
        else:
            formatted_detail["baseline_products"] = {}

        formatted_details.append(formatted_detail)

    return {
        "query": results["query"],
        "summary": results["summary"],
        "jobs_by_tile": results["jobs_by_tile"],
        "jobs_by_date": results["jobs_by_date"],
        "details": formatted_details,
    }


def format_temporal_window_output(results: dict, args) -> str:
    """Format temporal window analysis results."""
    if args.output == "json":
        # Determine output filename
        if args.output_file:
            output_file = args.output_file
        else:
            # Auto-generate filename based on query parameters
            start = results["query"]["start_date"].replace(":", "").replace("-", "")[:8]
            end = results["query"]["end_date"].replace(":", "").replace("-", "")[:8]
            output_file = f"temporal_window_analysis_{start}_{end}.json"

        # Auto-generate log filename if not specified
        if not args.log_file and args.output == "json":
            log_file = output_file.replace(".json", ".log")
            file_handler = logging.FileHandler(log_file, mode="w")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            )
            logging.getLogger().addHandler(file_handler)
            logger.info(f"Auto-generated log file: {log_file}")

        # Format results for JSON serialization
        formatted_results = format_temporal_window_json(results, args)

        # Save full results to file
        with open(output_file, "w") as f:
            json.dump(formatted_results, f, indent=2)

        # Return summary message
        summary = results["summary"]
        log_msg = (
            f" (logs: {output_file.replace('.json', '.log')})" if not args.log_file else f" (logs: {args.log_file})"
        )
        return (
            f"\nSaved detailed results to: {output_file}{log_msg}\n"
            f"Summary: {summary['jobs_with_sufficient_inputs']}/{summary['total_acquisition_times']} "
            f"jobs have sufficient inputs across {summary['total_tiles']} tiles\n"
        )

    # Text output format
    lines = []
    query = results["query"]
    summary = results["summary"]
    jobs_by_tile = results["jobs_by_tile"]
    jobs_by_date = results["jobs_by_date"]

    lines.extend(
        [
            "",
            "=" * 80,
            "DIST-S1 Temporal Window Job Forecast",
            "=" * 80,
            f"Query Period: {query['start_date']} to {query['end_date']}",
        ]
    )

    # Add window config info
    wc = query["window_configs"]
    lines.append(f"Window Configuration: w1={wc[0][2]}, w2={wc[1][2]}, w3={wc[2][2]} ({wc[0][1]}-day windows)")

    lines.extend(
        [
            "",
            "Summary:",
            f"  Unique tiles with RTC data: {summary['total_tiles']}",
            f"  Total acquisition times analyzed: {summary['total_acquisition_times']}",
            f"  Jobs with sufficient inputs: {summary['jobs_with_sufficient_inputs']}",
            f"  Jobs with insufficient inputs: {summary['jobs_with_insufficient_inputs']}",
        ]
    )

    # Breakdown by tile (top 15)
    if jobs_by_tile:
        lines.extend(
            [
                "",
                "Breakdown by Tile (top 15):",
            ]
        )

        # Sort by total jobs (sufficient + insufficient)
        sorted_tiles = sorted(
            jobs_by_tile.items(), key=lambda x: x[1]["sufficient"] + x[1]["insufficient"], reverse=True
        )

        for tile, counts in sorted_tiles[:15]:
            total = counts["sufficient"] + counts["insufficient"]
            lines.append(
                f"  {tile:10s}: {total:3d} jobs ({counts['sufficient']:3d} sufficient, {counts['insufficient']:3d} insufficient)"
            )

    # Breakdown by date (show all dates with data)
    if jobs_by_date:
        lines.extend(
            [
                "",
                "Breakdown by Date:",
            ]
        )

        for date_str in sorted(jobs_by_date.keys()):
            counts = jobs_by_date[date_str]
            total = counts["sufficient"] + counts["insufficient"]
            lines.append(
                f"  {date_str}: {total:3d} jobs ({counts['sufficient']:3d} sufficient, {counts['insufficient']:3d} insufficient)"
            )

    # Show insufficient jobs if there are any (and not too many)
    insufficient_jobs = [d for d in results["details"] if not d["is_sufficient"]]
    if insufficient_jobs and len(insufficient_jobs) <= 20:
        lines.extend(
            [
                "",
                "Insufficient Jobs (missing data):",
            ]
        )
        for job in insufficient_jobs:
            lines.append(f"  {job['tile_id']:10s} @ {job['acquisition_time'].isoformat()}: {job['reason']}")
    elif len(insufficient_jobs) > 20:
        lines.extend(
            [
                "",
                f"Insufficient Jobs: {len(insufficient_jobs)} jobs have insufficient data",
                "  (Use --output json --full-output for complete list)",
            ]
        )

    lines.extend(["=" * 80, ""])

    return "\n".join(lines)


def format_text_output(results: list[dict], args) -> str:
    """Format results as human-readable text output."""
    lines = []
    if len(results) == 1:
        # Single query mode
        result = results[0]
        tile_id, time, baseline_products = result["tile_id"], result["reference_time"], result["baseline_products"]
        lines.extend(
            [
                "\n" + "=" * 80,
                "DIST-S1 Baseline Product Selection Results",
                "=" * 80 + "\n",
                f"Tile: {tile_id}",
                f"Acquisition time: {time.isoformat()}",
                f"Found {len(baseline_products)} baseline products (unique burst+subswath combinations)\n",
            ]
        )

        total_files = 0
        for baseline_id, product in sorted(baseline_products.items()):
            t0, w1, w2, w3 = product["t0"], product["w1"], product["w2"], product["w3"]
            baseline_total = len(t0) + len(w1) + len(w2) + len(w3)
            total_files += baseline_total
            lines.extend(
                [
                    "-" * 80,
                    f"Baseline: {baseline_id} (burst={product['burst_id']}, subswath={product['subswath']})",
                    f"Total files: {baseline_total} (t0={len(t0)}, w1={len(w1)}, w2={len(w2)}, w3={len(w3)})",
                    "",
                    "  Acquisition Time (t0):",
                    f"    Files found: {len(t0)}",
                ]
            )
            lines.extend(f"      {g.acquisition_time.isoformat()}: {g.granule_id}" for g in t0) if t0 else lines.append(
                "      (No granules found)"
            )
            lines.append("")

            for window_name, granules, years_back in [("Window 1", w1, 1), ("Window 2", w2, 2), ("Window 3", w3, 3)]:
                window = _calculate_lookback_window(time, years_back, args.window_size)
                lines.extend(
                    [
                        f"  {window_name} (t0 - {years_back} year{'s' if years_back > 1 else ''}):",
                        f"    Target date: {window.window_end.isoformat()}",
                        f"    Range: {window.window_start.isoformat()} to {window.window_end.isoformat()}",
                        f"    Files found: {len(granules)}/{args.max_files[years_back - 1]}",
                    ]
                )
                if granules:
                    for g in granules:
                        days = (g.acquisition_time - window.window_end).days
                        lines.append(
                            f"      {g.acquisition_time.isoformat()} ({'+' if days >= 0 else ''}{days}d): {g.granule_id}"
                        )
                else:
                    lines.append("      (No granules found)")
                lines.append("")

        lines.extend(
            [
                "=" * 80,
                f"Total baselines: {len(baseline_products)}",
                f"Total files selected: {total_files}",
                "=" * 80 + "\n",
            ]
        )

    else:
        # Batch mode
        lines.extend(["\n" + "=" * 80, "DIST-S1 Baseline Product Selection Results (Batch)", "=" * 80 + "\n"])
        grand_total_files = grand_total_baselines = 0
        for i, result in enumerate(results, 1):
            baseline_products = result["baseline_products"]
            result_total_files = sum(
                len(p["t0"]) + len(p["w1"]) + len(p["w2"]) + len(p["w3"]) for p in baseline_products.values()
            )
            lines.extend(
                [
                    f"[{i}/{len(results)}] {result['tile_id']},{result['reference_time'].isoformat()} {len(baseline_products)} baselines"
                ]
            )
            grand_total_files += result_total_files
            grand_total_baselines += len(baseline_products)

        # Add section for products needing retriggering (successful queries with sufficient inputs)
        lines.extend(
            [
                "",
                "Products Needing Retriggering:",
                "=" * 80,
            ]
        )

        if results:
            lines.append("Format: tile_id,acquisition_time")
            lines.append("")
            for result in results:
                tile_id = result["tile_id"]
                acq_time = result["reference_time"]
                # Add acquisition group if present
                if "acq_group" in result and result["acq_group"]:
                    formatted_tile_id = f"{tile_id}_{result['acq_group']}"
                else:
                    formatted_tile_id = tile_id
                # Format as tile_id,timestamp (same format as input for easy copy-paste)
                lines.append(f"{formatted_tile_id},{acq_time.strftime('%Y%m%dT%H%M%SZ')}")

            lines.append("")
            lines.append(f"Total products to retrigger: {len(results)}")
        else:
            lines.append("(None - all queried products have insufficient inputs)")

        lines.extend(
            [
                "=" * 80 + "\n",
            ]
        )

    return "\n".join(lines)

# CCSLC Deletion Utility

A utility for selective deletion of CCSLC (Compact Copied SLC) data to enable DISP-S1 reprocessing of specific frames. This tool is especially useful in workflows where certain data needs to be regenerated due to processing errors, updates in algorithms, or partial data corruption.

## Features

- **Complete Cleanup**: Deletes both S3 data files and OpenSearch metadata documents
- **Multiple Selection Criteria**: Delete CCSLC data by frame IDs, date ranges, burst IDs, or specific granule IDs
- **Dry-Run Mode**: Preview deletions without actually executing them (includes multiple safety checks to prevent accidental deletions)
- **Comprehensive Logging**: Detailed logging of all operations for traceability
- **Input Validation**: Validates frame IDs, burst IDs, and granule ID formats
- **Safe Deletion**: Requires explicit confirmation before deleting objects
- **Batch Operations**: Efficiently handles large numbers of objects
- **Integration**: Works with existing OPERA data management systems

## Installation

The utility is part of the OPERA PCM tools and requires the OPERA PCM environment to be set up. Ensure you have:

1. OPERA PCM installed and configured
2. AWS credentials configured for S3 access
3. Access to the LTS bucket containing CCSLC data

## Usage

### Basic Syntax

```bash
python tools/ccslc_deletion_utility.py [OPTIONS] COMMAND [ARGS]
```

### Global Options

- `--dry-run`: Preview deletions without executing them (recommended for first use)
- `--verbose`, `-v`: Enable verbose logging for detailed output
- `--help`: Show help message

### Commands

#### Delete by Frame IDs

Delete CCSLC data for specific frame IDs:

```bash
python tools/ccslc_deletion_utility.py frames --frame-ids 10859,10860
```

**Options:**
- `--frame-ids`: Comma-separated list of frame IDs (e.g., '10859,10860')

**Example:**
```bash
# Dry run to preview deletions
python tools/ccslc_deletion_utility.py frames --frame-ids 10859,10860 --dry-run

# Actual deletion
python tools/ccslc_deletion_utility.py frames --frame-ids 10859,10860
```

#### Delete by Date Range

Delete CCSLC data within a specific date range:

```bash
python tools/ccslc_deletion_utility.py date-range --start-date 2023-01-01 --end-date 2023-01-31
```

**Options:**
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format

**Example:**
```bash
# Delete all CCSLC data created in January 2023
python tools/ccslc_deletion_utility.py date-range --start-date 2023-01-01 --end-date 2023-01-31 --dry-run
```

#### Delete by Burst IDs

Delete CCSLC data for specific burst IDs:

```bash
python tools/ccslc_deletion_utility.py bursts --burst-ids T175-374393-IW1,T175-374394-IW1
```

**Options:**
- `--burst-ids`: Comma-separated list of burst IDs (e.g., 'T175-374393-IW1,T175-374394-IW1')

**Example:**
```bash
# Delete CCSLC data for specific bursts
python tools/ccslc_deletion_utility.py bursts --burst-ids T175-374393-IW1,T175-374394-IW1 --dry-run
```

#### Delete by Granule IDs

Delete CCSLC data for specific granule IDs:

```bash
python tools/ccslc_deletion_utility.py granules --granule-ids "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
```

**Options:**
- `--granule-ids`: Comma-separated list of granule IDs

**Example:**
```bash
# Delete specific CCSLC granules
python tools/ccslc_deletion_utility.py granules --granule-ids "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0" --dry-run
```

## CCSLC Data Structure

CCSLC (Compact Copied SLC) files follow a specific naming convention:

```
OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id}_{burst_id}_{ref_date}T000000Z_{first_date}T000000Z_{last_date}T000000Z_{creation_ts}_{pol}_{version}.h5
```

**Components:**
- `frame_id`: 5-digit frame number (e.g., F10859)
- `burst_id`: Burst identifier (e.g., T175-374393-IW1)
- `ref_date`: Reference date (YYYYMMDD format)
- `first_date`: First acquisition date (YYYYMMDD format)
- `last_date`: Last acquisition date (YYYYMMDD format)
- `creation_ts`: Product creation timestamp (YYYYMMDDTHHMMSSZ format)
- `pol`: Polarization (VV, VH, HH, HV, VV+VH, HH+HV)
- `version`: Product version (e.g., v1.0)

## Storage Location

CCSLC files are stored in S3 under:
```
s3://{LTS_BUCKET}/products/CSLC_S1_COMPRESSED/{granule_id}/{granule_id}.h5
```

## Safety Features

### Dry-Run Mode

Always use `--dry-run` first to preview what will be deleted:

```bash
python tools/ccslc_deletion_utility.py frames --frame-ids 10859 --dry-run
```

This will show you exactly which objects would be deleted without actually deleting them.

### Confirmation Prompts

When not in dry-run mode, the utility will:
1. Show a summary of objects to be deleted
2. Display the total size of data to be deleted
3. Require you to type 'yes' to confirm the deletion

### Deletion Summary

After completion, the utility displays a comprehensive summary:
- **Objects - Successful**: Number of individual files successfully deleted
- **Objects - Failed**: Number of individual files that failed to delete
- **Datasets deleted**: Number of complete CCSLC datasets (granule IDs) processed

This provides both granular (file-level) and high-level (dataset-level) visibility into the deletion operation.

### Input Validation

The utility validates all inputs:
- **Frame IDs**: Must exist in the DISP-S1 burst database
- **Burst IDs**: Must exist in the DISP-S1 burst database
- **Granule IDs**: Must follow the CCSLC naming convention
- **Date Ranges**: Must be valid dates in YYYY-MM-DD format

## Logging

The utility provides comprehensive logging:

- **INFO**: General operation information
- **WARNING**: Non-fatal issues (e.g., objects not found)
- **ERROR**: Fatal errors that prevent operation
- **DEBUG**: Detailed information (use `--verbose` flag)

Log messages include:
- Number of objects found
- Object details (filename, size, path)
- Deletion results (successful/failed)
- Error details

## Error Handling

The utility handles various error conditions:

- **Invalid Input**: Invalid frame IDs, burst IDs, or granule IDs
- **S3 Errors**: Network issues, permission problems, or bucket access
- **Configuration Errors**: Missing LTS_BUCKET configuration
- **User Cancellation**: Graceful handling of Ctrl+C or cancellation

## Examples

### Example 1: Delete CCSLC data for a specific frame

```bash
# First, preview what would be deleted
python tools/ccslc_deletion_utility.py frames --frame-ids 10859 --dry-run --verbose

# If the preview looks correct, perform the actual deletion
python tools/ccslc_deletion_utility.py frames --frame-ids 10859 --verbose
```

### Example 2: Delete CCSLC data for a date range

```bash
# Delete all CCSLC data created in the first week of 2023
python tools/ccslc_deletion_utility.py date-range --start-date 2023-01-01 --end-date 2023-01-07 --dry-run
```

### Example 3: Delete CCSLC data for specific bursts

```bash
# Delete CCSLC data for multiple bursts
python tools/ccslc_deletion_utility.py bursts --burst-ids "T175-374393-IW1,T175-374394-IW1,T175-374395-IW1" --dry-run
```

### Example 4: Delete specific CCSLC granules

```bash
# Delete specific CCSLC granules by their full granule IDs
python tools/ccslc_deletion_utility.py granules --granule-ids "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0,OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374394-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0" --dry-run
```

## Best Practices

1. **Always use dry-run first**: Preview deletions before executing them
2. **Use verbose logging**: Enable `--verbose` for detailed output
3. **Validate inputs**: Ensure frame IDs and burst IDs are correct
4. **Backup important data**: Consider backing up critical CCSLC data before deletion
5. **Monitor logs**: Check logs for any warnings or errors
6. **Test with small datasets**: Start with small frame sets or date ranges

## Troubleshooting

### Common Issues

**"LTS_BUCKET not configured"**
- Ensure your OPERA PCM configuration includes the LTS_BUCKET setting
- Check your settings.yaml or configuration files

**"Invalid frame ID"**
- Verify the frame ID exists in the DISP-S1 burst database
- Use the `disp_s1_burst_db_tool.py` to check available frame IDs

**"Invalid burst ID"**
- Verify the burst ID format (e.g., T175-374393-IW1)
- Check the DISP-S1 burst database for valid burst IDs

**"No objects found"**
- Verify the CCSLC data exists in the LTS bucket
- Check the date range or frame IDs are correct
- Ensure you have proper S3 permissions

**"Permission denied"**
- Verify your AWS credentials are configured
- Check you have delete permissions on the LTS bucket
- Ensure your AWS profile has the necessary permissions

### Getting Help

For additional help:

1. Use the `--help` flag for command-line help
2. Check the OPERA PCM documentation
3. Review the logs for detailed error information
4. Contact the OPERA team for support

## Integration with DISP-S1 Reprocessing

After deleting CCSLC data using this utility:

1. **Trigger Reprocessing**: Use the appropriate DISP-S1 reprocessing workflows
2. **Monitor Progress**: Check job status and logs
3. **Verify Results**: Ensure new CCSLC data is generated correctly
4. **Update Catalogs**: Update any data catalogs or indices as needed

## Security Considerations

- **Access Control**: Ensure only authorized users can run this utility
- **Audit Logging**: All operations are logged for audit purposes
- **Confirmation Required**: Deletions require explicit user confirmation
- **Dry-Run Mode**: Always preview deletions before execution

## Performance Optimizations

The utility includes several performance optimizations to minimize S3 API calls and improve efficiency:

### Frame-Based Prefix Optimization
When deleting by frame ID, the utility uses an optimized S3 prefix that includes the frame ID:
- **Before**: `products/CSLC_S1_COMPRESSED/` (searches all CCSLC objects)
- **After**: `products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id:05d}_` (searches only objects for specific frame)

This optimization can reduce the number of S3 objects scanned by orders of magnitude for large datasets.

### Burst-Based Frame Optimization
When deleting by burst ID, the utility leverages the DISP-S1 burst-to-frames mapping to search only relevant frame prefixes:
- Uses the burst database to identify which frames contain the burst
- Searches only those specific frame prefixes instead of all CCSLC objects
- Falls back to full search if burst ID not found in mapping

### Granule ID Direct Access
When deleting by granule ID, the utility constructs the exact S3 key and uses `head_object` for direct access:
- No need to scan multiple objects
- Immediate verification of object existence
- Fastest method for specific granule deletions

### S3 Object Structure Handling
The utility correctly handles the CCSLC S3 storage structure:
- **CCSLC objects are stored as directories** (ending with `/`)
- **Actual data files are `.h5` files within these directories**
- The utility automatically skips directory entries and non-.h5 files
- Only processes actual CCSLC data files for deletion

### OpenSearch Document Deletion
The utility provides complete cleanup by also deleting corresponding OpenSearch documents:
- **Index Pattern**: Searches the primary CCSLC index pattern:
  - `grq_1_l2_cslc_s1_compressed*` (Primary CCSLC index pattern)
- **Robust Search**: Uses wildcard pattern to find documents regardless of index naming conventions
- **Document Matching**: Deletes documents using the `_id` field which contains the granule ID (most reliable method)
- **Batch Processing**: Efficiently processes deletions for the index pattern
- **Error Handling**: Continues processing even if some indices don't exist

## Performance Considerations

- **Batch Operations**: The utility processes objects in batches for efficiency
- **S3 Pagination**: Uses S3 pagination to handle large numbers of objects
- **Memory Usage**: Minimal memory footprint for large datasets
- **Network Efficiency**: Optimized S3 operations to minimize network overhead


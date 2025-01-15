<!-- Header block for project -->
<hr>
<div align="center">
<h1 align="center">OPERA Validator</h1>
</div>
<pre align="center">Validates OPERA production and delivery to DAACs via verifying inputs / outputs mapping strategies.</pre>
<!-- Header block for project -->

[![SLIM](https://img.shields.io/badge/Best%20Practices%20from-SLIM-blue)](https://nasa-ammos.github.io/slim/)
<!-- ☝️ Add more badges via: https://shields.io ☝️ -->

This repository contains a Python script for querying the CMR (Common Metadata Repository) to help validate OPERA products such as DSWx-S1 and DISP-S1. The script takes in specific temporal ranges to identify RTC/CSLC bursts from CMR (or a granule file list) and generates a list of expected input granules that can be compared against expected generated products' input file lists. By comparing and ensure all expected input products show up as dependencies in all expected output products, we can verify that the right products with the right dependencies were produced. It includes functionality for handling large datasets, implementing efficient data fetching with exponential backoff and jittering, and standard output tabular results.

## Features

* Checks if all expected input products show up in output, generated products dependencies list (i.e. 'InputGranules' metadata)
* Parallelized CMR querying with customizable temporal range to obtain a list of input granule bursts.
* Optional file input for granule IDs to extract burst information from.
* Exponential backoff and jittering for CMR data fetching.
* Visual results display using Pandas and Tabulate.
* DSWx-S1 specific support:
  * Validation of DSWx-S1 products against input RTC bursts by verifying all expected RTCs show up in all expected DSWx-S1 'InputGranules' metadata.
  * Lookup of RTC burst IDs from a provided MGRS Tile Collection Database (SQLite).
* DISP-S1 specific support:
  * Validation of DISP-S1 products against input CSLC bursts by verifying all expected CSLCs show up in all expected DISP-S1 'InputGranules' metadata.
  * Support for proper mappings between frames and bursts via burst-to-frame and frame-to-burst databases.

## Contents

* [Quick Start](#quick-start)
* [Changelog](#changelog)
* [FAQ](#frequently-asked-questions-faq)
* [Contributing Guide](#contributing)
* [License](#license)
* [Support](#support)

## Quick Start

This guide provides a quick way to get started with the script.

### Requirements

* Python 3.6 or higher with special library dependencies (see `pip` command below)
* Access to the CMR API (i.e., network access to the internet)
* Availability of database files for DSWx-S1 or DISP-S1
* For DISP-S1, the disp_s1 product ES index must be available in GRQ.

### Setup Instructions

1. Clone the repository to your local machine.
2. Install the required Python libraries: `pip install pandas tabulate tqdm sqlite3 requests`.
3. Ensure you have internet access to the CMR API.

### Run Instructions

1. Run the script using Python: 
   ```bash
   python opera_validator.py --start <start_date> --end <end_date> --dswx_s1_mgrs_db <database_path> --product DSWx-S1
   ```
   
2. Optional arguments:
   - **`--timestamp`**: Specifies the type of timestamp to query CMR with. Example values: `TEMPORAL`, `REVISION`, `PRODUCTION`, `CREATED`. Default value is `TEMPORAL`.
   - **`--file`**: Path to an optional file containing granule IDs.
   - **`--threshold`**: Sets a threshold percentage to filter MGRS Tile Set coverages by. If both `--threshold` and `--matching_burst_count` are provided, `--threshold` takes precedence.
   - **`--matching_burst_count`**: Specifies the minimum number of bursts to expect a match for filtering results.
   - **`--verbose`**: Enables verbose mode to provide detailed output, such as matching granule or burst IDs.
   - **`--endpoint_daac_input`**: Specifies the CMR endpoint venue for RTC granules. Accepted values are `OPS` or `UAT`. Default is `OPS`.
   - **`--endpoint_daac_output`**: Specifies the CMR endpoint venue for DSWx-S1 granules. Accepted values are `OPS` or `UAT`. Default is `OPS`.
   - **`--validate`**: Validates if DSWx-S1 products have been delivered for the specified time range. Only applicable when using `--timestamp` in `TEMPORAL` mode.
   - **`--product`**: Specifies the product to validate. Accepted values are `DSWx-S1` or `DISP-S1`. Default is `DSWx-S1`.

### Usage Examples

* **Retrieve a list of MGRS Tile Set IDs for RTC burst processing in a given time range on CMR, and filter the results to show only MGRS Tile Sets that had coverage of greater than or equal to 50%.**
  ```bash
  python opera_validator.py --start "2023-12-05T01:00:00" --end "2023-12-05T03:59:59" --product DSWx-S1 --dswx_s1_mgrs_db MGRS_tile_collection_v0.3.sqlite --threshold 50 
  ```

* **Process and display data related to granule IDs from a file.**
  ```bash
  python opera_validator.py --file granules.txt --product DSWx-S1 --dswx_s1_mgrs_db MGRS_tile_collection_v0.3.sqlite --threshold 50
  ```

* **Validate whether DSWx-S1 processing has kept up with input RTC processing (success condition).**
  ```bash
  python opera_validator.py --product DSWx-S1 --endpoint_daac_output UAT --start "2024-05-12T08:00:00" --end "2024-05-12T08:59:00" --dswx_s1_mgrs_db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate
  ```

* **Validate whether DSWx-S1 processing has kept up with input RTC processing (failure condition).**
  ```bash
  python opera_validator.py --product DSWx-S1 --endpoint_daac_output UAT --start "2024-05-12T04:10:00" --end "2024-05-12T05:10:00" --dswx_s1_mgrs_db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate
  ```

### DISP-S1 Specific Usage

If you're validating DISP-S1 products, you'll need to use the following additional arguments:

- **`--processing_mode`**: Must be provided. "forward", "reprocessing", or "historical".
- **`--frame`**: Highly recommended for historical processing mode. The frame number to validate. If this is not specified this tool will query for all CSLC bursts in CMR over the time period which can potentially take hours.
- **`--validate_with_grq`**: Optional. Retrieve DISP-S1 products also from GRQ instead of CMR. This is useful when you've run a test without delivering DISP-S1 products to DAAC.

* **Validate DISP-S1 forward processing by revision time.**
  ```bash 
  python opera_validator.py --product DISP-S1 --timestamp REVISION --start 2024-12-15T08:00:00Z --end 2024-12-15T09:00:00Z --endpoint_daac_input OPS --endpoint_daac_output UAT --processing_mode=forward 
  ```
  
* **Validate DISP-S1 forward processing by revision time but compare entirely against GRQ instead of CMR.**
  ```bash 
  python opera_validator.py --product DISP-S1 --timestamp REVISION --start 2024-12-15T08:00:00Z --end 2024-12-15T09:00:00Z --endpoint_daac_input OPS --endpoint_daac_output UAT --processing_mode=forward --validate_with_grq
  ```
  
* **Validate DISP-S1 historical processing by temporal time.**
  ```bash 
  python opera_validator.py --product DISP-S1 --timestamp TEMPORAL --start 2016-12-01T08:00:00Z --end 2024-12-15T09:00:00Z --endpoint_daac_input OPS --endpoint_daac_output UAT --processing_mode=historical --frame=11116
  ```

* **Validate DISP-S1 historical processing by temporal time but compare entirely against GRQ instead of CMR.**
  ```bash 
  python opera_validator.py --product DISP-S1 --timestamp TEMPORAL --start 2016-12-01T08:00:00Z --end 2024-12-15T09:00:00Z --endpoint_daac_input OPS --endpoint_daac_output UAT --processing_mode=historical --frame=11116 --validate_with_grq 
  ```  

## Running Tests

This repository includes unit tests to verify the functionality of the OPERA Validator script. The tests are located in the `test_opera_validator.py` file.

### Running the PyTests

To run the PyTests, follow these steps:

1. **Install pytest**: If you haven't already, install pytest in your Python environment:
   ```bash
   pip install pytest
   ```

2. **Run the tests**: You can run all tests by executing the following command in the root directory of the project:
   ```bash
   pytest test_opera_validator.py
   ```

   This command will discover and run all the test cases defined in the `test_opera_validator.py` file.

3. **View test results**: After running the tests, pytest will display the results in the terminal, showing which tests passed, failed, or were skipped.

4. **Debugging**: If a test fails, pytest will provide detailed output about the failure, including the line number and the specific assertion that failed. You can then debug your code or test cases based on this information.

### Example Output
A typical test run might look like this:
```bash
=========== test session starts ===========
platform darwin -- Python 3.9.12, pytest-8.3.2, pluggy-1.5.0
rootdir: /
configfile: pytest.ini
plugins: mock-3.14.0
collected 4 items                                                                                                                    

test_opera_validator.py::test_get_burst_id PASSED [25%]
test_opera_validator.py::test_get_burst_sensing_datetime PASSED [50%]
test_opera_validator.py::test_generate_url_params PASSED [75%]
test_opera_validator.py::test_map_cslc_bursts_to_frames PASSED  

=========== 5 passed in 0.35s ===========
```

This output indicates that all 5 tests passed successfully.

## Frequently Asked Questions (FAQ)

- **Q: One or more of the Python dependencies installed via `pip install` are not available on my machine/environment.**
  
  A: Consider [building the dependency from source](https://devguide.python.org/getting-started/setup-building/) on your machine. For example, a reported missing dependency on an Apple M2 MacBook with Python 3.9.6 is `sqlite3` and requires a build from source following [this guide](https://til.simonwillison.net/sqlite/build-specific-sqlite-pysqlite-macos).

- **Q: How do I get more detailed output like matching granule or burst IDs when running the script?**
  
  A: Run in verbose mode by adding the `--verbose` flag on the command line.

## Support

Key points of contact are: [@riverma](https://github.com/riverma)

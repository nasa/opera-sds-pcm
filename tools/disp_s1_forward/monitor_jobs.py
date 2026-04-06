import time
from opensearchpy import OpenSearch, ConnectionError, exceptions

# --- Configuration ---
JOB_TYPE = "job-SCIFLO_L3_DISP_S1:develop" # <-- FIXED JOB TYPE
POLL_INTERVAL = 30  # Time to sleep between checks (in seconds)
OS_HOST = "http://localhost:9200" # Using http as per your ES example
OS_INDEX = "job_status-current"

# Define what you consider "running" statuses
RUNNING_STATUSES = ["job-queued", "job-started"]

# Define the "final" (completed or failed) statuses to check against
COMPLETED_STATUSES = ["job-completed", "job-failed"]
# ---------------------

def main():
    try:
        # 1. Create the OpenSearch client
        # Based on your previous examples, you're likely using unsecured HTTP
        os_client = OpenSearch(hosts=[OS_HOST])

        # 2. Test the connection
        if not os_client.ping():
            print(f"❌ Error: Could not connect to OpenSearch at {OS_HOST}")
            return
            
    except ConnectionError as e:
        print(f"❌ ConnectionError: Could not connect to OpenSearch at {OS_HOST}.")
        print(e)
        return

    print(f"✅ Connected to OpenSearch. Monitoring for job type: {JOB_TYPE}")
    
    # --- Phase 1: Find all jobs to monitor ---
    
    initial_query = {
      "query": {
        "bool": {
          "must": [
            { "term": { "type.keyword": JOB_TYPE }},
            { "terms": { "status.keyword": RUNNING_STATUSES }}
          ]
        }
      },
      "_source": ["job_id", "status"],
      "size": 1000 # Increase if you expect >1000 concurrent jobs
    }

    try:
        response = os_client.search(index=OS_INDEX, body=initial_query)
        
        # Use a set for efficient lookups and management
        job_ids_to_monitor = set()
        for hit in response['hits']['hits']:
            job_ids_to_monitor.add(hit['_source']['job_id'])

    except exceptions.OpenSearchException as e:
        print(f"❌ Error during initial search: {e}")
        return

    # Check if we found any jobs
    if not job_ids_to_monitor:
        print(f"No active jobs found for type {JOB_TYPE} with status: {RUNNING_STATUSES}")
        return

    print(f"Found {len(job_ids_to_monitor)} active job(s). Starting monitoring...")
    for job_id in job_ids_to_monitor:
        print(f" - {job_id}")
    print("---")

    # --- Phase 2: Poll for job completion ---
    
    try:
        while job_ids_to_monitor:
            # Build the 'terms' query to check only the jobs we care about
            poll_query = {
                "query": {
                    "terms": { "job_id.keyword": list(job_ids_to_monitor) }
                },
                "_source": ["job_id", "status"],
                "size": len(job_ids_to_monitor)
            }

            try:
                poll_response = os_client.search(index=OS_INDEX, body=poll_query)
                
                # Create a new set for jobs that are STILL running
                still_running_jobs = set()
                
                for hit in poll_response['hits']['hits']:
                    job_id = hit['_source']['job_id']
                    status = hit['_source']['status']
                    
                    # If the status is NOT one of the final states, add it back
                    if status not in COMPLETED_STATUSES:
                        still_running_jobs.add(job_id)
                
                # Update the set of jobs to monitor for the next loop iteration
                job_ids_to_monitor = still_running_jobs

                if job_ids_to_monitor:
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{timestamp}] Still monitoring {len(job_ids_to_monitor)} job(s). Sleeping for {POLL_INTERVAL}s...")
                    time.sleep(POLL_INTERVAL)
                
            except exceptions.OpenSearchException as e:
                print(f"❌ Error during polling search: {e}")
                print(f"Will retry in {POLL_INTERVAL} seconds...")
                time.sleep(POLL_INTERVAL)

        print("---")
        print("✅ All monitored jobs have completed.")

    except KeyboardInterrupt:
        print("\n🛑 Monitoring interrupted by user.")
        return

if __name__ == "__main__":
    main()

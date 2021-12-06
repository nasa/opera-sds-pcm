import sys

import hysds.task_worker
from hysds.user_rules_dataset import queue_dataset_evaluation


if __name__ == "__main__":
    id = sys.argv[1]
    crid = sys.argv[2]
    print("Submitting dataset evaluation via", str(hysds.task_worker.run_task))
    queue_dataset_evaluation({"id": id, "system_version": crid.lower()})

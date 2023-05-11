#!/usr/bin/env python3
import os
import time
import argparse
import requests
import subprocess

NUM_RETRIES = 12
RETRY_INTERVAL = 10

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate the log probability of mutated sequences.")
    parser.add_argument('run_template', type=str, default=None,
                        help="Path to script template to schedule on new jobs")
    parser.add_argument("--worker-id", type=str,
                        required=True, help="Unique ID of worker")
    parser.add_argument("--scheduler-url", type=str,
                        required=True, help="URL of scheduler")
    parser.add_argument("--s3-path", type=str, default='s3://markslab-private/eve',
                        help="Subpath of main bucket")
    parser.add_argument("--s3-project", type=str, default='default', metavar='V',
                        help="Project name (subfolder of s3-subpath).")
    args = parser.parse_args()

    with open(args.run_template) as f:
        run_template = f.read().strip()

    error = False
    attempts = 0
    while attempts < NUM_RETRIES:
        try:
            r = requests.post(f"{args.scheduler_url}/get-job",
                              json={"worker_id": args.worker_id})
        except requests.exceptions.ConnectionError as e:
            print(e)
            error = True
            attempts += 1
            time.sleep(RETRY_INTERVAL)
            continue

        if r.status_code != 200:
            error = True
            attempts += 1
            time.sleep(RETRY_INTERVAL)
            continue
        d = r.json()
        if 'status' not in d or d['status'] != 'OK' or 'index' not in d:
            error = True
            attempts += 1
            time.sleep(RETRY_INTERVAL)
            continue
        index = d['index']
        if index is None:
            print("No jobs left to run.")
            break

        attempts = 0

        run_script = run_template.format(protein_index=index)
        with open('run_job.sh', 'w') as f:
            f.write(run_script)
        print(f"Running job {index}:\n{run_script}")

        log_file = f'logs/{args.worker_id}_{index}_log.txt'
        # if "TMUX" in os.environ:
        #     r = subprocess.run(f"sleep 1 && tmux split-window tail -f '{log_file}'", shell=True)
        with open(log_file, 'w') as f:
            r = subprocess.run(['bash', 'run_job.sh'], stdout=f, stderr=f)
        if r.returncode != 0:
            print("Error detected. Syncing all logs to S3.")
            subprocess.run(['aws', 's3', 'sync', 'logs/',
                           f'{args.s3_path}/{args.s3_project}/logs/_failed_jobs/'])
        else:
            subprocess.run(['aws', 's3', 'sync', 'results/',
                           f'{args.s3_path}/{args.s3_project}/results/'])
            subprocess.run(['aws', 's3', 'sync', 'logs/',
                           f'{args.s3_path}/{args.s3_project}/logs/'])
        os.remove(log_file)

        update_attempts = 0
        while update_attempts < NUM_RETRIES:
            try:
                r = requests.post(
                    f"{args.scheduler_url}/update-job",
                    json={
                        "worker_id": args.worker_id,
                        "status": "FINISHED" if r.returncode == 0 else "FAILED",
                    }
                )
                if (
                    r.status_code == 200
                    and r.json()["status"] == "OK"
                ):
                    break
                else:
                    print(f"Failed to update job status: {r.text}")
            except requests.exceptions.ConnectionError as e:
                print(e)
            update_attempts += 1
            time.sleep(RETRY_INTERVAL)
        else:
            print("Failed to update job status.")
            error = True
            break
    if error:
        exit(1)

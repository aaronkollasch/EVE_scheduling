#!/usr/bin/env python3
# import os
import time
import argparse
import requests
import subprocess

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
    while attempts < 10:
        try:
            r = requests.post(f"{args.scheduler_url}/get-job",
                              json={"worker_id": args.worker_id})
        except requests.exceptions.ConnectionError:
            error = True
            attempts += 1
            time.sleep(5)
            continue

        if r.status_code == 200:
            d = r.json()
        else:
            error = True
            attempts += 1
            time.sleep(5)
            continue

        if d['status'] != 'OK' or d['index'] is None:
            attempts += 1
            time.sleep(5)
            continue

        attempts = 0
        index = d['index']
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
    if error:
        exit(1)

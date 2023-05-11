#!/usr/bin/env python3
import os
import sys
import time
import argparse
import socket
import json
import uuid
import base64
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import traceback
import itertools

import boto3
import botocore.exceptions

hostname = socket.gethostname()
SCHEDULER_IP = socket.gethostbyname(hostname)
SCHEDULER_PORT = 8080
print("Scheduler IP:", SCHEDULER_IP)

CONDA_ENV = "protein_env"
USERNAME = "ubuntu"
EVE_FOLDER = "EVE"
AWS_REGION = 'us-west-2'
# number of minutes to wait after completion before terminating the instance
POWEROFF_TIME = 5

home_path = f"/home/{USERNAME}"
seqdesign_path = f"{home_path}/SeqDesign"
eve_path = f"{home_path}/{EVE_FOLDER}"
eve_run_path = f"{eve_path}"
env_bin_path = f"{home_path}/anaconda3/envs/{CONDA_ENV}/bin"
userdata_template = f"""#!/bin/bash
su {USERNAME} -c '
cd {home_path}
git clone https://github.com/aaronkollasch/EVE_scheduling.git
cd {eve_path}
git remote add aaronkollasch https://github.com/aaronkollasch/EVE
git fetch aaronkollasch
git checkout -b custom aaronkollasch/master
mkdir {home_path}/s3_mnt
s3fs {{s3_bucket}} {home_path}/s3_mnt -o umask=0002 -o iam_role
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/MSA {eve_path}/data/MSA_s3
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/mappings {eve_path}/data/mappings_s3
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/weights {eve_path}/data/weights_s3
echo {{run_template}} | base64 -d > {eve_run_path}/run_template.sh
'

cd {eve_run_path}
cat <<'EOF' > run.sh
#!/bin/bash
source {home_path}/anaconda3/etc/profile.d/conda.sh
conda activate {CONDA_ENV}
cd {eve_run_path}
python {home_path}/EVE_scheduling/aws_train/aws_run_worker.py \
    --scheduler-url http://{SCHEDULER_IP}:{SCHEDULER_PORT} \
    --worker-id {{worker_uuid}} \
    --s3-path {{s3_path}} \
    --s3-project {{s3_project}} \
    {eve_run_path}/run_template.sh || EXIT_STATUS=$?
if [ $EXIT_STATUS -ne 0 ]; then
    echo "Error detected. Syncing all logs to S3."
    aws s3 sync {eve_run_path}/logs/ {{s3_path}}/{{s3_project}}/logs/_failed_jobs/
fi
echo "Shutting down in {POWEROFF_TIME} minutes, press Ctrl-C to interrupt."
sleep {POWEROFF_TIME * 60} && sudo poweroff
EOF

chown {USERNAME}:{USERNAME} run.sh
chmod +x run.sh
su - {USERNAME} -c "cd {eve_run_path}
tmux new-session -s train -d -n 'train' 'bash'
tmux pipe-pane -o -t train.0 'cat >> {eve_run_path}/logs/tmux-output.#h.txt'
tmux send -t train.0 './run.sh' ENTER
"
"""


def check_instance_status(instance_id):
    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    response = ec2.describe_instance_status(
        InstanceIds=[instance_id],
        IncludeAllInstances=True,
    )
    if len(response['InstanceStatuses']) == 0:
        return "terminated"
    return response['InstanceStatuses'][0]['InstanceState']['Name']


def launch_worker(args, name, run_template, worker_uuid, s3_path):
    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    cw = boto3.client('cloudwatch', region_name=AWS_REGION)
    print(
        f"Launching {'spot' if args.spot else 'on-demand'} instance {name} {worker_uuid} with commands:")
    userdata = userdata_template.format(
        run_template=base64.b64encode(
            run_template.encode('utf-8')).decode('utf-8'),
        worker_uuid=worker_uuid,
        s3_path=s3_path,
        s3_project=args.s3_project,
        s3_bucket=args.s3_bucket,
        s3_subpath=args.s3_subpath,
    )
    try:
        instance_options = dict(
            LaunchTemplate={"LaunchTemplateName": "EVETrain"},
            InstanceType=args.instance_type,
            UserData=userdata,
            TagSpecifications=[
                {"ResourceType": "instance", "Tags": [
                    {"Key": "Name", "Value": name}]},
                {"ResourceType": "volume", "Tags": [
                    {"Key": "Name", "Value": name}]},
            ],
            InstanceInitiatedShutdownBehavior="terminate",
            MinCount=1,
            MaxCount=1,
            DryRun=args.dry_run,
        )
        if args.spot:
            instance_options["InstanceMarketOptions"] = {
                'MarketType': 'spot',
                'SpotOptions': {
                    'SpotInstanceType': 'one-time',
                    'InstanceInterruptionBehavior': 'terminate',
                }
            }
        response = ec2.run_instances(**instance_options)
        if args.alarm and not args.dry_run:
            instance_id = response['Instances'][0]['InstanceId']
            try:
                response2 = ec2.describe_instance_types(
                    InstanceTypes=[args.instance_type])
                core_count = response2['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
            except (IndexError, KeyError, botocore.exceptions.BotoCoreError) as e:
                print(e)
                if args.instance_type == 'p2.xlarge':
                    core_count = 4
                else:
                    core_count = 8
                print(f"Using core count: {core_count}")
            threshold = round(1.4 / core_count * 100, 2)
            response3 = cw.put_metric_alarm(
                AlarmName=name+'_min_cpu_util',
                MetricName='CPUUtilization',
                Namespace='AWS/EC2',
                Period=300,
                Statistic='Average',
                EvaluationPeriods=2,
                DatapointsToAlarm=2,
                ComparisonOperator='LessThanThreshold',
                Threshold=threshold,
                TreatMissingData='missing',
                ActionsEnabled=False,
                AlarmDescription=f'Alarm when server CPU is below {threshold}% for 10 minutes',
                Dimensions=[
                    {
                        'Name': 'InstanceId',
                        'Value': instance_id,
                    },
                ],
            )
            if response3['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Alarm set for {name} at {threshold}% CPU utilization.")
            else:
                print(response3)
        print("Launched.")
        return response['Instances'][0]['InstanceId']
    except Exception as e:
        print(e)


class Scheduler(BaseHTTPRequestHandler):
    def _set_headers(self, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        self._set_headers(405)
        self.wfile.write(json.dumps(
            {"status": "ERROR", "message": "GET not supported."}).encode('utf-8'))

    def do_POST(self):
        try:
            parsed_path = urlparse(self.path)

            # /get-job
            # Request: {"worker_id": "uuid"}
            # Response: {"status": "OK", "index": int|null}
            if parsed_path.path == "/get-job":
                rdata = json.loads(self.rfile.read(
                    int(self.headers['Content-Length'])))
                worker_id = rdata['worker_id']
                worker = self.server.worker_database["workers"][worker_id]
                if worker["current_index"] is None and len(self.server.protein_indices) > 0:
                    current_index = self.server.protein_indices.pop(0)
                    worker["current_index"] = current_index
                    print(
                        f"Worker {worker_id} assigned index {current_index}.", file=sys.stderr)
                    self.server.save_database()
                self._set_headers()
                self.wfile.write(json.dumps(
                    {"status": "OK", "index": worker['current_index']}).encode('utf-8'))

            # /update-job
            # Request: {"worker_id": "uuid", "status": "FINISHED"|"FAILED"}
            # Response: {"status": "OK"}
            elif parsed_path.path == "/update-job":
                rdata = json.loads(self.rfile.read(
                    int(self.headers['Content-Length'])))
                worker_id = rdata['worker_id']
                worker = self.server.worker_database["workers"][worker_id]
                if worker.get("current_index") is None:
                    print(
                        f"Worker {worker_id} sent update with no current index.", file=sys.stderr)
                    self._set_headers(400)
                    self.wfile.write(json.dumps(
                        {"status": "ERROR", "message": "No current index."}).encode('utf-8'))
                elif rdata['status'] == "FINISHED":
                    print(
                        f"Worker {worker_id} finished index {worker['current_index']}.", file=sys.stderr)
                    worker["index_history"].append(worker["current_index"])
                    self.server.worker_database["finished_indices"].append(
                        worker["current_index"])
                    worker["current_index"] = None
                    self.server.save_database()
                    self._set_headers()
                    self.wfile.write(json.dumps(
                        {"status": "OK"}).encode('utf-8'))
                elif rdata['status'] == "FAILED":
                    print(
                        f"Worker {worker_id} failed on index {worker['current_index']}.", file=sys.stderr)
                    worker["index_history"].append(worker["current_index"])
                    self.server.worker_database["error_indices"].append(
                        worker["current_index"])
                    worker["current_index"] = None
                    self.server.save_database()
                    self._set_headers()
                    self.wfile.write(json.dumps(
                        {"status": "OK"}).encode('utf-8'))
                else:
                    print(
                        f"Unrecognized status {rdata['status']} from worker {worker_id}.", file=sys.stderr)
                    self._set_headers(400)
                    self.wfile.write(json.dumps(
                        {"status": "ERROR", "message": "Unrecognized status."}).encode('utf-8'))

            else:
                self._set_headers(404)
                self.wfile.write(json.dumps(
                    {"status": "ERROR", "message": "Unknown path."}).encode('utf-8'))
        except Exception:
            self._set_headers(500)
            self.wfile.write(json.dumps(
                {"status": "ERROR", "message": "Internal server error."}).encode('utf-8'))
            traceback.print_exc(file=sys.stderr)

    def do_HEAD(self):
        self._set_headers()


if __name__ == "__main__":
    sys.path.append(seqdesign_path)
    from seqdesign import aws_utils

    parser = argparse.ArgumentParser(
        description="Launch an EVE job scheduler on AWS")
    parser.add_argument('run_template', type=str, default=None,
                        help="Path to script template to schedule on new jobs")
    parser.add_argument('database_path', type=str,
                        default="job_database.json", help="Path to save/load job database")
    parser.add_argument("--protein-index", type=str, default="0",
                        help="Index (or range) of protein to train")
    parser.add_argument("--num-workers", type=int, default=5,
                        help="Number of workers to request")
    parser.add_argument("--instance-type", type=str, default='p2.xlarge', metavar='TYPE',
                        help="AWS instance type (e.g. p2.xlarge)")
    parser.add_argument("--alarm", action='store_true',
                        help="Add a minimum CPU utilization alarm")
    parser.add_argument("--dry-run", action='store_true',
                        help="Perform a dry run")
    parser.add_argument("--spot", action='store_true',
                        help="Request a spot instance")
    parser.add_argument("--s3-bucket", type=str, default='markslab-private',
                        help="s3 bucket")
    parser.add_argument("--s3-subpath", type=str, default='eve',
                        help="Subpath of main bucket")
    parser.add_argument("--s3-project", type=str, default='default', metavar='V',
                        help="Project name (subfolder of s3-subpath).")
    args = parser.parse_args()

    s3_path = f"s3://{args.s3_bucket}/{args.s3_subpath}"

    aws_util = aws_utils.AWSUtility(
        s3_project=args.s3_project, s3_base_path=s3_path)
    aws_util.s3_sync(
        local_folder=f"{home_path}/EVE_scheduling/aws_train/",
        s3_folder="scheduling/aws_train/",
        destination='s3',
        args=("--exclude", "*.py"),
    )

    protein_indices = args.protein_index
    if '-' in protein_indices:
        protein_indices = protein_indices.split('-')
        protein_indices = list(
            range(int(protein_indices[0]), int(protein_indices[1])+1))
    else:
        protein_indices = [int(protein_indices)]

    if args.run_template is None:
        run_template = [
            ("python train_VAE.py "
             "--MSA_data_folder ./data/MSA "
             "--MSA_list ./data/mappings/example_mapping.csv "
             "--protein_index {protein_index} "
             "--MSA_weights_location ./data/weights "
             "--VAE_checkpoint_location ./results/VAE_parameters "
             "--model_name_suffix Jan1_PTEN_example "
             "--model_parameters_location ./EVE/default_model_params.json "
             "--training_logs_location ./logs/"),
        ]
        print("Usage: aws_run_scheduler.py [template] ...")
        print("Running test in 5 seconds (Press Ctrl-C to cancel).")
        time.sleep(5)
    else:
        with open(args.run_template) as f:
            run_template = f.read().strip()

    if os.path.exists(args.database_path):
        with open(args.database_path) as f:
            worker_database = json.load(f)
        for index in itertools.chain(
            worker_database["finished_indices"],
            worker_database["error_indices"],
            (worker["current_index"] for worker in worker_database["workers"].values()),
        ):
            if index in protein_indices:
                protein_indices.remove(index)
    else:
        worker_database = {
            "finished_indices": [],
            "error_indices": [],
            "workers": {},
        }
    for i_worker in range(min(args.num_workers, len(protein_indices))):
        worker_name = f"eve_train_{i_worker}"
        worker_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, worker_name))
        worker_database["workers"].setdefault(worker_uuid, {})
        worker_database["workers"][worker_uuid].setdefault("worker_name", worker_name)
        worker_database["workers"][worker_uuid].setdefault("worker_uuid", worker_uuid)
        worker_database["workers"][worker_uuid].setdefault("instance_id", None)
        worker_database["workers"][worker_uuid].setdefault("index_history", [])
        worker_database["workers"][worker_uuid].setdefault("current_index", None)
        if (
                worker_database["workers"][worker_uuid]["instance_id"] is None or
                aws_util.check_instance_status(
                    worker_database["workers"][worker_uuid]["instance_id"]
                ) not in ["running", "pending"]
        ):
            try:
                instance_id = launch_worker(
                    args=args,
                    name=worker_name,
                    worker_uuid=worker_uuid,
                    run_template=run_template,
                    s3_path=s3_path,
                )
                worker_database["workers"][worker_uuid]["instance_id"] = instance_id
            except botocore.exceptions.ClientError as e:
                print(e)
                break
    print("workers:", worker_database)
    print("protein_indices:", protein_indices)

    server_address = ('', SCHEDULER_PORT)
    server = HTTPServer(server_address, Scheduler)

    def save_database():
        with open(args.database_path, 'w') as f:
            json.dump(worker_database, f)
    server.save_database = save_database
    server.worker_database = worker_database
    server.protein_indices = protein_indices
    save_database()

    print(f"Starting scheduler on http://{SCHEDULER_IP}:{SCHEDULER_PORT}")
    server.serve_forever()

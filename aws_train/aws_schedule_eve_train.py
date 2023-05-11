#!/usr/bin/env python3
import sys
import time
import boto3
import botocore.exceptions
import argparse

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
cd {eve_path}
git remote add aaronkollasch https://github.com/aaronkollasch/EVE
git fetch aaronkollasch
git checkout -b custom aaronkollasch/master
mkdir {home_path}/s3_mnt
s3fs {{s3_bucket}} {home_path}/s3_mnt -o umask=0002 -o iam_role
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/MSA {eve_path}/data/MSA_s3
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/mappings {eve_path}/data/mappings_s3
ln -s {home_path}/s3_mnt/{{s3_subpath}}/{{s3_project}}/data/weights {eve_path}/data/weights_s3
'

cd {eve_run_path}
cat <<'EOF' > run.sh
#!/bin/bash
source {home_path}/anaconda3/etc/profile.d/conda.sh
conda activate {CONDA_ENV}
cd {eve_run_path}
EXIT_STATUS=0
{{run_strings}}
if [ $EXIT_STATUS -ne 0 ]; then
    echo "Error detected. Syncing all logs to S3."
    aws s3 sync {eve_run_path}/logs/ {{s3_path}}/{{s3_project}}/logs/_failed_jobs/
    aws s3 sync {eve_run_path}/results/ {{s3_path}}/{{s3_project}}/results/_failed_jobs/
else
    echo "Job successful. Syncing data to S3."
    aws s3 sync {eve_run_path}/results/ {{s3_path}}/{{s3_project}}/results/
    cd {eve_run_path}/logs && run_name="$(find -maxdepth 1 -name '*_losses.csv' | head -n 1 | sed -e 's/.\///' -e 's/_losses.csv//')" && cd {home_path}
    aws s3 cp {eve_run_path}/logs/${{{{run_name}}}}_losses.csv {{s3_path}}/{{s3_project}}/logs/
    aws s3 cp "$(find {eve_run_path}/logs/ -maxdepth 1 -name 'tmux-output*.txt' | head -n 1)" {{s3_path}}/{{s3_project}}/logs/${{{{run_name}}}}_log.txt
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

if __name__ == "__main__":
    sys.path.append(seqdesign_path)
    from seqdesign import aws_utils

    parser = argparse.ArgumentParser(
        description="Calculate the log probability of mutated sequences.")
    parser.add_argument('script', type=str, nargs='+', default=[],
                        help="Script(s) to schedule on new instances")
    parser.add_argument("--instance-type", type=str, default='p2.xlarge', metavar='TYPE',
                        help="AWS instance type (e.g. p2.xlarge)")
    parser.add_argument("--split-lines", action='store_true',
                        help="Run every line in a separate instance")
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

    if args.script is None:
        names = ["test_scheduler"]
        run_strings = [
            ("python train_VAE.py "
             "--MSA_data_folder ./data/MSA "
             "--MSA_list ./data/mappings/example_mapping.csv "
             "--protein_index 0 "
             "--MSA_weights_location ./data/weights "
             "--VAE_checkpoint_location ./results/VAE_parameters "
             "--model_name_suffix Jan1_PTEN_example "
             "--model_parameters_location ./EVE/default_model_params.json "
             "--training_logs_location ./logs/"),
        ]
        print("Usage: aws_schedule_train.py [script1] [script2] ...")
        print("Running test in 5 seconds (Press Ctrl-C to cancel).")
        time.sleep(5)
    else:
        names = args.script
        run_strings = []
        for fname in names:
            with open(fname) as f:
                run_strings.append(f.read().strip())
        names = [name.split('/')[-1] for name in names]
        names = [name[:name.rfind('.')] for name in names]

    if args.split_lines:
        new_names, new_strings = [], []
        for name, run_string in zip(names, run_strings):
            for i, line in enumerate(run_string.splitlines(keepends=False)):
                if line.lstrip().startswith('#') or not line.strip():
                    continue
                new_names.append(f'{name}_{i}')
                new_strings.append(line)
        names, run_strings = new_names, new_strings

    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    cw = boto3.client('cloudwatch', region_name=AWS_REGION)
    for name, run_string in zip(names, run_strings):
        print(
            f"Launching {'spot' if args.spot else 'on-demand'} instance {name} with commands:")
        print(run_string)
        run_string = '\n'.join([
            f"{line.strip()} || EXIT_STATUS=$?"
            for line in run_string.splitlines(keepends=False)
            if line.strip()
        ])
        userdata = userdata_template.format(
            run_strings=run_string,
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
            print("Launched.")
        except Exception as e:
            print(e)
    print("Done.")

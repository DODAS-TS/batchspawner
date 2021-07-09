from batchspawner import UserEnvMixin, BatchSpawnerRegexStates


class RemoteSlurmSpawner(UserEnvMixin, BatchSpawnerRegexStates):
    batch_script = Unicode(
        """#!/bin/bash
#SBATCH --output={{homedir}}/jupyterhub_slurmspawner_%j.log
#SBATCH --job-name=spawner-jupyterhub
#SBATCH --chdir={{homedir}}
#SBATCH --export={{keepvars}}
#SBATCH --get-user-env=L
{% if partition  %}#SBATCH --partition={{partition}}
{% endif %}{% if runtime    %}#SBATCH --time={{runtime}}
{% endif %}{% if memory     %}#SBATCH --mem={{memory}}
{% endif %}{% if gres       %}#SBATCH --gres={{gres}}
{% endif %}{% if nprocs     %}#SBATCH --cpus-per-task={{nprocs}}
{% endif %}{% if reservation%}#SBATCH --reservation={{reservation}}
{% endif %}{% if options    %}#SBATCH {{options}}{% endif %}

set -euo pipefail

trap 'echo SIGTERM received' TERM
{{prologue}}
which jupyterhub-singleuser
{% if srun %}{{srun}} {% endif %}{{cmd}}
echo "jupyterhub-singleuser ended gracefully"
{{epilogue}}
"""
    ).tag(config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_cluster = Unicode(
        "",
        help="Cluster name to submit job to resource manager",
    ).tag(config=True)

    req_qos = Unicode(
        "",
        help="QoS name to submit job to resource manager",
    ).tag(config=True)

    req_srun = Unicode(
        "srun",
        help="Set req_srun='' to disable running in job step, and note that "
        "this affects environment handling.  This is effectively a "
        "prefix for the singleuser command.",
    ).tag(config=True)

    req_reservation = Unicode(
        "",
        help="Reservation name to submit to resource manager",
    ).tag(config=True)

    req_gres = Unicode(
        "",
        help="Additional resources (e.g. GPUs) requested",
    ).tag(config=True)

    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode("sbatch --parsable").tag(config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode("squeue -h -j {job_id} -o '%T %B'").tag(config=True)
    batch_cancel_cmd = Unicode("scancel {job_id}").tag(config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    #  RUNNING,  COMPLETING = running
    state_pending_re = Unicode(r"^(?:PENDING|CONFIGURING)").tag(config=True)
    state_running_re = Unicode(r"^(?:RUNNING|COMPLETING)").tag(config=True)
    state_unknown_re = Unicode(
        r"^slurm_load_jobs error: (?:Socket timed out on send/recv|Unable to contact slurm controller)"
    ).tag(config=True)
    state_exechost_re = Unicode(r"\s+((?:[\w_-]+\.?)+)$").tag(config=True)

    def parse_job_id(self, output):
        # make sure jobid is really a number
        try:
            # use only last line to circumvent slurm bug
            output = output.splitlines()[-1]
            id = output.split(";")[0]
            int(id)
        except Exception as e:
            self.log.error("SlurmSpawner unable to parse job ID from text: " + output)
            raise e
        return id
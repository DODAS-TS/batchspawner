# Copyright (c) Regents of the University of Minnesota
# Copyright (c) Michael Gilbert
# Distributed under the terms of the Modified BSD License.

"""Batch spawners

This file contains an abstraction layer for batch job queueing systems, and implements
Jupyterhub spawners for Torque, SLURM, and eventually others.

Common attributes of batch submission / resource manager environments will include notions of:
  * queue names, resource manager addresses
  * resource limits including runtime, number of processes, memory
  * singleuser child process running on (usually remote) host not known until runtime
  * job submission and monitoring via resource manager utilities
  * remote execution via submission of templated scripts
  * job names instead of PIDs
"""
import asyncio
import subprocess
from async_generator import async_generator, yield_, yield_from_
import pwd
import os
import re
import sys

import xml.etree.ElementTree as ET

from enum import Enum

from jinja2 import Template

from tornado import gen
from tornado.process import Subprocess
from subprocess import CalledProcessError
from tornado.iostream import StreamClosedError

from jupyterhub.spawner import Spawner
from jupyterhub.traitlets import Command
from traitlets import Integer, Unicode, Float, Dict, default

from jupyterhub.utils import random_port
from jupyterhub.spawner import set_user_setuid
import jupyterhub


def format_template(template, *args, **kwargs):
    """Format a template, either using jinja2 or str.format().

    Use jinja2 if the template is a jinja2.Template, or contains '{{' or
    '{%'.  Otherwise, use str.format() for backwards compatability with
    old scripts (but you can't mix them).
    """
    if isinstance(template, Template):
        return template.render(*args, **kwargs)
    elif "{{" in template or "{%" in template:
        return Template(template).render(*args, **kwargs)
    return template.format(*args, **kwargs)


class JobStatus(Enum):
    NOTFOUND = 0
    RUNNING = 1
    PENDING = 2
    UNKNOWN = 3


class BatchSpawnerBase(Spawner):
    """Base class for spawners using resource manager batch job submission mechanisms

    This base class is developed targetting the TorqueSpawner and SlurmSpawner, so by default
    assumes a qsub-like command that reads a script from its stdin for starting jobs,
    a qstat-like command that outputs some data that can be parsed to check if the job is running
    and on what remote node, and a qdel-like command to cancel a job. The goal is to be
    sufficiently general that a broad range of systems can be supported with minimal overrides.

    At minimum, subclasses should provide reasonable defaults for the traits:
        batch_script
        batch_submit_cmd
        batch_query_cmd
        batch_cancel_cmd

    and must provide implementations for the methods:
        state_ispending
        state_isrunning
        state_gethost
    """

    # override default since batch systems typically need longer
    start_timeout = Integer(300).tag(config=True)

    # override default server ip since batch jobs normally running remotely
    ip = Unicode(
        "0.0.0.0",
        help="Address for singleuser server to listen at",
    ).tag(config=True)

    exec_prefix = Unicode(
        "sudo -E -u {username}",
        help="Standard executon prefix (e.g. the default sudo -E -u {username})",
    ).tag(config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_queue = Unicode(
        "",
        help="Queue name to submit job to resource manager",
    ).tag(config=True)

    req_host = Unicode(
        "",
        help="Host name of batch server to submit job to resource manager",
    ).tag(config=True)

    req_memory = Unicode(
        "",
        help="Memory to request from resource manager",
    ).tag(config=True)

    req_nprocs = Unicode(
        "",
        help="Number of processors to request from resource manager",
    ).tag(config=True)

    req_ngpus = Unicode(
        "",
        help="Number of GPUs to request from resource manager",
    ).tag(config=True)

    req_runtime = Unicode(
        "",
        help="Length of time for submitted job to run",
    ).tag(config=True)

    req_partition = Unicode(
        "",
        help="Partition name to submit job to resource manager",
    ).tag(config=True)

    req_account = Unicode(
        "",
        help="Account name string to pass to the resource manager",
    ).tag(config=True)

    req_options = Unicode(
        "",
        help="Other options to include into job submission script",
    ).tag(config=True)

    req_prologue = Unicode(
        "",
        help="Script to run before single user server starts.",
    ).tag(config=True)

    req_epilogue = Unicode(
        "",
        help="Script to run after single user server ends.",
    ).tag(config=True)

    req_username = Unicode()

    @default("req_username")
    def _req_username_default(self):
        return self.user.name

    # Useful IF getpwnam on submit host returns correct info for exec host
    req_homedir = Unicode()

    @default("req_homedir")
    def _req_homedir_default(self):
        return pwd.getpwnam(self.user.name).pw_dir

    req_keepvars = Unicode()

    @default("req_keepvars")
    def _req_keepvars_default(self):
        return ",".join(self.get_env().keys())

    req_keepvars_extra = Unicode(
        help="Extra environment variables which should be configured, "
        "added to the defaults in keepvars, "
        "comma separated list.",
    )

    batch_script = Unicode(
        "",
        help="Template for job submission script. Traits on this class named like req_xyz "
        "will be substituted in the template for {xyz} using string.Formatter. "
        "Must include {cmd} which will be replaced with the jupyterhub-singleuser command line.",
    ).tag(config=True)

    batchspawner_singleuser_cmd = Unicode(
        "batchspawner-singleuser",
        help="A wrapper which is capable of special batchspawner setup: currently sets the port on "
        "the remote host.  Not needed to be set under normal circumstances, unless path needs "
        "specification.",
    ).tag(config=True)

    # Raw output of job submission command unless overridden
    job_id = Unicode()

    # Will get the raw output of the job status command unless overridden
    job_status = Unicode()

    # Prepare substitution variables for templates using req_xyz traits
    def get_req_subvars(self):
        reqlist = [t for t in self.trait_names() if t.startswith("req_")]
        subvars = {}
        for t in reqlist:
            subvars[t[4:]] = getattr(self, t)
        if subvars.get("keepvars_extra"):
            subvars["keepvars"] += "," + subvars["keepvars_extra"]
        return subvars

    batch_submit_cmd = Unicode(
        "",
        help="Command to run to submit batch scripts. Formatted using req_xyz traits as {xyz}.",
    ).tag(config=True)

    def parse_job_id(self, output):
        "Parse output of submit command to get job id."
        return output

    def cmd_formatted_for_batch(self):
        """The command which is substituted inside of the batch script"""
        return " ".join([self.batchspawner_singleuser_cmd] + self.cmd + self.get_args())

    async def run_command(self, cmd, input=None, env=None):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        inbytes = None

        if input:
            inbytes = input.encode()

        try:
            out, eout = await proc.communicate(input=inbytes)
        except:
            self.log.debug("Exception raised when trying to run command: %s" % cmd)
            proc.kill()
            self.log.debug("Running command failed, killed process.")
            try:
                out, eout = await asyncio.wait_for(proc.communicate(), timeout=2)
                out = out.decode().strip()
                eout = eout.decode().strip()
                self.log.error("Subprocess returned exitcode %s" % proc.returncode)
                self.log.error("Stdout:")
                self.log.error(out)
                self.log.error("Stderr:")
                self.log.error(eout)
                raise RuntimeError(
                    "{} exit status {}: {}".format(cmd, proc.returncode, eout)
                )
            except asyncio.TimeoutError:
                self.log.error(
                    "Encountered timeout trying to clean up command, process probably killed already: %s"
                    % cmd
                )
                return ""
            except:
                self.log.error(
                    "Encountered exception trying to clean up command: %s" % cmd
                )
                raise
        else:
            eout = eout.decode().strip()
            err = proc.returncode
            if err != 0:
                self.log.error("Subprocess returned exitcode %s" % err)
                self.log.error(eout)
                raise RuntimeError(eout)

        out = out.decode().strip()
        return out

    async def _get_batch_script(self, **subvars):
        """Format batch script from vars"""
        # Could be overridden by subclasses, but mainly useful for testing
        return format_template(self.batch_script, **subvars)

    async def submit_batch_script(self):
        subvars = self.get_req_subvars()
        # `cmd` is submitted to the batch system
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_submit_cmd, **subvars),
            )
        )
        # `subvars['cmd']` is what is run _inside_ the batch script,
        # put into the template.
        subvars["cmd"] = self.cmd_formatted_for_batch()
        if hasattr(self, "user_options"):
            subvars.update(self.user_options)
        script = await self._get_batch_script(**subvars)
        self.log.info("Spawner submitting job using " + cmd)
        self.log.info("Spawner submitted script:\n" + script)
        out = await self.run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info("Job submitted. cmd: " + cmd + " output: " + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error("Job submission failed with exit code " + out)
            self.job_id = ""
        return

    # Override if your batch system needs something more elaborate to query the job status
    batch_query_cmd = Unicode(
        "",
        help="Command to run to query job status. Formatted using req_xyz traits as {xyz} "
        "and self.job_id as {job_id}.",
    ).tag(config=True)

    async def query_job_status(self):
        """Check job status, return JobStatus object."""
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ""
            return JobStatus.NOTFOUND
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_query_cmd, **subvars),
            )
        )
        self.log.debug("Spawner querying job: " + cmd)
        try:
            self.job_status = await self.run_command(cmd)
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.job_status = e.args[0]
        except:
            self.log.error("Error querying job " + self.job_id)
            self.job_status = ""

        if self.state_isrunning():
            return JobStatus.RUNNING
        elif self.state_ispending():
            return JobStatus.PENDING
        elif self.state_isunknown():
            return JobStatus.UNKNOWN
        else:
            return JobStatus.NOTFOUND

    batch_cancel_cmd = Unicode(
        "",
        help="Command to stop/cancel a previously submitted job. Formatted like batch_query_cmd.",
    ).tag(config=True)

    async def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_cancel_cmd, **subvars),
            )
        )
        self.log.info("Cancelling job " + self.job_id + ": " + cmd)
        await self.run_command(cmd)

    def load_state(self, state):
        """load job_id from state"""
        super(BatchSpawnerBase, self).load_state(state)
        self.job_id = state.get("job_id", "")
        self.job_status = state.get("job_status", "")

    def get_state(self):
        """add job_id to state"""
        state = super(BatchSpawnerBase, self).get_state()
        if self.job_id:
            state["job_id"] = self.job_id
        if self.job_status:
            state["job_status"] = self.job_status
        return state

    def clear_state(self):
        """clear job_id state"""
        super(BatchSpawnerBase, self).clear_state()
        self.job_id = ""
        self.job_status = ""

    def make_preexec_fn(self, name):
        """make preexec fn to change uid (if running as root) before job submission"""
        return set_user_setuid(name)

    def state_ispending(self):
        "Return boolean indicating if job is still waiting to run, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_isrunning(self):
        "Return boolean indicating if job is running, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    def state_isunknown(self):
        "Return boolean indicating if job state retrieval failed because of the resource manager"
        return None

    def state_gethost(self):
        "Return string, hostname or addr of running job, likely by parsing self.job_status"
        raise NotImplementedError("Subclass must provide implementation")

    async def poll(self):
        """Poll the process"""
        status = await self.query_job_status()
        if status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.UNKNOWN):
            return None
        else:
            self.clear_state()
            return 1

    startup_poll_interval = Float(
        0.5,
        help="Polling interval (seconds) to check job state during startup",
    ).tag(config=True)

    async def start(self):
        """Start the process"""
        self.ip = self.traits()["ip"].default_value
        self.port = self.traits()["port"].default_value

        if jupyterhub.version_info >= (0, 8) and self.server:
            self.server.port = self.port

        await self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure (no jobid in output)"
            )
        while True:
            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await gen.sleep(self.startup_poll_interval)

        self.ip = self.state_gethost()
        while self.port == 0:
            await gen.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, "mock_port"):
                self.port = self.mock_port

        if jupyterhub.version_info < (0, 7):
            # store on user for pre-jupyterhub-0.7:
            self.user.server.port = self.port
            self.user.server.ip = self.ip
        self.db.commit()
        self.log.info(
            "Notebook server job {0} started at {1}:{2}".format(
                self.job_id, self.ip, self.port
            )
        )

        return self.ip, self.port

    # TODO: stop with paramiko
    async def stop(self, now=False):
        """Stop the singleuser server job.

        Returns immediately after sending job cancellation command if now=True, otherwise
        tries to confirm that job is no longer running."""

        self.log.info("Stopping server job " + self.job_id)
        await self.cancel_batch_job()
        if now:
            return
        for _ in range(10):
            status = await self.query_job_status()
            if status not in (JobStatus.RUNNING, JobStatus.UNKNOWN):
                return
            await gen.sleep(1.0)
        if self.job_id:
            self.log.warning(
                "Notebook server job {0} at {1}:{2} possibly failed to terminate".format(
                    self.job_id, self.ip, self.port
                )
            )

    @async_generator
    async def progress(self):
        while True:
            if self.state_ispending():
                await yield_(
                    {
                        "message": "Pending in queue...",
                    }
                )
            elif self.state_isrunning():
                await yield_(
                    {
                        "message": "Cluster job running... waiting to connect",
                    }
                )
                return
            else:
                await yield_(
                    {
                        "message": "Unknown status...",
                    }
                )
            await gen.sleep(1)


class BatchSpawnerRegexStates(BatchSpawnerBase):
    """Subclass of BatchSpawnerBase that uses config-supplied regular expressions
    to interact with batch submission system state. Provides implementations of
        state_ispending
        state_isrunning
        state_gethost

    In their place, the user should supply the following configuration:
        state_pending_re - regex that matches job_status if job is waiting to run
        state_running_re - regex that matches job_status if job is running
        state_exechost_re - regex with at least one capture group that extracts
                            execution host from job_status
        state_exechost_exp - if empty, notebook IP will be set to the contents of the
            first capture group. If this variable is set, the match object
            will be expanded using this string to obtain the notebook IP.
            See Python docs: re.match.expand
    """

    state_pending_re = Unicode(
        "",
        help="Regex that matches job_status if job is waiting to run",
    ).tag(config=True)
    state_running_re = Unicode(
        "",
        help="Regex that matches job_status if job is running",
    ).tag(config=True)
    state_exechost_re = Unicode(
        "",
        help="Regex with at least one capture group that extracts "
        "the execution host from job_status output",
    ).tag(config=True)
    state_exechost_exp = Unicode(
        "",
        help="""If empty, notebook IP will be set to the contents of the first capture group.

        If this variable is set, the match object will be expanded using this string
        to obtain the notebook IP.
        See Python docs: re.match.expand""",
    ).tag(config=True)
    state_unknown_re = Unicode(
        "",
        help="Regex that matches job_status if the resource manager is not answering."
        "Blank indicates not used.",
    ).tag(config=True)

    def state_ispending(self):
        assert self.state_pending_re, "Misconfigured: define state_running_re"
        return self.job_status and re.search(self.state_pending_re, self.job_status)

    def state_isrunning(self):
        assert self.state_running_re, "Misconfigured: define state_running_re"
        return self.job_status and re.search(self.state_running_re, self.job_status)

    def state_isunknown(self):
        # Blank means "not set" and this function always returns None.
        if self.state_unknown_re:
            return self.job_status and re.search(self.state_unknown_re, self.job_status)

    def state_gethost(self):
        assert self.state_exechost_re, "Misconfigured: define state_exechost_re"
        match = re.search(self.state_exechost_re, self.job_status)
        if not match:
            self.log.error(
                "Spawner unable to match host addr in job status: " + self.job_status
            )
            return
        if not self.state_exechost_exp:
            return match.groups()[0]
        else:
            return match.expand(self.state_exechost_exp)


class UserEnvMixin:
    """Mixin class that computes values for USER, SHELL and HOME in the environment passed to
    the job submission subprocess in case the batch system needs these for the batch script."""

    def user_env(self, env):
        """get user environment"""
        env["USER"] = self.user.name
        home = pwd.getpwnam(self.user.name).pw_dir
        shell = pwd.getpwnam(self.user.name).pw_shell
        if home:
            env["HOME"] = home
        if shell:
            env["SHELL"] = shell
        return env

    def get_env(self):
        """Get user environment variables to be passed to the user's job

        Everything here should be passed to the user's job as
        environment.  Caution: If these variables are used for
        authentication to the batch system commands as an admin, be
        aware that the user will receive access to these as well.
        """
        env = super().get_env()
        env = self.user_env(env)
        return env


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

    req_sshPwdFile = Unicode(
        "~/.passwd",
        help="Path to file containing the ssh password",
    ).tag(config=True)

    req_sshUser = Unicode(
        "",
        help="ssh username",
    ).tag(config=True)

    req_sshHost = Unicode(
        "",
        help="ssh login node host",
    ).tag(config=True)

    req_sshTunnelsFolder = Unicode(
        "~/.tunnels",
        help="Directory where to store tunnel socks",
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
            output = (
                output.splitlines()[-1]
                .strip("\n")
                .strip("\\n")
                .strip("\\\n")
                .replace("\\n", "")
                .replace("'", "")
                .replace("b", "")
            )
            id = output.split(";")[0]
            self.log.debug("SlurmSpawner job ID from text: " + id)
            int(str(id))
        except Exception as e:
            self.log.error("SlurmSpawner unable to parse job ID from text: " + e)
            raise e
        return id

    def state_gethost(self):
        host = BatchSpawnerRegexStates.state_gethost(self)
        import socket

        return socket.gethostbyname(host)

    async def start(self):
        """Start the process"""
        self.ip = self.traits()["ip"].default_value
        self.port = self.traits()["port"].default_value

        if jupyterhub.version_info >= (0, 8) and self.server:
            self.server.port = self.port

        await self.submit_batch_script()

        subvars = self.get_req_subvars()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure (no jobid in output)"
            )
        while True:
            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await gen.sleep(self.startup_poll_interval)

        self.ip = self.state_gethost()

        while self.port == 0:
            await gen.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, "mock_port"):
                self.port = self.mock_port

        try:
            cmd = "nohup sshpass -f %s ssh -n -f %s@%s -L %s:%s:%s -M -S %s/tunnel-%s -fN machine-%s &" % (
                subvars["sshPwdFile"],
                subvars["sshUser"],
                subvars["sshHost"],
                self.port,
                self.ip,
                self.port,
                subvars["sshTunnelsFolder"],
                self.port,
                self.port,
            )
            self.log.info(
                "Tunneling "
                + self.ip
                + ":"
                + str(self.port)
                + " via  localhost:"
                + str(self.port)
            )
            self.log.debug("Executing command: %s", cmd)
            p = subprocess.Popen(cmd, shell=True)
            outs, errs = p.communicate(timeout=15)
            self.log.debug("Output: %s \n Errors: %s", outs, errs)
        except Exception as ex:
            self.log.error("Failed to setup tunnel to remote server", ex)
            raise ex

        self.ip = "127.0.0.1"
        self.log.info("Registering server at: " + self.ip + ":" + str(self.port))
        if jupyterhub.version_info < (0, 7):
            # store on user for pre-jupyterhub-0.7:
            self.user.server.port = self.port
            self.user.server.ip = self.ip
        self.db.commit()
        self.log.info(
            "Notebook server job {0} started at {1}:{2}".format(
                self.job_id, self.ip, self.port
            )
        )

        return self.ip, self.port

    async def stop(self, now=False):
        """Stop the singleuser server job.

        Returns immediately after sending job cancellation command if now=True, otherwise
        tries to confirm that job is no longer running."""

        subvars = self.get_req_subvars()
        self.log.info("Stopping ssh tunnel on port: " + self.port)
        try:
            cmd = "ssh -S %s/tunnel-%s -O exit machine-%s" % (
                subvars["sshTunnelsFolder"],
                self.port,
                self.port,
            )
            self.log.debug("Executing command: %s", cmd)
            p = subprocess.Popen(cmd, shell=True)
            outs, errs = p.communicate(timeout=15)
            self.log.debug("Output: %s \n Errors: %s", outs, errs)
        except Exception as ex:
            self.log.error("Failed to kill tunnel to remote server", ex)
            raise ex

        self.log.info("Stopping server job " + self.job_id)
        await self.cancel_batch_job()
        if now:
            return
        for _ in range(10):
            status = await self.query_job_status()
            if status not in (JobStatus.RUNNING, JobStatus.UNKNOWN):
                return
            await gen.sleep(1.0)
        if self.job_id:
            self.log.warning(
                "Notebook server job {0} at {1}:{2} possibly failed to terminate".format(
                    self.job_id, self.ip, self.port
                )
            )

    async def submit_batch_script(self):
        subvars = self.get_req_subvars()
        # `cmd` is submitted to the batch system
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_submit_cmd, **subvars),
            )
        )
        # `subvars['cmd']` is what is run _inside_ the batch script,
        # put into the template.
        subvars["cmd"] = self.cmd_formatted_for_batch()
        if hasattr(self, "user_options"):
            subvars.update(self.user_options)
        script = await self._get_batch_script(**subvars)
        self.log.info("Spawner submitting job using " + cmd)
        self.log.info("Spawner submitted script:\n" + script)
        # cmd_good = "echo '%s' | " % script + cmd
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        with open(subvars["sshPwdFile"]) as f:
            ssh.connect(
                subvars["sshHost"], username=subvars["sshUser"], password=f.read()
            )
        env_string = ""
        for k, v in self.get_env().items():
            if "JUPYTERHUB" in k:
                env_string += "export %s=%s \n" % (k, v)
        self.log.info(env_string)
        _, ssh_stdout, ssh_stderr = ssh.exec_command(
            'echo "%s" | ' % script.replace("__export__", env_string) + cmd
        )
        out = str(ssh_stdout.read())
        self.log.info(str(ssh_stderr.read()))
        self.log.info(self.get_env())
        self.log.info(str(ssh_stdout.read()))
        self.log.info(str(ssh_stderr.read()))
        ssh.close()
        # out = await self.run_command(cmd_good, env=self.get_env())
        # out = await self.run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info("Job submitted. cmd: " + cmd + " output: " + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error("Job submission failed with exit code " + out)
            self.job_id = ""
        return self.job_id

    # Override if your batch system needs something more elaborate to query the job status
    batch_query_cmd = Unicode(
        "",
        help="Command to run to query job status. Formatted using req_xyz traits as {xyz} "
        "and self.job_id as {job_id}.",
    ).tag(config=True)

    async def query_job_status(self):
        """Check job status, return JobStatus object."""
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ""
            return JobStatus.NOTFOUND
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_query_cmd, **subvars),
            )
        )
        self.log.debug("Spawner querying job: " + cmd)
        import paramiko

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            with open(subvars["sshPwdFile"]) as f:
                ssh.connect(
                    subvars["sshHost"], username=subvars["sshUser"], password=f.read()
                )
            _, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
            out = str(ssh_stdout.readlines())
            self.log.info(str(ssh_stderr.readlines()))
            self.log.info(out)
            out = out.replace("[", "").replace("'", "").split(" n")[0].split("\\n")[0]
            ssh.close()
            self.job_status = out
        except:
            self.log.error("Error querying job " + self.job_id)
            self.job_status = ""

        if self.state_isrunning():
            return JobStatus.RUNNING
        elif self.state_ispending():
            return JobStatus.PENDING
        elif self.state_isunknown():
            return JobStatus.UNKNOWN
        else:
            return JobStatus.NOTFOUND


# vim: set ai expandtab softtabstop=4:

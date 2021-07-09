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
    state_exechost_re = Unicode(r'\s+((?:[\w_-]+\.?)+)$').tag(config=True)

    def parse_job_id(self, output):
        # make sure jobid is really a number
        try:
            # use only last line to circumvent slurm bug
            output = output.splitlines()[-1].strip('\n').strip('\\n').strip('\\\n').replace("\\n", "").replace("'", "").replace("b", "")
            id = output.split(';')[0]
            self.log.debug("SlurmSpawner job ID from text: " + id )
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
        self.ip = self.traits()['ip'].default_value
        self.port = self.traits()['port'].default_value

        if jupyterhub.version_info >= (0,8) and self.server:
            self.server.port = self.port

        job = await self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError("Jupyter batch job submission failure (no jobid in output)")
        while True:
            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                break
            elif status == JobStatus.PENDING:
                self.log.debug('Job ' + self.job_id + ' still pending')
            elif status == JobStatus.UNKNOWN:
                self.log.debug('Job ' + self.job_id + ' still unknown')
            else:
                self.log.warning('Job ' + self.job_id + ' neither pending nor running.\n' +
                    self.job_status)
                self.clear_state()
                raise RuntimeError('The Jupyter batch job has disappeared'
                        ' while pending in the queue or died immediately'
                        ' after starting.')
            await gen.sleep(self.startup_poll_interval)

        self.ip = self.state_gethost()

        while self.port == 0:
            await gen.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, 'mock_port'):
                self.port = self.mock_port

        try:
            cmd = 'nohup sshpass -f ~/.passwd ssh -n -f tboccali@login01-ext.m100.cineca.it -L %s:%s:%s -M -S ~/.tunnels/tunnel-%s -fN machine-%s &' % (self.port, self.ip, self.port, self.port, self.port)
            self.log.info('Tunneling ' + self.ip + ":" + str(self.port) + ' via  localhost:' + str(self.port))
            self.log.debug('Executing command: %s', cmd)
            p = subprocess.Popen(cmd, shell=True)
            outs, errs = p.communicate(timeout=15)
        except Exception as ex:
            self.log.error("Failed to setup tunnel to remote server", ex)
            raise ex

        self.ip = "127.0.0.1"
        self.log.info('Registering server at: ' + self.ip + ":" + str(self.port) )
        if jupyterhub.version_info < (0,7):
            # store on user for pre-jupyterhub-0.7:
            self.user.server.port = self.port
            self.user.server.ip = self.ip
        self.db.commit()
        self.log.info("Notebook server job {0} started at {1}:{2}".format(
                        self.job_id, self.ip, self.port)
            )

        return self.ip, self.port

    async def stop(self, now=False):
        """Stop the singleuser server job.

        Returns immediately after sending job cancellation command if now=True, otherwise
        tries to confirm that job is no longer running."""

        # TODO: kill ssh tunnel

        self.log.info("Stopping server job " + self.job_id)
        await self.cancel_batch_job()
        if now:
            return
        for i in range(10):
            status = await self.query_job_status()
            if status not in (JobStatus.RUNNING, JobStatus.UNKNOWN):
                return
            await gen.sleep(1.0)
        if self.job_id:
            self.log.warning("Notebook server job {0} at {1}:{2} possibly failed to terminate".format(
                             self.job_id, self.ip, self.port)
                )

    async def submit_batch_script(self):
        subvars = self.get_req_subvars()
        # `cmd` is submitted to the batch system
        cmd = ' '.join((format_template(self.exec_prefix, **subvars),
                        format_template(self.batch_submit_cmd, **subvars)))
        # `subvars['cmd']` is what is run _inside_ the batch script,
        # put into the template.
        subvars['cmd'] = self.cmd_formatted_for_batch()
        if hasattr(self, 'user_options'):
            subvars.update(self.user_options)
        script = await self._get_batch_script(**subvars)
        self.log.info('Spawner submitting job using ' + cmd)
        self.log.info('Spawner submitted script:\n' + script)
        #cmd_good = "echo '%s' | " % script + cmd
        import paramiko
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("login01-ext.m100.cineca.it", username="tboccali", password="___345@@")
        env_string = ""
        for k,v in self.get_env().items():
            if "JUPYTERHUB" in k:
                env_string += "export %s=%s \n" % (k,v)
        self.log.info(env_string)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('echo "%s" | ' % script.replace("__export__", env_string) + cmd )
        out = str(ssh_stdout.read())
        self.log.info(str(ssh_stderr.read()))
        self.log.info(self.get_env())
        self.log.info(str(ssh_stdout.read()))
        self.log.info(str(ssh_stderr.read()))
        ssh.close()
        #out = await self.run_command(cmd_good, env=self.get_env())
        #out = await self.run_command(cmd, input=script, env=self.get_env())
        try:
            self.log.info('Job submitted. cmd: ' + cmd + ' output: ' + out)
            self.job_id = self.parse_job_id(out)
        except:
            self.log.error('Job submission failed with exit code ' + out)
            self.job_id = ''
        return self.job_id

    # Override if your batch system needs something more elaborate to query the job status
    batch_query_cmd = Unicode('',
        help="Command to run to query job status. Formatted using req_xyz traits as {xyz} "
             "and self.job_id as {job_id}."
        ).tag(config=True)

    async def query_job_status(self):
        """Check job status, return JobStatus object."""
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ''
            return JobStatus.NOTFOUND
        subvars = self.get_req_subvars()
        subvars['job_id'] = self.job_id
        cmd = ' '.join((format_template(self.exec_prefix, **subvars),
                        format_template(self.batch_query_cmd, **subvars)))
        self.log.debug('Spawner querying job: ' + cmd)
        import paramiko
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect("login01-ext.m100.cineca.it", username="tboccali", password="___345@@")
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command( cmd )
            out = str(ssh_stdout.readlines())
            self.log.info(str(ssh_stderr.readlines()))
            self.log.info(out)
            out = out.replace("[","").replace("'","").split(" n")[0].split("\\n")[0]
            ssh.close()
            self.job_status = out
        except Exception as e:
            self.log.error('Error querying job ' + self.job_id)
            self.job_status = ''

        if self.state_isrunning():
            return JobStatus.RUNNING
        elif self.state_ispending():
            return JobStatus.PENDING
        elif self.state_isunknown():
            return JobStatus.UNKNOWN
        else:
            return JobStatus.NOTFOUND

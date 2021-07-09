#!/bin/bash -i

# i cannot set the tsocks from here, I skip

echo -e `env`
echo == SLURM SCRIPT starting at `date`

module load anaconda

#CONDA_BASE=$(conda info --base)
eval $(conda shell.bash hook)
#conda init bash

conda activate testJH

export tsocks_ssh_cmd="ssh -N -D 1087 -o TCPKeepAlive=yes -o ServerAliveInterval=60 boccali@lxplus.cern.ch" 

export use_tsocks=1

echo ===== STARTING JOB

if [ "$use_tsocks" == 1 ]
then
        echo ===== SETTING FOR SINGULARITY WITH  $tsocks_ssh_cmd
        $tsocks_ssh_cmd &
        echo ===== SLEEPING FOR 20 SEC
        sleep 20
        cd $WORK/tsocks-1.8beta5+ds1/
        export LD_PRELOAD=`pwd`/libtsocks.so
        cd -

fi

echo ===== Starting jupyterhub-singleuser with
#echo `env`
which jupyterhub-singleuser

batchspawner-singleuser jupyterhub-singleuser --log-level=DEBUG --ip=0.0.0.0 


echo == SLURM SCRIPT finishing at `date` with code $?

exit 0

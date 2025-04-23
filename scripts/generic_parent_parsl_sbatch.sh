#!/bin/bash
#SBATCH --job-name=parslParent
#SBATCH --time=48:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --gpus=0
#SBATCH --account=rubin:commissioning
#SBATCH --partition=roma
#SBATCH --output=parsl_parent_%j.out
#SBATCH --comment="____comment____"

# defaults should look like this, and we should plan on installing kbmod and kbmod-wf in the stack directory for sanity
rubindir="/sdf/home/c/colinc/rubin-user"
stackdir="$rubindir"/"lsst_stack_v28_0_1"
# kbmodwfdir="$stackdir"/"kbmod-wf"
kbmodwfdir="$rubindir"/"parsl/kbmod-wf"

export GPUNODE="ampere"
if [ $# -gt 0 ];then
    if [ $(echo $1 | grep -c "ada") -gt 0 ];then
        export GPUNODE="ada"
#       stackdir="$rubindir"/"lsst_stack_v29_0_0"
        stackdir="$rubindir"/"lsst_stack_v28_0_1_ada"
        kbmodwfdir="$stackdir"/"kbmod-wf"
    else
        export GPUNODE="ampere"
        stackdir="$rubindir"/"lsst_stack_v28_0_1"
        kbmodwfdir="$rubindir"/"parsl/kbmod-wf"
    fi
fi

echo ""
echo "GPUNODE is $GPUNODE"
echo "stackdir is $stackdir"
echo "kbmodwfdir is $kbmodwfdir"
echo ""

sd="$(pwd)"

date

echo "$(date) hostname: $(hostname)"

hostnamectl
nvidia-smi

echo ""
echo "$(date) -- Loading LSST stack environment..."
time source "$stackdir"/"loadLSST.bash"

echo "$(date) -- Running setup lsst_distrib next..."
time setup "lsst_distrib"

nvcc --version
gcc --version

# python "$kbmodwfdir"/"src/kbmod_wf/multi_night_workflow.py" --runtime-config="$rubindir"/"parsl/staging/39.0_20X20_shards/runtime_config_39.0.toml" --env="usdf"

echo "Command:"
echo "python $kbmodwfdir/src/kbmod_wf/multi_night_workflow.py --runtime-config=____tomlfile____ --env=usdf"

python "$kbmodwfdir/src/kbmod_wf/multi_night_workflow.py" --runtime-config="____tomlfile____" --env="usdf"

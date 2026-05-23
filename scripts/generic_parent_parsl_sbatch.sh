#!/bin/bash
#SBATCH --job-name=parslParent
#SBATCH --time=72:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --gpus=0
#SBATCH --account=rubin:commissioning
#SBATCH --partition=roma
#SBATCH --output=parsl_parent_%j.out
#SBATCH --comment="____comment____"

# defaults should look like this, and we should plan on installing kbmod and kbmod-wf in the stack directory for sanity
python_bin="$(command -v python)"
if [ -z "$python_bin" ];then
    echo "ERROR: Could not find python."
    exit 2
fi

stackdir="$("$python_bin" - <<'PY'
from pathlib import Path
import sys

p = Path(sys.executable).resolve()
for parent in p.parents:
    if parent.name == "lsst_stack":
        print(parent)
        raise SystemExit(0)

raise SystemExit(f"ERROR: Could not find lsst_stack in resolved python path: {p}")
PY
)" || exit 2

kbmodwfdir="$("$python_bin" - <<'PY'
from importlib.util import find_spec
from pathlib import Path

spec = find_spec("kbmod_wf")
if spec is None:
    raise SystemExit("ERROR: Could not find import spec for kbmod_wf.")

if spec.submodule_search_locations:
    package_dir = Path(next(iter(spec.submodule_search_locations))).resolve()
elif spec.origin:
    package_dir = Path(spec.origin).resolve().parent
else:
    raise SystemExit("ERROR: kbmod_wf import spec has no usable location.")

for parent in (package_dir, *package_dir.parents):
    workflow_script = parent / "src" / "kbmod_wf" / "multi_night_workflow.py"
    if workflow_script.exists():
        print(parent)
        raise SystemExit(0)

raise SystemExit(f"ERROR: Could not determine kbmod-wf root from {package_dir}")
PY
)" || exit 2

export GPUNODE="ampere"
if [ $# -gt 0 ];then
    if [ $(echo $1 | grep -c "ada") -gt 0 ];then
        export GPUNODE="ada"
    else
        export GPUNODE="ampere"
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

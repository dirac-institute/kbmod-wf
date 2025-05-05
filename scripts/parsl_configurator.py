# 4/21/2025 COC

import os
import inspect
import os
import shutil
import glob

script_directory = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def get_reflex_distance(basedir, mode="collection"):
    """Determine reflex distance, either by filename or by .collection content. 5/5/2025 COC"""
    reflex_distance = None
    first_ic_path = glob.glob(f"{basedir}/*.collection")[0]
    if mode == "collection":
        print(f"Determining reflex distances via first row of first collection in {basedir}...")
        import kbmod
        ic = kbmod.ImageCollection.read(first_ic_path)
        reflex_distance = ic["helio_guess_dist"][0]
    else:
        print("Using default mode for get_reflex_distance: collection name")
        reflex_distance = os.path.basename(first_ic_path).split("_")[1]
    reflex_distance = float(reflex_distance)
    print(f"Using reflex_distance {reflex_distance}")
    return reflex_distance


def make_toml_file(basedir, generic_toml_path, site_name="Rubin", disable_cleanup=False, nworkers=32, repo_path="/repo/main", reflex_distances=None):
    #
    if reflex_distances == None:
        print(f'WARNING: default reflex distances hardcoded to [39.0]')
        reflex_distances = [39.0]
    lookup_dict = {}
    lookup_dict["____basedir____"] = basedir
    lookup_dict["____butlerpath____"] = repo_path
    lookup_dict["____reflexdist____"] = ",".join([str(float(i)) for i in reflex_distances])
    lookup_dict["____nworkers____"] = str(nworkers)
    lookup_dict["____sitename____"] = site_name
    if disable_cleanup == True:
        lookup_dict["____cleanupwu____"] = "false"
    else:
        lookup_dict["____cleanupwu____"] = "true"
    #
    outfile = f"{basedir}/runtime_config.toml"
    with open(outfile, "w") as f:
        with open(generic_toml_path, "r") as g:
            for line in g:
                line = line.strip()
                if line.startswith("#") or len(line.strip()) == 0: # comment or blank line
                    print(line, file=f)
                    continue
                found_key = False
                for key in lookup_dict:
                    if key in line:
                        print(line.replace(key, lookup_dict[key]), file=f)
                        found_key = True
                        break
                if found_key == True:
                    continue
                print(line, file=f) # catch-all
    print(f"Finished writing {outfile} to disk.")


def make_sbatch_file(generic_sbatch_file, basedir, comment=None):
    """
    Make accompanying Parsl parent sbatch file.
    """
    outfile = f"{basedir}/parent_parsl_sbatch.sh"
    tomlfile = f"{basedir}/runtime_config.toml"
    #
    lookup_dict = {}
    lookup_dict["____basedir____"] = basedir
    lookup_dict["____tomlfile____"] = tomlfile
    #
    if comment == None:
        lookup_dict["____comment____"] = os.path.basename(basedir)
    else:
        lookup_dict["____comment____"] = comment
    #
    with open(outfile, "w") as f:
        with open(generic_sbatch_file, "r") as g:
            for line in g:
                line = line.rstrip("\n")
#               if line.startswith("#") or len(line) == 0:
#                   print(line, file=f)
#                   continue
                found_key = False
                for key in lookup_dict:
                    if key in line:
                        print(line.replace(key, lookup_dict[key]), file=f)
                        found_key = True
                        break
                if found_key == True:
                    continue
                print(line, file=f)
    print(f'Wrote {outfile} to disk.')
    return outfile

def make_yaml_file(generic_yaml_file, basedir):
    yaml_outfile = f"{basedir}/search_config.yaml"
    with open(generic_yaml_file, 'r') as g:
        with open(yaml_outfile, 'w') as f:
            for line in g:
                line = line.rstrip("\n")
                print(line, file=f)
    print(f'Wrote {yaml_outfile} to disk.')
    return yaml_outfile


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Tool to generate .toml files for Parsl Workflow.') # 4/21/2025 COC
    parser.add_argument('--basedir', dest='basedir', help='path to staging directory containing ImageCollections. Default: current working directory.', type=str, default=os.getcwd())
    parser.add_argument('--reflex-distances', dest='reflex_distances', help='reflex-correction distances; default is None, which results in the directory being crawled and the first reflex-distance found in an ImageCollection is used. There should only be one distance, but multiple are supported.', type=float, default=None, nargs='+')
    parser.add_argument('--site-name', dest='site_name', help='Observatory site name. Default: "Rubin"', type=str, default="Rubin")
    parser.add_argument('--disable-cleanup', dest='disable_cleanup', help='disable deletion of reprojected WorkUnits after successful run. Default: False', type=bool, default=False)
    parser.add_argument('--nworkers', dest='nworkers', help='number of workers (cores) to assume. Default: 32', type=int, default=32)
    parser.add_argument('--repo-path', dest='repo_path', help='Butler repository path. Default: /repo/main', type=str, default="/repo/main")
    parser.add_argument('--generic-toml-path', dest='generic_toml_path', help='Path to a special generic TOML file. Default: ~/generic_runtime_config.toml', type=str, default=f"{script_directory}/generic_runtime_config.toml")
    parser.add_argument('--generic-sbatch-path', dest='generic_sbatch_path', help='Path to a special generic sbatch file. Default: ~/generic_parent_parsl_sbatch.sh', type=str, default=f"{script_directory}/generic_parent_parsl_sbatch.sh")
    parser.add_argument('--generic-yaml-path', dest='generic_yaml_path', help='Path to a special generic kbmod search config yaml file. Default: ~/generic_search_config.yaml', type=str, default=f"{script_directory}/generic_search_config.yaml")
    #
    args = parser.parse_args()
    #
    # TODO add checks for input files here
    #
    reflex_distances = args.reflex_distances
    if reflex_distances == None or reflex_distances == []:
        reflex_distances = [get_reflex_distance(basedir=args.basedir)]
    make_toml_file(basedir=args.basedir, 
                generic_toml_path=args.generic_toml_path, 
                site_name=args.site_name, 
                disable_cleanup=args.disable_cleanup, 
                nworkers=args.nworkers, 
                repo_path=args.repo_path, 
                reflex_distances=reflex_distances
            )
    make_sbatch_file(generic_sbatch_file=args.generic_sbatch_path, basedir=args.basedir)
    make_yaml_file(generic_yaml_file=args.generic_yaml_path, basedir=args.basedir)
    #
    # Copy the GPU Worker script
    shutil.copy(os.path.join(script_directory, "gpu_worker_launcher.sh"), args.basedir)
    #
    print(f"Finished!")

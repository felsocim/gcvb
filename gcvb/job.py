import os
import subprocess
import re
from copy import copy
from . import util
from . import yaml_input
from . import template


def templates_to_files(test,template_path,target_dir):
    for file in os.listdir(template_path):
        src=os.path.join(template_path,file)
        dst=os.path.join(target_dir,test["id"],file)
        format_dic=test["template_instantiation"].copy()
        format_dic["@job"]={}
        if "batch" in test:
            format_dic["@job"]["batch"]=test["batch"]
        if "singleton" in test:
            format_dic["@job"]["batch"]=test["singleton"]
        format_dic["@job"]["id"]=test["id"]
        format_dic["@job_creation"]=template.job_creation_dict()
        template.apply_format_to_file(src,dst,format_dic)

def generate(target_dir,gcvb):
    """Generate computation directories

    Keyword arguments:
    target_dir -- targeted directory
    gcvb       -- gcvb struct
    """
    data_root=gcvb["data_root"]
    os.makedirs(target_dir)
    test_file=os.path.join(target_dir,"tests.yaml")
    util.write_yaml(gcvb,test_file)
    for p in gcvb["Packs"]:
        for t in p["Tests"]:
            os.makedirs(os.path.join(target_dir,t["id"]))
            if "data" in t:
                data_path=os.path.join(data_root,t["data"],"input")
                for file in os.listdir(data_path):
                    src=os.path.join(data_path,file)
                    dst=os.path.join(target_dir,t["id"],file)
                    os.symlink(src,dst)
            if ("template_files" in t):
                if isinstance(t["template_files"], list):
                    for template_dir in t["template_files"]:
                        template_path=os.path.join(data_root,t["data"],"templates",template_dir)
                        templates_to_files(t,template_path,target_dir)
                else:
                    template_path=os.path.join(data_root,t["data"],"templates",t["template_files"])
                    templates_to_files(t,template_path,target_dir)

def format_launch_command(format_string, config, at_job_creation):
    regexp="{@executable\[(?P<identifier>[^\]]*)\]}"
    l=re.findall(regexp,format_string)
    executable=copy(config["executables"])
    for e in l:
        if e not in executable:
            executable[e]=e
    d={"@job_creation" : at_job_creation, "@executable" : executable}
    return format_string.format(**d)

def fill_at_job_creation_task(at_job_creation, task, full_id, config, singularity):
    at_job_creation["nthreads"]=task["nthreads"]
    at_job_creation["nprocs"]=task["nprocs"]
    at_job_creation["full_id"]=full_id #test["id"]+"_"+str(c)
    at_job_creation["executable"]=task["executable"]
    if task["executable"] in config["executables"]:
        at_job_creation["executable"]=config["executables"][task["executable"]]
    at_job_creation["options"]=task.get("options","")
    if singularity:
        at_job_creation["singularity"]=" ".join(config["singularity"])
    else:
        at_job_creation["singularity"]=""
    return None

def fill_at_job_creation_validation(at_job_creation, validation, data_root, ref_data, config, valid, singularity):
    at_job_creation["va_id"]=validation["id"]
    at_job_creation["va_executable"]=validation["executable"]
    if validation["type"]=="file_comparison":
        #specific values for file comparison
        if "base" not in validation:
            tmp=validation["id"].split("-")
            if len(tmp)!=2:
                raise ValueError(f"No base specified, and there is no or more than one '-' in id. Validation id : '{validation['id']}'' for test '{test['id']}'.")
            v_dir,v_id=tmp[0],tmp[1]
        else:
            v_dir,v_id=validation["base"],validation["ref"]
        if v_dir in valid[ref_data]:
           at_job_creation["va_filename"]=valid[ref_data][v_dir][v_id]["file"]
        else:
           at_job_creation["va_filename"]="notavailable"
        at_job_creation["va_refdir"]=os.path.join(data_root,ref_data,"references",v_dir)
    if validation["executable"] in config["executables"]:
        at_job_creation["va_executable"]=config["executables"][validation["executable"]]
    at_job_creation["nprocs"]=validation.get("nprocs","1") # should we default to one or impose definition ?
    at_job_creation["nthreads"]=validation.get("nthreads","1")
    if singularity:
        at_job_creation["singularity"]=" ".join(config["singularity"])
    else:
        at_job_creation["singularity"]=""

def write_script(tests, config, data_root, base_id, run_id, *, job_file="job.sh", header=None, local_header=None, validate_only=False, singularity=False):
    valid=yaml_input.get_references(tests,data_root)
    singularity_prefix = ""
    if singularity:
        singularity_prefix = " ".join(config["singularity"]) + " "
    with open(job_file,'w') as f:
        if (header):
            with open(header, 'r') as h:
                for line in h:
                    f.write(line)
            f.write("\n")
        if (local_header):
            with open("results/{0}/{1}/{2}".format(str(base_id), tests[0]["id"], local_header), 'r') as h:
                for line in h:
                    f.write(line)
            f.write("\n")
        f.write(singularity_prefix + "python3 -m gcvb db start_run {0} -1 -1 \n".format(run_id))
        f.write("cd results/{0}\n".format(str(base_id)))
        for test in tests:
            f.write("\n#TEST {}\n".format(test["id"]))
            if not singularity:
                f.write("export GCVB_RUN_ID={!s}\n".format(run_id))
                f.write("export GCVB_TEST_ID={!s}\n".format(test["id_db"]))
            f.write("cd {0}\n".format(test["id"]))
            f.write(singularity_prefix + "python3 -m gcvb db start_test {0} {1} {2}\n".format(run_id,test["id_db"],test["id"]))
            step = 0
            for c,t in enumerate(test["Tasks"]):
                step += 1
                f.write("export GCVB_STEP_ID={!s}\n".format(step))
                f.write(singularity_prefix + "python3 -m gcvb db start_task {0} {1} 0\n".format(test["id_db"],step))
                at_job_creation={}
                fill_at_job_creation_task(at_job_creation, t, test["id"]+"_"+str(c), config, singularity)
                if not(validate_only):
                    f.write(format_launch_command(t["launch_command"],config,at_job_creation))
                    f.write("\n")
                f.write(singularity_prefix + "python3 -m gcvb db end_task {0} {1} $?\n".format(test["id_db"],step))
                for d,v in enumerate(t.get("Validations",[])):
                    step += 1
                    f.write("export GCVB_STEP_ID={!s}\n".format(step))
                    f.write(singularity_prefix + "python3 -m gcvb db start_task {0} {1} 0\n".format(test["id_db"],step))
                    fill_at_job_creation_validation(at_job_creation, v, data_root, test["data"] if "data" in test else "", config, valid, singularity)
                    if singularity:
                        va_command_pieces=v["launch_command"].split()
                        va_command_pieces.insert(va_command_pieces.index("{@job_creation[singularity]}") + 1, "bash -c 'export GCVB_RUN_ID={0} GCVB_TEST_ID={1} &&".format(run_id,test["id_db"]))
                        v["launch_command"]=" ".join(va_command_pieces)
                        v["launch_command"]+="'"
                    va_command = format_launch_command(v["launch_command"],config,at_job_creation)
                    f.write(va_command)
                    f.write("\n")
                    f.write(singularity_prefix + "python3 -m gcvb db end_task {0} {1} $?\n".format(test["id_db"],step))
            f.write(singularity_prefix + "python3 -m gcvb db end_test {0} {1} {2}\n".format(run_id,test["id_db"],test["id"]))
            f.write("cd ..\n")
        f.write("cd ../..\n")
        f.write(singularity_prefix + "python3 -m gcvb db end_run {0} -1 -1 \n".format(run_id))

def launch(job_file, config, validate_only=False, wait_after_submitting=False):
    submit_command = config["submit_command"]
    if validate_only:
        submit_command = config["va_submit_command"]

    process = subprocess.Popen([submit_command, job_file])
    if wait_after_submitting:
        process.wait()

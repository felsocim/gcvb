import argparse
import yaml
import re
import os
import sys
import pprint
from . import yaml_input
from . import template
from . import job
from . import util
from . import db
from . import validation
from . import snippet

def parse():
    parser = argparse.ArgumentParser(description="(G)enerate (C)ompute (V)alidate (B)enchmark",prog="gcvb")

    #filters options
    parser.add_argument('--yaml-file',metavar="filename",default="test.yaml")
    parser.add_argument('--modifier',metavar="python_module", default=None)
    parser.add_argument('--filter-by-pack',metavar="regexp",help="Regexp to select packs")
    parser.add_argument('--filter-by-test-id',metavar="regexp",help="Regexp to select jobs by test-id")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--filter-by-tag',metavar="tag",help="a tag name to filter tests")
    group.add_argument('--filter-by-tag-and',metavar="tag_list",help="comma-separated list of tags to filter tests (AND operator)")
    group.add_argument('--filter-by-tag-or',metavar="tag_list",help="comma-separated list of tags to filter tests (OR operator)")
    
    subparsers = parser.add_subparsers(dest="command")
    parser_generate = subparsers.add_parser('generate', help="generate a new gcvb instance")
    parser_list = subparsers.add_parser('list', help="list tests (YAML)")
    parser_compute = subparsers.add_parser('compute', help="run tests")
    parser_db = subparsers.add_parser('db', add_help=False)
    parser_report = subparsers.add_parser('report', help="get a report regarding a gcvb run")
    parser_dashboard = subparsers.add_parser('dashboard', help="launch a Dash instance to browse results" )
    parser_snippet = snippet.generate_parser(subparsers)

    parser_generate.add_argument('--data-root',metavar="dir",default=None)

    parser_compute.add_argument("--gcvb-base",metavar="base_id",help="choose a specific base (default: last one created)", default=None)
    parser_compute.add_argument("--header", metavar="file", help="use file as header when generating job script", default=None)

    parser_db.add_argument("db_command", choices=["start_test","end_test","start_run","end_run"])
    parser_db.add_argument("run_id", type=str)
    parser_db.add_argument("test_db_id", type=str)
    parser_db.add_argument("test_id", type=str)


    args=parser.parse_args()
    return args

def filter_tests(args,data):
    if (args.filter_by_pack):
        data["Packs"]=[p for p in data["Packs"] if re.match(args.filter_by_pack,p["pack_id"])]
    if (args.filter_by_test_id):
        for e in data["Packs"]:
            e["Tests"]=[t for t in e["Tests"] if re.match(args.filter_by_test_id,t["id"])]
    if (args.filter_by_tag):
        for e in data["Packs"]:
            e["Tests"]=[t for t in e["Tests"] if args.filter_by_tag in t.get("tags",[])]
    if (args.filter_by_tag_and):
        tags=set(args.filter_by_tag_and.split(","))
        for e in data["Packs"]:
            e["Tests"]=[t for t in e["Tests"] if (tags.intersection(set(t.get("tags",[])))==tags)]
    if (args.filter_by_tag_or):
        tags=set(args.filter_by_tag_or.split(","))
        for e in data["Packs"]:
            e["Tests"]=[t for t in e["Tests"] if (tags.intersection(set(t.get("tags",[])))!=set())]
    return data

def get_to_gcvb_root():
    while not(os.path.isfile("config.yaml")):
        current_path=os.getcwd()
        os.chdir("..")
        if (os.getcwd()==current_path):
            print("You are not inside a gcvb instance. The config.yaml was not found in a parent directory.")
            sys.exit()

def main():
    args=parse()
    if args.command not in ["db","snippet"]:
        #currently db is a special command that is supposed to be invoked only internaly by gcvb.
        get_to_gcvb_root()

    if args.command in ["list","generate"]:
        a=yaml_input.load_yaml(args.yaml_file, args.modifier)
        a=filter_tests(args,a)
    #Commands
    if args.command=="list":
        print(yaml.dump({"Packs" : a["Packs"]}))
    if args.command=="generate":
        data_root=os.path.join(os.getcwd(),"data")
        if (args.data_root):
            data_root=os.path.abspath(args.data_root)

        if not(os.path.isfile(db.database)):
            db.create_db()
        gcvb_id=db.new_gcvb_instance(args.yaml_file,' '.join(sys.argv[1:]))
        target_dir="./results/{}".format(str(gcvb_id))
        a["data_root"]=data_root
        job.generate(target_dir,a)

    if args.command=="compute":
        gcvb_id=args.gcvb_base
        config=util.open_yaml("config.yaml")
        config_id=config.get("machine_id")
        if not(gcvb_id):
            gcvb_id=db.get_last_gcvb()
        run_id=db.add_run(gcvb_id,config_id)
        computation_dir="./results/{}".format(str(gcvb_id))
        a=yaml_input.load_yaml(os.path.join(computation_dir,"tests.yaml"))
        a=filter_tests(args,a)

        all_tests=[t for p in a["Packs"] for t in p["Tests"]]
        db.add_tests(run_id,all_tests)
        job_file=os.path.join(computation_dir,"job.sh")
        data_root=a["data_root"]
        job.write_script(all_tests, config, data_root, gcvb_id, run_id, job_file=job_file, header=args.header)
        job.launch(job_file,config)


    if args.command=="db":
        if args.db_command=="start_run":
            db.start_run(args.run_id)
        if args.db_command=="end_run":
            db.end_run(args.run_id)
        if args.db_command=="start_test":
            db.set_db("../../../gcvb.db")
            db.start_test(args.run_id,args.test_db_id)
        if args.db_command=="end_test":
            db.set_db("../../../gcvb.db")
            db.end_test(args.run_id,args.test_db_id)
            a=yaml_input.load_yaml("../tests.yaml")
            t=a["Tests"][args.test_id]
            if "keep" in t:
                db.save_files(args.run_id,args.test_db_id,t["keep"])

    if args.command=="report":
        run_id,gcvb_id=db.get_last_run()
        computation_dir="./results/{}".format(str(gcvb_id))
        a=yaml_input.load_yaml_from_run(run_id)

        #Is the run finished ?
        tests=db.get_tests(run_id)
        completed_tests=list(filter(lambda x: x["end_date"], tests))
        print("Tests completed : {!s}/{!s}".format(len(completed_tests),len(tests)))
        finished=(len(completed_tests)==len(tests))

        tmp=db.load_report(run_id)
        report=validation.Report(a,tmp)
        if report.is_success():
            if finished:
                print("Success!")
            else:
                print("No failure yet, computation in progress...")
        else:
            if report.missing_validations:
                print("Some validation metrics are missing :")
                pprint.pprint(report.missing_validations)
            failed=report.get_failed_tests()
            print("{!s} failure(s) : {!s}".format(len(failed),list(failed)))
            print("Details of failures :")
            pprint.pprint(report.failure)

    if args.command == "snippet":
        snippet.display(args)

    if args.command=="dashboard":
        from . import dashboard
        dashboard.run_server()

if __name__ == '__main__':
    main()
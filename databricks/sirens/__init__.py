import os
import sys
import argparse
import configparser
from pathlib import Path
from .logging import colours
from . import _version

ENGINE_VERSION = _version.__version__
AUTHORS = _version.__author__
RELEASE_DATE = _version.__release_date__


def sirens_main():
    from . import controller

    parser = argparse.ArgumentParser(description="Generate and Deploy Data Engineering and Detection Notebooks for Cybersecurity Data")

    subparser = parser.add_subparsers(dest='action', required=True, help="Specify one of command(s) to execute sirens with")
    generate_notebooks = subparser.add_parser('generate_notebooks')
    generate_detections = subparser.add_parser('generate_detections')
    generate_threat_hunts = subparser.add_parser('generate_threat_hunts')
    generate_threat_intel = subparser.add_parser('generate_threat_intel')
    generate_all = subparser.add_parser('generate_all')
    build = subparser.add_parser('build')
    deploy = subparser.add_parser('deploy')
    destroy = subparser.add_parser('destroy')
    plan = subparser.add_parser('plan')
    validate = subparser.add_parser('validate')
    cim_tables = subparser.add_parser('generate_cim_tables')
    auto_deploy = subparser.add_parser('auto_deploy')

    cim_tables.add_argument('-p', '--path', required=False, type=str, help='Path to CIM definitions')

    # generate_notebooks
    generate_notebooks.add_argument('-s', '--stanza', required=False, type=str, nargs='+',
                                    help='Generate Notebooks for specific stanza(s)')

    # generate_detections
    generate_detections.add_argument('-s', '--stanza', required=False, type=str, nargs='+',
                                     help='Generate Detections for specific stanza(s)')

    # generate_threathunts
    generate_threat_hunts.add_argument('-n', '--name', required=False, type=str, nargs='+',
                                       help='Generate specific threat hunting notebooks by name')
    
    # generate_threat_intelligence
    generate_threat_intel.add_argument('-n', '--name', required=False, type=str, nargs='+',
                                       help='Generate jobs for a specific threat intelligence feed')

    # generate_all
    generate_all.add_argument('-n', '--no-clobber', required=False, action='store_true')
    generate_all.add_argument('-cp', '--cim_path', required=False, type=str, help='Path to CIM definitions')

    # validate yaml files.
    validate.add_argument('-t', '--type', required=True, type=str, choices=['inputs', 'detections'], help='validate config files')
    validate.add_argument('-s', '--source', required=False, type=str, nargs='+', default='all',
                          help="specific source file(s) to validate. defaults to all.")

    # auto_deploy options
    auto_deploy.add_argument('-gco', '--gen_config_only', required=False, action='store_true', default=False,
                             help="Only generate sirens.config, using interactive wizard.")
    auto_deploy.add_argument('-gdo', '--gen_and_deploy_only', required=False, action='store_true', default=False,
                             help="Don't generate sirens.config, only deploy.")

    # global options
    parser.add_argument("-cf", "--config_file", required=False, type=str, default="sirens.config",
                        help="path to the configuration file. Defaults to sirens.config in the current directory")
    parser.add_argument("-ll", "--log_level", default="INFO", required=False, action="store", dest="LOG_LEVEL",
                        help="override default log_level")
    parser.add_argument("-V", "--version", default=False, action="store_true", required=False,
                        help="shows current sirens version")

    args = parser.parse_args()
    # print(args)

    if args.version:
        print(f"Version: {ENGINE_VERSION}")
        sys.exit(0)

    if args.action != "auto_deploy":
        config_file = Path(args.config_file)
        if config_file.is_file():
            configpath = str(config_file)
        else:
            print(f"{colours.ERROR}ERROR: sirens failed to find config file at: {args.config_file}{colours.ENDC}")
            sys.exit(1)

    config = configparser.ConfigParser()
    config.read(args.config_file)
    ctl = controller.Controller(config)

    print(f"{colours.INFO}INFO: Sirens called with arg: {args.action}{colours.ENDC}")


    if args.action == "auto_deploy":
        ret = ctl.auto_deploy(args.gen_config_only, args.gen_and_deploy_only)

    if args.action == "validate":
        print(f"{colours.INFO}INFO: Validating input package(s) in: {args.source}{colours.ENDC}")
        if args.type == "inputs" and args.source == "all":
            input_dir = 'log_sources'
        elif args.type == "detections" and args.source == "all":
            input_dir = 'detections'
        if not os.path.isdir(input_dir):
            print(f"{colours.ERROR}argument: {input_dir}, must be a directory{colours.ENDC}")
            sys.exit(1)

        if (ctl.validate(args.type, input_dir)):
            sys.exit(0)

    if args.action == "generate_notebooks":
        ret = ctl.generate_notebooks(stanzas=args.stanza)

    if args.action == 'generate_detections':
        ret = ctl.generate_detections(stanzas=args.stanza)
        ret = ctl.generate_alerts(stanzas=args.stanza)

    if args.action == 'generate_threat_hunts':
        ret = ctl.generate_threat_hunts(hunts=args.name)

    if args.action == 'generate_threat_intel':
        ret = ctl.generate_threat_intel(collections=args.name)

    # build everything
    if args.action == 'generate_all':
        ctl.generate_notebooks(no_clobber=args.no_clobber)
        ctl.generate_detections(no_clobber=args.no_clobber)
        ctl.generate_threat_hunts(no_clobber=args.no_clobber)
        ctl.generate_threat_intel(no_clobber=args.no_clobber)
        ctl.generate_alerts(no_clobber=args.no_clobber)
        ctl.generate_cim_tables(path=args.cim_path)
    
    if args.action == 'generate_cim_tables':
        ret = ctl.generate_cim_tables(path=args.path)
    
    if args.action == "build":
        ret = ctl.build()

    if args.action == "deploy":
        ret = ctl.deploy()

    if args.action == "plan":
        ret = ctl.plan()

    if args.action == "destroy":
        ret = ctl.destroy()

import importlib

if __name__ == "__main__":
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    from textwrap import dedent

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        epilog=dedent("""Pyspark algorithms arguments."""),
    )

    parser.add_argument(
        "algorithm_name",
        help="The name of the spark algorithm you want to run",
    )
    parser.add_argument("-e", "--env", help="environment", default="prd")
    parser.add_argument("-c", "--client", help="client provider", default="os")
    parser.add_argument("-d", "--datetime", help="reference datetime", default="today")
    parser.add_argument(
        "--dry-run",
        help="just map input and output paths",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--algorithm-args", help="algorithm arguments", required=False, nargs="*"
    )

    args = parser.parse_args()

    tunings = dict([x.split("=") for x in (args.algorithm_args or [])])

    algorithm = args.algorithm_name.split(":")
    if len(algorithm) == 1:
        algorithm.append("setup")
    module = "algorithms.{}.{}".format(*algorithm)
    algorithm_module = importlib.import_module(module)

    algorithm_module.setup(
        args.env, args.client, args.datetime, args.dry_run, **tunings
    )

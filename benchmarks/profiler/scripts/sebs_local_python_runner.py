#!/usr/bin/env python3
"""Local SeBS Python benchmark runner.

Usage: sebs_local_python_runner.py <function.py> <input.json>

Loads the function module, calls handler(event) with the parsed input,
and prints the result as JSON to stdout.
"""

import importlib.util
import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: sebs_local_python_runner.py <function.py> <input.json>", file=sys.stderr)
        sys.exit(2)

    function_path = Path(sys.argv[1]).resolve()
    input_path = Path(sys.argv[2]).resolve()

    with open(input_path, "r", encoding="utf-8") as fh:
        event = json.load(fh)

    sys.path.insert(0, str(function_path.parent))
    spec = importlib.util.spec_from_file_location("sebs_function", function_path)
    if spec is None or spec.loader is None:
        print(f"could not load {function_path}", file=sys.stderr)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    sys.path.pop(0)

    # Set up the module's __file__ so relative-path imports work
    module.__file__ = str(function_path)

    spec.loader.exec_module(module)

    handler = getattr(module, "handler", None) or getattr(module, "main", None)
    if handler is None:
        print(f"{function_path} exports neither handler nor main", file=sys.stderr)
        sys.exit(1)

    output = handler(event)
    print(json.dumps(output))


if __name__ == "__main__":
    main()

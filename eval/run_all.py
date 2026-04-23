#!/usr/bin/env python3

import ast
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DiscoveryResult:
    runnable: bool
    reason: str


SCRIPT_DIR = Path(__file__).resolve().parent
SELF_PATH = Path(__file__).resolve()
SKIP_FILENAMES = {"aggregate.py", "run_all.py"}


def has_main_function(module: ast.Module) -> bool:
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return True
    return False


def has_main_guard(module: ast.Module) -> bool:
    for node in module.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not isinstance(test, ast.Compare):
            continue
        if not isinstance(test.left, ast.Name) or test.left.id != "__name__":
            continue
        if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
            continue
        if len(test.comparators) != 1:
            continue
        comparator = node.test.comparators[0]
        if isinstance(comparator, ast.Constant) and comparator.value == "__main__":
            return True
    return False


def add_argument_has_required_positional(call: ast.Call) -> bool:
    if not call.args:
        return False

    first_arg = call.args[0]
    if not isinstance(first_arg, ast.Constant) or not isinstance(first_arg.value, str):
        return False
    if first_arg.value.startswith("-"):
        return False

    for keyword in call.keywords:
        if keyword.arg == "nargs" and isinstance(keyword.value, ast.Constant):
            if keyword.value.value in ("?", "*"):
                return False

    return True


def has_required_positional_args(module: ast.Module) -> bool:
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "add_argument":
            continue
        if add_argument_has_required_positional(node):
            return True
    return False


def inspect_script(path: Path) -> DiscoveryResult:
    try:
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return DiscoveryResult(False, f"syntax error: {exc.msg}")

    if not has_main_function(module):
        return DiscoveryResult(False, "no main()")
    if not has_main_guard(module):
        return DiscoveryResult(False, 'no `if __name__ == "__main__"` guard')
    if has_required_positional_args(module):
        return DiscoveryResult(False, "requires positional CLI arguments")
    return DiscoveryResult(True, "ok")


def discover_scripts() -> tuple[list[Path], list[tuple[Path, str]]]:
    runnable: list[Path] = []
    skipped: list[tuple[Path, str]] = []

    for path in sorted(SCRIPT_DIR.glob("*.py")):
        if path.resolve() == SELF_PATH:
            continue
        if path.name in SKIP_FILENAMES:
            skipped.append((path, "explicitly skipped"))
            continue
        result = inspect_script(path)
        if result.runnable:
            runnable.append(path)
        else:
            skipped.append((path, result.reason))

    return runnable, skipped


def run_script(path: Path) -> int:
    print(f"==> Running {path.name}")
    completed = subprocess.run([sys.executable, str(path)], cwd=SCRIPT_DIR.parent)
    return completed.returncode


def main() -> int:
    runnable, skipped = discover_scripts()

    if skipped:
        for path, reason in skipped:
            print(f"Skipping {path.name}: {reason}")

    if not runnable:
        print("No runnable Python scripts found.")
        return 0

    failures: list[tuple[Path, int]] = []
    for path in runnable:
        return_code = run_script(path)
        if return_code != 0:
            failures.append((path, return_code))

    if failures:
        print("\nFailures:")
        for path, return_code in failures:
            print(f"- {path.name} exited with status {return_code}")
        return 1

    print("\nAll runnable scripts completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

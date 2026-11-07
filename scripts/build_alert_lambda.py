"""Build an offline Python 3.12 Lambda ZIP from lock-verified universal wheels.

No installation, dependency resolution, AWS clients or credential access occurs.
Supply previously downloaded wheels and an output directory outside the checkout.
"""

import argparse
import hashlib
import io
import json
import tomllib
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]


def dependency_closure(packages, name="boto3"):
    result = set()

    def visit(current):
        if current in result:
            return
        result.add(current)
        for dependency in packages[current].get("dependencies", []):
            visit(dependency["name"])

    visit(name)
    return sorted(result)


def wheel_files(package, wheel_dir):
    candidates = [
        w for w in package.get("wheels", []) if w["url"].endswith("-none-any.whl")
    ]
    if len(candidates) != 1:
        raise ValueError(
            "Exactly one universal wheel per Lambda dependency is required"
        )
    wheel = candidates[0]
    data = (wheel_dir / wheel["url"].rsplit("/", 1)[-1]).read_bytes()
    if "sha256:" + hashlib.sha256(data).hexdigest() != wheel["hash"]:
        raise ValueError("Dependency wheel does not match uv.lock")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        result = {}
        for entry in archive.infolist():
            path = PurePosixPath(entry.filename)
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\\" in entry.filename
                or (entry.external_attr >> 16) & 0o170000 == 0o120000
            ):
                raise ValueError("Unsafe wheel archive path")
            if entry.is_dir():
                continue
            # Universal wheels may include optional CLI scripts. Lambda needs no CLI.
            if (
                path.parts[0].endswith(".data")
                and len(path.parts) > 1
                and path.parts[1] == "scripts"
            ):
                continue
            if ".data" in path.parts[0] or path.suffix in (".so", ".pyd", ".pyc"):
                raise ValueError(
                    "Lambda dependencies must be pure Python without install hooks"
                )
            if entry.filename in result:
                raise ValueError("Duplicate wheel path")
            result[entry.filename] = archive.read(entry)
        return result


def build(wheel_dir, output_dir, *, root=ROOT):
    root, output_dir = root.resolve(), output_dir.resolve()
    if output_dir == root or root in output_dir.parents:
        raise ValueError("Build output must be outside the repository")
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path, manifest_path = (
        output_dir / "alert-lambda.zip",
        output_dir / "alert-lambda-manifest.json",
    )
    if archive_path.exists() or manifest_path.exists():
        raise ValueError("Refusing to overwrite an existing deployment artifact")
    lock_bytes = (root / "uv.lock").read_bytes()
    packages = {p["name"]: p for p in tomllib.loads(lock_bytes.decode())["package"]}
    entries, dependencies = {}, {}
    for name in dependency_closure(packages):
        dependencies[name] = packages[name]["version"]
        for path, data in wheel_files(packages[name], wheel_dir).items():
            if path in entries:
                raise ValueError("Overlapping dependency wheel paths")
            entries[path] = data
    sources = {}
    for folder in ("alerts", "bot_lambda/src"):
        for path in sorted((root / folder).glob("*.py")):
            if path.is_symlink():
                raise ValueError("Application source cannot be a symlink")
            relative = path.relative_to(root).as_posix()
            if relative in entries:
                raise ValueError("Application/dependency path collision")
            data = path.read_bytes()
            sources[relative] = hashlib.sha256(data).hexdigest()
            entries[relative] = data
    if "bot_lambda/src/lambda_function.py" not in entries:
        raise ValueError("Lambda handler is missing")
    with zipfile.ZipFile(
        archive_path, "x", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for name, data in sorted(entries.items()):
            entry = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            entry.compress_type, entry.external_attr = zipfile.ZIP_DEFLATED, 0o644 << 16
            archive.writestr(entry, data)
    manifest = {
        "runtime": "python3.12",
        "architecture": "pure-python; validate on target runtime",
        "handler": "bot_lambda.src.lambda_function.lambda_handler",
        "dependencies": dependencies,
        "uv_lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "source_sha256": sources,
        "archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "archive_bytes": archive_path.stat().st_size,
        "file_count": len(entries),
        "deployment_status": "NOT DEPLOYED",
        "wheel_verification": "SHA-256 matched uv.lock",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheels", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.wheels, args.output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

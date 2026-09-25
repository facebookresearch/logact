#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
"""Custom setup to generate protobuf files at install time."""

import tempfile
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


def _ensure_init(out_dir: Path) -> None:
    init_file = out_dir / "__init__.py"
    if not init_file.exists():
        init_file.write_text(
            "# Copyright (c) Meta Platforms, Inc. and affiliates.\n"
            "#\n"
            "# This source code is licensed under the MIT license found in the\n"
            "# LICENSE file in the root directory of this source tree.\n"
            "\n"
            "# Auto-generated protobuf bindings.\n"
        )


class BuildPyWithProto(build_py):
    """Custom build_py that generates protobuf files before building."""

    def run(self):
        self._generate_proto()
        super().run()

    def _generate_proto(self):
        import grpc_tools
        from grpc_tools import protoc

        base_dir = Path(__file__).parent
        proto_include = str(Path(grpc_tools.__file__).parent / "_proto")
        src_dir = base_dir / "src"
        agentbus_out = src_dir / "agentbus_proto"
        appserver_out = src_dir / "appserver_proto"
        agentbus_out.mkdir(parents=True, exist_ok=True)
        appserver_out.mkdir(parents=True, exist_ok=True)

        # ── agent_bus.proto ───────────────────────────────────────
        # We need the descriptor to register as "agentbus_proto/agent_bus.proto"
        # to match the import in appserver.proto. Create a symlink layout
        # so protoc sees the file at that path, and use --python_out=src/
        # so the generated files land in src/agentbus_proto/.
        with tempfile.TemporaryDirectory() as tmpdir:
            inc_dir = Path(tmpdir)
            pkg_dir = inc_dir / "agentbus_proto"
            pkg_dir.mkdir()
            (pkg_dir / "agent_bus.proto").symlink_to(base_dir / "agent_bus.proto")
            result = protoc.main(
                [
                    "grpc_tools.protoc",
                    f"-I{inc_dir}",
                    f"-I{proto_include}",
                    f"--python_out={src_dir}",
                    f"--pyi_out={src_dir}",
                    f"--grpc_python_out={src_dir}",
                    "agentbus_proto/agent_bus.proto",
                ]
            )
        if result != 0:
            raise RuntimeError(f"protoc agent_bus.proto failed (exit {result})")

        _ensure_init(agentbus_out)

        # ── appserver.proto ───────────────────────────────────────
        # appserver.proto imports "agentbus_proto/agent_bus.proto".
        # Reuse the same symlink trick for the include path.
        with tempfile.TemporaryDirectory() as tmpdir:
            inc_dir = Path(tmpdir)
            pkg_dir = inc_dir / "agentbus_proto"
            pkg_dir.mkdir()
            (pkg_dir / "agent_bus.proto").symlink_to(base_dir / "agent_bus.proto")

            result = protoc.main(
                [
                    "grpc_tools.protoc",
                    f"-I{base_dir}",
                    f"-I{inc_dir}",
                    f"-I{proto_include}",
                    f"--python_out={appserver_out}",
                    f"--pyi_out={appserver_out}",
                    f"--grpc_python_out={appserver_out}",
                    "appserver.proto",
                ]
            )
        if result != 0:
            raise RuntimeError(f"protoc appserver.proto failed (exit {result})")

        # Fix imports in generated gRPC file to use package-qualified imports
        grpc_file = appserver_out / "appserver_pb2_grpc.py"
        if grpc_file.exists():
            content = grpc_file.read_text()
            content = content.replace(
                "import appserver_pb2",
                "from appserver_proto import appserver_pb2",
            )
            grpc_file.write_text(content)

        _ensure_init(appserver_out)


setup(cmdclass={"build_py": BuildPyWithProto})

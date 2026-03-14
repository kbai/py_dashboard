#!/usr/bin/env python3
"""Detect unused XML config attributes.

Heuristic:
- Scan C++ sources for `XMLConfig::parseAttribute(config, "Attr", ...)`.
- Associate each attribute with the most recent `ClassName::method(` seen in that file.
- Parse one or more `.cfg` XML files and report attributes present on each node element
  that are not consumed by that class' `parseAttribute` calls.

This is a best-effort static check (string-literals only). It won't detect attributes
read via other helpers or dynamic attribute names.

Usage:
  ./detect_unused_config.py src/Muse2/Engine/testCG.cfg
  ./detect_unused_config.py --src-root src/Muse2 src/Muse2/Engine/*.cfg
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple


_CPP_GLOB_EXTS = {".c", ".cc", ".cpp", ".cxx"}


SENSITIVE_KEYS = {
    "apikey",
    "secret",
    "password",
    "pass",
    "token",
    "key",
}


def is_sensitive_attr(name: str) -> bool:
    n = name.strip().lower()
    return n in SENSITIVE_KEYS or any(k in n for k in ("secret", "apikey", "password", "token"))


def iter_cpp_files(src_root: Path) -> Iterable[Path]:
    for p in src_root.rglob("*"):
        if p.suffix.lower() in _CPP_GLOB_EXTS and p.is_file():
            yield p


def strip_cpp_comments(text: str) -> str:
    """Remove // and /* */ comments (best-effort)."""
    # Remove block comments first
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # Remove line comments
    text = re.sub(r"//.*", "", text)
    return text

def build_attr_usage(src_root: Path) -> Dict[str, Set[str]]:
    usage: DefaultDict[str, Set[str]] = defaultdict(set)

    # We do a light parse: scan file left-to-right; maintain current owner.
    owner_re = re.compile(r"\b([A-Za-z_]\w*)::([A-Za-z_]\w*)\s*\(")
    attr_re = re.compile(r"\bXMLConfig::parseAttribute\s*\(\s*config\s*,\s*\"([^\"]+)\"")

    for cpp in iter_cpp_files(src_root):
        try:
            raw = cpp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        text = strip_cpp_comments(raw)
        current_owner: Optional[str] = None

        # Process line-by-line so owner updates are local and predictable.
        for line in text.splitlines():
            m_owner = owner_re.search(line)
            if m_owner:
                current_owner = m_owner.group(1)

            m_attr = attr_re.search(line)
            if m_attr and current_owner:
                usage[current_owner].add(m_attr.group(1))

    return dict(usage)


@dataclass(frozen=True)
class UnusedAttr:
    node_tag: str
    node_path: str
    attr: str


def iter_node_elements(root: ET.Element) -> Iterable[Tuple[str, ET.Element]]:
    """Yield (path, element) for likely "node" elements.

    We focus on `<Muse2>/<Nodes>/*` children, which are runtime nodes.
    """
    muse2 = root
    if muse2.tag != "Muse2":
        muse2 = root.find("Muse2") or root

    nodes = muse2.find("Nodes")
    if nodes is None:
        return

    for child in list(nodes):
        yield f"Muse2/Nodes/{child.tag}", child


def parse_cfg(path: Path) -> ET.Element:
    # These .cfg files are XML; parse as-is.
    data = path.read_text(encoding="utf-8", errors="ignore")
    return ET.fromstring(data)


def detect_unused_in_cfg(cfg_path: Path, usage: Dict[str, Set[str]]) -> List[UnusedAttr]:
    root = parse_cfg(cfg_path)
    unused: List[UnusedAttr] = []

    for node_path, elem in iter_node_elements(root):
        tag = elem.tag
        # Strip any prefix, e.g. CG:EMACalculation => EMACalculation
        type_name = tag.split(":")[-1]
        used = usage.get(type_name)
        if not used:
            # Unknown node type => we can't say what's unused.
            continue

        for attr_name in elem.attrib.keys():
            if attr_name not in used:
                unused.append(UnusedAttr(node_tag=tag, node_path=node_path, attr=attr_name))

    return unused


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("configs", nargs="+", help=".cfg XML files to analyze")
    ap.add_argument("--src-root", default="src/Muse2", help="Root to scan for C++ parseAttribute usage")
    args = ap.parse_args(argv)

    src_root = Path(args.src_root)
    if not src_root.exists():
        print(f"error: --src-root not found: {src_root}", file=sys.stderr)
        return 2

    usage = build_attr_usage(src_root)

    any_findings = False
    for cfg in args.configs:
        cfg_path = Path(cfg)
        if not cfg_path.exists():
            print(f"missing: {cfg}", file=sys.stderr)
            continue

        try:
            unused = detect_unused_in_cfg(cfg_path, usage)
        except ET.ParseError as e:
            print(f"parse error: {cfg}: {e}", file=sys.stderr)
            continue

        if not unused:
            print(f"{cfg}: no unused attributes detected (for known node types)")
            continue

        any_findings = True
        print(f"{cfg}: unused attributes")
        for item in sorted(unused, key=lambda x: (x.node_tag, x.attr, x.node_path)):
            print(f"  - {item.node_path}: {item.attr}")

    return 1 if any_findings else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

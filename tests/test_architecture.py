import ast
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parent.parent / "app"

# Composition roots wire layers together and are the only modules allowed to see all of them.
COMPOSITION_ROOTS = {APP / "api" / "factory.py"}

# package -> import prefixes it must never use
RULES = {
    "api": ("sqlalchemy", "asyncpg", "app.models", "app.repositories", "app.db"),
    "services": ("sqlalchemy", "asyncpg", "fastapi", "starlette", "faststream", "app.api"),
    "repositories": ("fastapi", "starlette", "httpx", "faststream", "app.api", "app.services"),
}


def imported_modules(path: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.add(node.module)
            modules.update(f"{node.module}.{alias.name}" for alias in node.names)
    return modules


def violations(files: list[Path], forbidden: tuple[str, ...]) -> list[str]:
    found = []
    for path in files:
        for module in sorted(imported_modules(path)):
            if any(module == rule or module.startswith(f"{rule}.") for rule in forbidden):
                found.append(f"{path.name} imports {module}")
    return found


@pytest.mark.parametrize("package", RULES)
def test_layer_imports(package):
    files = [p for p in (APP / package).rglob("*.py") if p not in COMPOSITION_ROOTS]
    assert files
    assert violations(files, RULES[package]) == []


def test_checker_detects_violations(tmp_path):
    source = tmp_path / "bad.py"
    source.write_text("import sqlalchemy.orm\nfrom app.models import Payment\nimport os\n")
    assert violations([source], ("sqlalchemy", "app.models")) == [
        "bad.py imports app.models",
        "bad.py imports app.models.Payment",
        "bad.py imports sqlalchemy.orm",
    ]
    assert violations([source], ("fastapi",)) == []

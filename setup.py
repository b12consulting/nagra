from pathlib import Path

from setuptools import setup


def get_version():
    ini_path = Path(__file__).parent / "nagra" / "__init__.py"
    for line in ini_path.open():
        if line.startswith("__version__"):
            return line.split("=")[1].strip("' \"\n")
    raise ValueError(f"__version__ line not found in {ini_path}")

long_description = """Nagra is a Python ORM that tries to emphasise
the declarative nature of relational databases. It also comes with
builtin features usually not available in traditonal ORM like
declarative row-level permissions or a command-line interface"""

setup(
    name="Nagra",
    version=get_version(),
    description=" ORM-like library for OLAP use cases",
    long_description=long_description,
    url="https://github.com/b12consulting/nagra",
    install_requires=[
        "jinja2",
        "rich",
        "toml",
    ],
    packages=[
        "nagra",
    ],
    extras_require={
        "test": ["pytest", "pandas"],
    },
    package_data={"nagra": ["template/*/*sql"]},
    entry_points={
        "console_scripts": [
            "nagra = nagra.cli:run",
        ],
    },
)

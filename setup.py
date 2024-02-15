from setuptools import setup


setup(
    name="Nagra",
    version="0.0.2",
    description="Query databases without models",
    install_requires=[
        "jinja2",
        "tabulate",
        "toml",
    ],
    packages=[
        "nagra",
    ],
    extras_require={
        "test": ["pytest"],
    },
    package_data={"nagra": ["template/*/*sql"]},  # TODO test this works!
    entry_points={
        "console_scripts": [
            "nagra = nagra.cli:run",
        ],
    },
)

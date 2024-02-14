from setuptools import find_packages, setup

setup(
    name="diplwmatikh",
    packages=find_packages(exclude=["diplwmatikh_tests"]),
    install_requires=[
        "dagster-pandera",
        "dagster-postgres",
        "psycopg2",
        "dagster",
        "requests",
        "pandas",
        "pangres",
        "matplotlib",
        "entsoe-py",
        "entsog-py"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

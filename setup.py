from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="reddit_datasource",
        packages=find_packages(exclude=["reddit_datasource_tests"]),
        install_requires=[
            "dagster",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )

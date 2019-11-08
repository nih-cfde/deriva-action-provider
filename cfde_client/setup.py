from setuptools import find_packages, setup


setup(
    name="cfde_client",
    version="0.0.1",
    packages=find_packages(),
    entry_points='''
    [console_scripts]
    cfde=cfde_client.main:cli
''',
    install_requires=[
        "bdbag>=1.5.5",
        "Click>=7.0",
        "datapackage>=1.10.0",
        "fair-research-login>=0.1.3",
        "GitPython>=3.0.4",
        "globus-automate-client>=0.4",
        "globus-sdk>=1.8.0",
        "requests>=2.22.0"
    ],
    python_requires=">=3.4"
)

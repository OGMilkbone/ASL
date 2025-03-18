from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="adaptive-schema-layer",
    version="0.1.0",
    author="OGMilkbone",
    author_email="your.email@example.com",
    description="A revolutionary approach to schema evolution in distributed systems",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OGMilkbone/ASL",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "redis>=4.5.0",
        "pydantic>=2.0.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.22.0",
        "jsonschema>=4.17.0",
        "jsonpatch>=1.33",
    ],
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.3.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.3.0",
            "mkdocs>=1.4.0",
            "mkdocs-material>=9.2.0",
        ],
    },
) 
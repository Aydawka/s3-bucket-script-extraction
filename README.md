# Software Heritage README DOI & EMD Extractor

## Overview

This repository contains a scalable extraction pipeline designed to
retrieve README files from the Software Heritage archive and extract:

-   DOI references
-   EMD (Electron Microscopy Data Bank) identifiers

The system operates over large-scale datasets using AWS Athena and S3,
with parallelized HTTP retrieval and decompression.

The repository consists of six primary scripts. Each script processes a
partition of the Software Heritage table to enable distributed execution
and improved performance.

------------------------------------------------------------------------
## Architecture

The pipeline follows this workflow:

1.  Query metadata (repository URL, SHA1, visit date) from AWS Athena.
2.  Download the Athena result CSV from S3.
3.  Fetch compressed README content from Software Heritage S3 storage.
4.  Decompress content using `pigz`.
5.  Extract DOIs and EMD identifiers using regex patterns.
6.  Write structured results to S3 as NDJSON.
7.  Log skipped entries and checkpoint progress.

Each script processes a different partition of the Software Heritage
table to distribute workload efficiently.

------------------------------------------------------------------------

## Repository Structure

-   `script_part1.py`
-   `script_part2.py`
-   `script_part3.py`
-   `script_part4.py`
-   `script_part5.py`
-   `script_part6.py`
-   (Optional) Graph database validation / verification script

Each script is structurally similar but targets a different Athena table
partition.

## Environment Setup

### 1. Clone the repository

    git clone <your-repository-url>
    cd <repository-folder>

### 2. Create a virtual environment

On Windows:

    python -m venv venv
    venv\Scripts\activate

On macOS / Linux:

    python3 -m venv venv
    source venv/bin/activate

### 3. Install dependencies

Create a `requirements.txt` file with:

    boto3
    requests
    urllib3

Then install:

    pip install -r requirements.txt

### 4. Install pigz

Ensure `pigz` is installed and accessible from the command line.

Linux:

    sudo apt install pigz

macOS:

    brew install pigz

### 5. Configure AWS credentials

Make sure AWS credentials are configured using one of:

    aws configure

or environment variables:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

------------------------------------------------------------------------

## Usage

Run any partition script:

    python script_partX.py

Where `X` corresponds to the partition number (1--6).

Each script:

1.  Executes an Athena query.
2.  Downloads the result CSV from S3.
3.  Fetches compressed README files from Software Heritage.
4.  Extracts DOIs and EMD identifiers.
5.  Stores results back into S3.

------------------------------------------------------------------------

## Extraction Logic

### DOI Extraction

Multiple regex patterns are used to capture:

-   Standard DOI links (doi.org)
-   Inline DOI references
-   DOI-prefixed strings
-   Raw DOI patterns

Extracted DOIs are normalized and cleaned to remove:

-   URL prefixes
-   Trailing punctuation
-   Unbalanced brackets

### EMD Extraction

EMD identifiers are extracted using:

    EMD-XXXX or EMD-XXXXX

------------------------------------------------------------------------

## AWS Components

-   **Athena** -- Query metadata (URL, SHA1, visit date)
-   **S3** -- Store Athena results and extraction outputs
-   **Software Heritage S3** -- Source of compressed README files

------------------------------------------------------------------------

## Performance Characteristics

-   ThreadPoolExecutor parallelization (default: 30 workers)
-   HTTP retry strategy with exponential backoff
-   Rate limiting protection
-   Periodic checkpoint saves
-   Batch processing for memory control

------------------------------------------------------------------------

## Output Format

Results are stored as NDJSON with structure:

``` json
{
  "repo_link": "repository_url",
  "repo_creation_date": "YYYY-MM-DD",
  "dois_mentioned": ["10.xxxx/..."],
  "emd_ids": ["EMD-1234"]
}
```

Skipped entries are stored separately with reason codes.

------------------------------------------------------------------------

## Scalability Design

The Software Heritage dataset was split into six partitions to:

-   Reduce single-query bottlenecks
-   Enable parallel execution
-   Improve fault tolerance
-   Lower memory pressure
-   Allow incremental processing

Additional validation or graph-based scripts may be added in future
versions.

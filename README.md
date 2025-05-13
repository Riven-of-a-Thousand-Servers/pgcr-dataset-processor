# PGCR Dataset Processor

This is a lightweight Go CLI tool that imports Destiny 2 Post Game Carnage Reports (PGCRs) from [D2Asun’s](https://d2.asun.co/pgcr.html) dataset into a PostgreSQL database.

## Purpose
Fetching PGCRs directly from Bungie's API is inefficient due to the massive volume (>16 billion) and infrastructure constraints. D2Asun provides a precompiled dataset that simplifies the process.

## How It Works
The tool scans a directory for .zst files, then:

1. Decompresses each file 
2. Parses it as JSON
3. Compresses the PGCR with Gzip
4. Stores it in a PostgreSQL database using the PGCR ID as the key

This utility is meant to help developers quickly ingest PGCR data for use in stats tracking, leaderboards, or analysis.

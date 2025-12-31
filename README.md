# Clinical Trial Intelligence Platform - Data Infrastructure (Person 1)

## Overview
Event-driven data infrastructure providing real-time ingestion, harmonization, and quality assurance for clinical trial operations.

## Key Features
- **Latency-Tiered Ingestion**: P0 (<5min), P1 (<15min), P2 (<1hr), P3 (<6hr)
- **Canonical Data Model**: SDTM-inspired schema with full provenance
- **Quality Scoring**: Multi-dimensional data quality assessment
- **Audit Trail**: 100% immutable event logging
- **Failure Handling**: Graceful degradation with retry logic

## Quick Start
```bash
# Clone repository
git clone https://github.com/Zenoguy/CLINICAL-TRIAL-PLATFORM.git
cd clinical-trial-platform

# Setup environment
make setup

# Start infrastructure
make docker-up

# Run database migrations
make migrate

# Generate test data
make simulate

# Start ingestion engine
make run-ingestion

# Run tests
make test

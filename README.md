# ISAAC AI-Ready Scientific Record

**Version:** 1.0 (Frozen)

## Overview
This repository defines the authoritative standard for the **ISAAC AI-Ready Record**. 
It provides the schema, documentation, and examples required to represent scientific data in a format that is semantically rigorous, machine-readable, and optimized for autonomous agent reasoning.

## Documentation Authority
The **[Wiki](wiki/Home.md)** is the normative single source of truth for the v1.0 standard. All definitions, constraints, and vocabularies are rigorously defined there.

*   **[Record Overview](wiki/Record-Overview.md)**: The 8-block anatomy of a record.
*   **[Measurement](wiki/Measurement.md)**: The observable schema and data contract.
*   **[System](wiki/System.md)**: Infrastructure and configuration definitions.
*   **[Sample](wiki/Sample.md)**: Material identity and realization.
*   **[Links](wiki/Links.md)**: The knowledge graph ontology.
*   **[Descriptors](wiki/Descriptors.md)**: Scientific claims and features.
*   **[Assets](wiki/Assets.md)**: External immutable objects.

## Repository Structure
*   `schema/`: Strict JSON Schema definitions (`isaac_record_v1.json`) for validation.
*   `examples/`: **Golden Records** demonstrating 100% compliant usage across domains:
    *   `operando_xanes_co2rr_record.json`: Operando characterization.
    *   `simulation_xas_record.json`: Computational simulation.
    *   `ex_situ_xanes_cuo2_record.json`: Basic experimental characterization.
    *   `co2rr_performance_record.json`: Flow cell performance.
    *   `echem_performance_record.json`: RDE electrochemistry.
*   `wiki/`: Detailed normative documentation.

## Core Design Philosophy (v1.0)
1.  **Strict Separation of Concerns**: Sample identity != Measurement data != System config.
2.  **Machine-First Semantics**: Closed vocabularies for all structural types to enable reliable agent queries.
3.  **Refrence-Based**: Heavy data lives in immutable Assets; the Record is the metadata graph.
4.  **Shared Abstraction**: Experiment and Simulation share the same `measurement.series` structure for direct comparability.

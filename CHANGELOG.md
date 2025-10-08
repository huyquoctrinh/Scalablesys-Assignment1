# Changelog

This file documents the changes made to the OpenCEP codebase.

## [Unreleased]

### Added

- Created `loadshedding/StatefulLoadSheddingStrategy.py`: Introduced an abstract base class for stateful load shedding strategies that can prune partial matches.
- Created `loadshedding/PriorityBasedLoadShedding.py`: Implemented a stateful load shedding strategy that prioritizes partial matches based on their length.

### Changed

- **Refactored `StateAwareTreeBasedEvaluationMechanism` for functional load shedding:**
    - Replaced the simulated evaluation logic with the real tree-based evaluation from the parent class.
    - The mechanism now correctly processes events, collects all partial matches from the evaluation tree, and applies the load shedding strategy.
- **Improved `PriorityBasedLoadShedding`:**
    - The priority calculation now considers both the length and age of a partial match, providing a more sophisticated and effective load shedding strategy.
- Modified `CEP.py`:
    - Updated the constructor to accept a pre-built evaluation mechanism, allowing for more flexible configurations.

### Fixed

- **`memory_optimized_demo.py`:**
    - Fixed a `TypeError` in the `CEP` constructor call.
    - Fixed an `AttributeError` in `StreamingCitiBikeInputStream` to correctly add events to the stream.
- **`StateAwareTreeBasedEvaluationMechanism`:**
    - Fixed an issue where it was not creating `TreePlan` objects correctly, by using a `TrivialLeftDeepTreeBuilder`.
    - Provided dummy statistics to the `TreePlanBuilder` to avoid errors.

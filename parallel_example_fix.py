#!/usr/bin/env python3
"""
Example of correct parallel execution parameters usage
"""

from datetime import timedelta
from parallel.ParallelExecutionParameters import (
    DataParallelExecutionParameters,
    ParallelExecutionModes
)
from parallel.ParallelExecutionModes import DataParallelExecutionModes
from CEP import CEP

# CORRECT: Use DataParallelExecutionParameters for units_number
def create_parallel_cep(patterns, config=None, num_units=4):
    """Create CEP with parallel execution."""
    
    # Option 1: Use DataParallelExecutionParameters
    parallel_params = DataParallelExecutionParameters(
        data_parallel_mode=DataParallelExecutionModes.GROUP_BY_KEY_ALGORITHM,  # or other algorithm
        units_number=num_units
    )
    
    # Option 2: For specific algorithms
    # from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm
    # parallel_params = DataParallelExecutionParametersHirzelAlgorithm(
    #     units_number=num_units,
    #     key="bikeid"  # partition key
    # )
    
    return CEP(patterns, parallel_execution_params=parallel_params, load_shedding_config=config)

# INCORRECT (causes the error you saw):
# parallel_params = ParallelExecutionParameters(units_number=4)  # ❌ No units_number parameter

if __name__ == "__main__":
    print("Example of correct parallel CEP setup")
    
    # Dummy pattern for example
    from base.Pattern import Pattern
    from base.PatternStructure import PrimitiveEventStructure
    
    pattern = Pattern(
        PrimitiveEventStructure("BikeTrip", "a"),
        condition=None,
        time_window=timedelta(minutes=30)
    )
    
    # This works:
    cep_parallel = create_parallel_cep([pattern], num_units=4)
    print(f"Created parallel CEP: {type(cep_parallel)}")
    print("Parallel execution configured successfully!")
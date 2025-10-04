# loadshedding/wrapper.py
"""
Wrapper for CEP class that adds load shedding without modifying original.
"""
from CEP import CEP as OriginalCEP
from loadshedding.manager import LoadSheddingManager
from loadshedding.proxy import EvaluationMechanismProxy
from loadshedding.config import LoadSheddingConfig


class LoadSheddingCEP:
    """
    Wrapper around original CEP that adds load shedding functionality.
    Uses composition and delegation pattern.
    """
    
    def __init__(self, patterns, eval_mechanism_params=None, 
                 load_shedding_config: LoadSheddingConfig = None):
        """
        Initialize CEP with load shedding.
        
        Args:
            patterns: Pattern or list of patterns
            eval_mechanism_params: Evaluation mechanism parameters
            load_shedding_config: Load shedding configuration
        """
        # Create original CEP instance
        self._cep = OriginalCEP(patterns, eval_mechanism_params)
        
        # Configure load shedding manager (singleton)
        self._manager = LoadSheddingManager.get_instance()
        self._load_shedding_config = load_shedding_config
        
        if load_shedding_config:
            self._manager.configure(load_shedding_config)
        
        # Wrap the evaluation mechanism with proxy
        if load_shedding_config and load_shedding_config.enabled:
            self._cep.eval_mechanism = EvaluationMechanismProxy(
                self._cep.eval_mechanism
            )
    
    def run(self, input_stream, output_stream, data_formatter):
        """Run CEP with load shedding"""
        return self._cep.run(input_stream, output_stream, data_formatter)
    
    def is_load_shedding_enabled(self):
        """Check if load shedding is enabled"""
        return self._manager.is_enabled()
    
    def get_load_shedding_statistics(self):
        """Get load shedding statistics"""
        return self._manager.get_statistics()
    
    def reset_load_shedding(self):
        """Reset load shedding state (useful between test runs)"""
        self._manager.reset()
        if self._load_shedding_config:
            self._manager.configure(self._load_shedding_config)
    
    def __getattr__(self, name):
        """Delegate all other attributes to wrapped CEP"""
        return getattr(self._cep, name)
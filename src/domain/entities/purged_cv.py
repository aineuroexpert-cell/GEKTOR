import pandas as pd
import numpy as np
from typing import List, Tuple
from loguru import logger

class PurgedKFold:
    """
    [GEKTOR v21.74] Marcos Lopez de Prado's Purged K-Fold CV.
    Destroys Data Leakage in financial time series by applying 
    Purging (overlapping labels) and Embargo (serial correlation quarantine).
    """
    def __init__(self, n_splits: int = 5, embargo_pct: float = 0.01):
        self.n_splits = n_splits
        self.embargo_pct = embargo_pct

    def split(self, events: pd.DataFrame) -> List[Tuple[np.ndarray, np.ndarray]]:
        """
        events: DataFrame with index=EntryTime, column 't1'=ExitTime
        """
        t1 = events['t1']
        indices = np.arange(events.shape[0])
        embargo_size = int(events.shape[0] * self.embargo_pct)
        
        # 1. Standard chunking into Test Folds
        test_starts = [(i[0], i[-1] + 1) for i in np.array_split(np.arange(events.shape[0]), self.n_splits)]
        
        splits = []
        for test_start_idx, test_end_idx in test_starts:
            # Test indices
            test_indices = indices[test_start_idx:test_end_idx]
            
            # The actual timestamps of the Test Set boundaries
            test_t0 = events.index[test_start_idx]
            test_t1 = t1.iloc[test_end_idx - 1] if test_end_idx < len(t1) else t1.iloc[-1]
            
            train_indices = []
            
            for i in indices:
                if i in test_indices:
                    continue
                    
                entry_time = events.index[i]
                exit_time = t1.iloc[i]
                
                # -------------------------------------------------------------
                # 2. PURGING (Очистка)
                # If a Train observation's evaluation window overlaps with the Test set
                # -------------------------------------------------------------
                if entry_time <= test_t1 and exit_time >= test_t0:
                    continue # Purged! (Data Leakage prevented)
                    
                # -------------------------------------------------------------
                # 3. EMBARGO (Карантин памяти)
                # If a Train observation starts immediately AFTER the Test set
                # -------------------------------------------------------------
                if entry_time > test_t1:
                    # Is it within the Embargo window?
                    embargo_boundary_idx = min(test_end_idx + embargo_size, len(events) - 1)
                    embargo_t1 = events.index[embargo_boundary_idx]
                    
                    if entry_time <= embargo_t1:
                        continue # Embargoed! (Serial correlation prevented)
                
                # If it survived Purging and Embargo, it is safe for Training
                train_indices.append(i)
                
            splits.append((np.array(train_indices), test_indices))
            
        logger.success(f"🔪 [PURGED CV] Generated {self.n_splits} clean splits. Leakage destroyed.")
        return splits

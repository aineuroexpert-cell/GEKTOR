import numpy as np
from loguru import logger

class SentryBrain:
    """
    [GEKTOR v13.4] Sectorial Sensitivity Matrix (SSM).
    Manages "Sector Heat" to implement Sympathetic Echo and Predictive Triage.
    """
    def __init__(self, symbols: list, sector_config: dict):
        self.symbols = symbols
        self.num_symbols = len(symbols)
        self.symbol_to_idx = {s: i for i, s in enumerate(symbols)}
        
        # Define sectors
        all_sectors = sorted(list(set(sector_config.values())))
        if not all_sectors: all_sectors = ["UNKNOWN"]
        
        self.sectors = all_sectors
        self.num_sectors = len(self.sectors)
        self.sector_to_idx = {name: i for i, name in enumerate(self.sectors)}
        
        # Mapping: symbol_idx -> sector_idx
        self.symbol_sector_map = np.zeros(self.num_symbols, dtype=np.int32)
        for sym, sector in sector_config.items():
            if sym in self.symbol_to_idx:
                self.symbol_sector_map[self.symbol_to_idx[sym]] = self.sector_to_idx[sector]

        # Thermodynamics v13.5
        self.sector_heat = np.zeros(self.num_sectors, dtype=np.float64)
        self.sector_fatigue = np.zeros(self.num_sectors, dtype=np.float64)
        
        self.base_decay = 0.995 
        self.sensitivity_factor = 0.4 # Increment per awakening

    def on_symbol_awakening(self, symbol: str):
        """Heat up the entire sector when one member wakes up."""
        if symbol in self.symbol_to_idx:
            idx = self.symbol_to_idx[symbol]
            s_idx = self.symbol_sector_map[idx]
            # Heat capacity is higher, but fatigue will fight it back
            self.sector_heat[s_idx] = min(2.0, self.sector_heat[s_idx] + self.sensitivity_factor)

    def on_awakening_failed(self, symbol: str):
        """Called if a symbol woke up but failed to generate an Alpha Intent."""
        if symbol in self.symbol_to_idx:
            s_idx = self.symbol_sector_map[self.symbol_to_idx[symbol]]
            self.sector_fatigue[s_idx] = min(50.0, self.sector_fatigue[s_idx] + 1.0)

    def on_intent_generated(self, symbol: str):
        """Successful signal validates the sector. Reward efficiency."""
        if symbol in self.symbol_to_idx:
            s_idx = self.symbol_sector_map[self.symbol_to_idx[symbol]]
            self.sector_fatigue[s_idx] = max(0.0, self.sector_fatigue[s_idx] - 2.0)

    def tick_decay(self):
        """Adaptive Cooling: Fatigued sectors cool down faster."""
        # Decay is accelerated by fatigue: Decay_adj = base / (1 + fatigue * 0.1)
        adj_decay = self.base_decay / (1.0 + self.sector_fatigue * 0.1)
        self.sector_heat *= adj_decay

    def get_adjusted_threshold(self, symbol: str, base_threshold: float) -> float:
        """O(1) Threshold adjustment with Fatigue Wall."""
        if symbol not in self.symbol_to_idx: return base_threshold
        
        idx = self.symbol_to_idx[symbol]
        s_idx = self.symbol_sector_map[idx]
        heat = self.sector_heat[s_idx]
        fatigue = self.sector_fatigue[s_idx]
        
        # 1. Reduction via Heat (Sympathetic Echo)
        heat_multiplier = np.clip(1.0 - (heat * 0.8), 0.2, 1.0)
        
        # 2. Penalty via Fatigue (Wolf Cry Wall)
        # Exponential wall after 10 failed attempts
        fatigue_wall = np.exp(max(0.0, fatigue - 10.0) * 0.3)
        
        return base_threshold * heat_multiplier * fatigue_wall

    def get_sector_confidence(self, symbol: str) -> float:
        """
        Returns 0.0-1.5 confidence boost based on sector activity and stability.
        High fatigue = low trust (WarZone).
        """
        if symbol not in self.symbol_to_idx: return 0.5
        s_idx = self.symbol_sector_map[self.symbol_to_idx[symbol]]
        
        # Confidence score adjusted by fatigue
        confidence = self.sector_heat[s_idx] / (1.0 + self.sector_fatigue[s_idx] * 0.5)
        return float(np.clip(confidence, 0.0, 1.5))

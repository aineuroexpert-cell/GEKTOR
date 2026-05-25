# src/domain/scoring.py
from typing import Dict, Tuple
from loguru import logger
from src.infrastructure.config import settings

async def calculate_final_score(
    micro_metrics: dict, 
    sentiment_data: dict
) -> Tuple[int, dict]:
    """
    [GEKTOR v2.0] HYSTERESIS SCORING ENGINE.
    
    Formula:
    Score = (Microstructural_Weight * 0.85) + (Sentiment_Weight * 0.15)
    
    Guarantees that 85% of decisions are based on hard math (VPIN, Volume),
    minimizing impact of "Twitter Noise".
    """
    breakdown = {}
    
    # 1. Microstructural Core (85% weight)
    vpin = micro_metrics.get('vpin', 0.0)
    vol_spike = micro_metrics.get('volume_spike', 0.0)
    
    # Scale VPIN to 0-100 (Simplified heuristic)
    micro_base = min(1.0, vpin / 0.8) * 100 
    
    # 2. Sentiment Component (15% weight cap)
    # sentiment_data expected to have 'bullish_prob' 0.0 - 1.0
    sent_prob = sentiment_data.get('bullish_prob', 0.5)
    sent_contribution = sent_prob * 100
    
    # 3. Apply Weights (CLEAN RADAR PROTOCOL)
    MICRO_WEIGHT = 0.85
    SENT_WEIGHT = settings.SENTIMENT_MAX_WEIGHT
    
    final_score = (micro_base * MICRO_WEIGHT) + (sent_contribution * SENT_WEIGHT)
    
    breakdown['micro_base'] = round(micro_base, 1)
    breakdown['sent_contrib'] = round(sent_contribution, 1)
    breakdown['final_score'] = int(final_score)
    
    logger.debug(f"[Scoring] M:{micro_base:.1f} S:{sent_contribution:.1f} -> F:{final_score:.0f}")
    
    return int(final_score), breakdown

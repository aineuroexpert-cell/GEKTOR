/**
 * [GEKTOR v15.0] React Hook: useTiltBreaker
 *
 * Provides reactive access to the TiltBreakerEngine for React components.
 * Manages lifecycle (bind/destroy) and exposes snapshot + action methods.
 */

import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  TiltBreakerEngine,
  TiltSnapshot,
  TiltState,
} from '../engine/TiltBreakerEngine';

interface UseTiltBreakerReturn {
  /** Current tilt snapshot (reactive — triggers re-render on change) */
  snapshot: TiltSnapshot;
  /** Call when APPROVED_EXECUTION signal is displayed to operator */
  onSignalDisplayed: () => void;
  /** Call when operator clicks execution button. Returns false if blocked. */
  onExecutionClick: () => boolean;
  /** Call when trade P&L is resolved */
  onTradeResult: (isProfitable: boolean) => void;
  /** O(1) gate check — does NOT trigger re-render */
  isExecutionAllowed: () => boolean;
}

const DEFAULT_SNAPSHOT: TiltSnapshot = {
  state: TiltState.CLEAR,
  score: 0,
  reactionDrift: 0,
  errorStreak: 0,
  spamIntensity: 0,
  generation: 0,
  lockedUntilEpoch: 0,
  isExecutionAllowed: true,
};

/**
 * Hook: binds TiltBreakerEngine to a WebSocket and provides reactive state.
 *
 * @param ws - Active WebSocket connection to signal_stream
 */
export function useTiltBreaker(ws: WebSocket | null): UseTiltBreakerReturn {
  const engineRef = useRef<TiltBreakerEngine | null>(null);
  const [snapshot, setSnapshot] = useState<TiltSnapshot>(DEFAULT_SNAPSHOT);

  // Initialize engine once
  useEffect(() => {
    engineRef.current = new TiltBreakerEngine();
    return () => {
      engineRef.current?.destroy();
      engineRef.current = null;
    };
  }, []);

  // Bind engine to WebSocket
  useEffect(() => {
    const engine = engineRef.current;
    if (!engine || !ws) return;

    engine.bind(ws, (newSnapshot: TiltSnapshot) => {
      setSnapshot(newSnapshot);
    });

    return () => {
      engine.destroy();
    };
  }, [ws]);

  const onSignalDisplayed = useCallback(() => {
    engineRef.current?.onSignalDisplayed();
  }, []);

  const onExecutionClick = useCallback((): boolean => {
    return engineRef.current?.onExecutionClick() ?? false;
  }, []);

  const onTradeResult = useCallback((isProfitable: boolean) => {
    engineRef.current?.onTradeResult(isProfitable);
  }, []);

  const isExecutionAllowed = useCallback((): boolean => {
    return engineRef.current?.isExecutionAllowed() ?? false;
  }, []);

  return useMemo(
    () => ({
      snapshot,
      onSignalDisplayed,
      onExecutionClick,
      onTradeResult,
      isExecutionAllowed,
    }),
    [snapshot, onSignalDisplayed, onExecutionClick, onTradeResult, isExecutionAllowed],
  );
}

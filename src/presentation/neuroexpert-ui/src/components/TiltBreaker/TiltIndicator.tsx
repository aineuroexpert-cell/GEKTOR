/**
 * [GEKTOR v15.0] TiltIndicator — Real-Time Cognitive Health Gauge.
 *
 * A compact, always-visible indicator showing the operator's current tilt score.
 * Changes color and animation intensity as the score escalates:
 *   - CLEAR:    Green, subtle pulse
 *   - ELEVATED: Amber, faster pulse, expanded view
 *   - CRITICAL: Red, aggressive pulse, full breakdown
 *
 * Positioned as a floating badge (bottom-right by default).
 * Click to expand detailed metrics panel.
 */

import React, { useState, useMemo } from 'react';
import { TiltSnapshot, TiltState } from '../../engine/TiltBreakerEngine';

interface TiltIndicatorProps {
  snapshot: TiltSnapshot;
}

export const TiltIndicator: React.FC<TiltIndicatorProps> = ({ snapshot }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const theme = useMemo(() => getTheme(snapshot.state, snapshot.score), [
    snapshot.state,
    snapshot.score,
  ]);

  // Don't render during LOCKED/BLIND (LockoutOverlay handles those)
  if (
    snapshot.state === TiltState.LOCKED ||
    snapshot.state === TiltState.COOLDOWN ||
    snapshot.state === TiltState.BLIND
  ) {
    return null;
  }

  return (
    <>
      <div
        id="tilt-indicator"
        style={{
          ...indicatorStyles.container,
          background: theme.bg,
          borderColor: theme.border,
          boxShadow: theme.glow,
        }}
        onClick={() => setIsExpanded(!isExpanded)}
        title="Когнитивный монитор оператора"
      >
        {/* Status dot */}
        <div
          style={{
            ...indicatorStyles.dot,
            background: theme.dotColor,
            animation: theme.dotAnimation,
          }}
        />

        {/* Score */}
        <span
          style={{
            ...indicatorStyles.scoreText,
            color: theme.textColor,
          }}
        >
          {(snapshot.score * 100).toFixed(0)}%
        </span>

        {/* Label */}
        <span style={indicatorStyles.label}>
          {snapshot.state === TiltState.ELEVATED ? 'TILT ⚠️' : 'TILT'}
        </span>
      </div>

      {/* Expanded panel */}
      {isExpanded && (
        <div style={indicatorStyles.expandedPanel}>
          <div style={indicatorStyles.panelHeader}>
            <span style={indicatorStyles.panelTitle}>Cognitive Health Monitor</span>
            <span
              style={{
                ...indicatorStyles.stateBadge,
                background: theme.badgeBg,
                color: theme.badgeText,
              }}
            >
              {snapshot.state}
            </span>
          </div>

          <div style={indicatorStyles.panelMetrics}>
            <PanelMetric
              label="Reaction Drift"
              value={`${(snapshot.reactionDrift * 100).toFixed(0)}%`}
              bar={Math.min(1, snapshot.reactionDrift / 2)}
              color={snapshot.reactionDrift > 1.5 ? '#ef4444' : '#22c55e'}
            />
            <PanelMetric
              label="Error Streak"
              value={`${snapshot.errorStreak}`}
              bar={Math.min(1, snapshot.errorStreak / 4)}
              color={snapshot.errorStreak >= 3 ? '#ef4444' : '#22c55e'}
            />
            <PanelMetric
              label="Click Entropy"
              value={snapshot.spamIntensity.toFixed(1)}
              bar={Math.min(1, snapshot.spamIntensity / 5)}
              color={snapshot.spamIntensity > 3 ? '#ef4444' : '#22c55e'}
            />
          </div>

          <div style={indicatorStyles.panelFooter}>
            Gen: {snapshot.generation} | Composite: {(snapshot.score * 100).toFixed(1)}%
          </div>
        </div>
      )}

      {/* Keyframes */}
      <style>{`
        @keyframes tilt-dot-normal {
          0%, 100% { opacity: 0.6; }
          50% { opacity: 1; }
        }
        @keyframes tilt-dot-warning {
          0%, 100% { opacity: 0.5; transform: scale(1); }
          50% { opacity: 1; transform: scale(1.3); }
        }
        @keyframes tilt-dot-critical {
          0%, 100% { opacity: 0.4; transform: scale(1); }
          25% { opacity: 1; transform: scale(1.5); }
          75% { opacity: 0.8; transform: scale(1.2); }
        }
      `}</style>
    </>
  );
};

// ─── PanelMetric Sub-Component ───────────────────────────────────

interface PanelMetricProps {
  label: string;
  value: string;
  bar: number; // [0, 1]
  color: string;
}

const PanelMetric: React.FC<PanelMetricProps> = ({ label, value, bar, color }) => (
  <div style={indicatorStyles.metric}>
    <div style={indicatorStyles.metricHeader}>
      <span style={indicatorStyles.metricLabel}>{label}</span>
      <span style={{ ...indicatorStyles.metricValue, color }}>{value}</span>
    </div>
    <div style={indicatorStyles.barOuter}>
      <div
        style={{
          ...indicatorStyles.barInner,
          width: `${bar * 100}%`,
          background: color,
        }}
      />
    </div>
  </div>
);

// ─── Theme Helper ────────────────────────────────────────────────

interface IndicatorTheme {
  bg: string;
  border: string;
  glow: string;
  dotColor: string;
  dotAnimation: string;
  textColor: string;
  badgeBg: string;
  badgeText: string;
}

function getTheme(state: TiltState, score: number): IndicatorTheme {
  if (state === TiltState.ELEVATED || score >= 0.4) {
    return {
      bg: 'rgba(30, 20, 5, 0.95)',
      border: 'rgba(245, 158, 11, 0.4)',
      glow: '0 0 20px rgba(245, 158, 11, 0.15)',
      dotColor: '#f59e0b',
      dotAnimation: 'tilt-dot-warning 1s ease-in-out infinite',
      textColor: '#f59e0b',
      badgeBg: 'rgba(245, 158, 11, 0.15)',
      badgeText: '#f59e0b',
    };
  }

  // CLEAR / default
  return {
    bg: 'rgba(5, 15, 5, 0.9)',
    border: 'rgba(34, 197, 94, 0.2)',
    glow: '0 0 10px rgba(34, 197, 94, 0.08)',
    dotColor: '#22c55e',
    dotAnimation: 'tilt-dot-normal 3s ease-in-out infinite',
    textColor: '#22c55e',
    badgeBg: 'rgba(34, 197, 94, 0.1)',
    badgeText: '#22c55e',
  };
}

// ─── Styles ──────────────────────────────────────────────────────

const indicatorStyles: Record<string, React.CSSProperties> = {
  container: {
    position: 'fixed',
    bottom: 24,
    right: 24,
    zIndex: 9990,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '8px 16px',
    borderRadius: 12,
    border: '1px solid',
    cursor: 'pointer',
    transition: 'all 200ms ease',
    backdropFilter: 'blur(8px)',
    WebkitBackdropFilter: 'blur(8px)',
    fontFamily: "'Inter', system-ui, sans-serif",
    userSelect: 'none',
  },
  dot: {
    width: 8,
    height: 8,
    borderRadius: '50%',
    flexShrink: 0,
  },
  scoreText: {
    fontSize: 14,
    fontWeight: 700,
    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
  },
  label: {
    fontSize: 10,
    fontWeight: 600,
    color: '#71717a',
    textTransform: 'uppercase' as const,
    letterSpacing: '0.1em',
  },

  // Expanded panel
  expandedPanel: {
    position: 'fixed',
    bottom: 72,
    right: 24,
    zIndex: 9990,
    width: 280,
    padding: 20,
    borderRadius: 16,
    background: 'rgba(10, 10, 15, 0.97)',
    border: '1px solid rgba(255, 255, 255, 0.08)',
    backdropFilter: 'blur(12px)',
    WebkitBackdropFilter: 'blur(12px)',
    boxShadow: '0 8px 32px rgba(0, 0, 0, 0.5)',
    fontFamily: "'Inter', system-ui, sans-serif",
  },
  panelHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 16,
  },
  panelTitle: {
    fontSize: 12,
    fontWeight: 600,
    color: '#a1a1aa',
    textTransform: 'uppercase' as const,
    letterSpacing: '0.05em',
  },
  stateBadge: {
    fontSize: 10,
    fontWeight: 700,
    padding: '2px 8px',
    borderRadius: 6,
    textTransform: 'uppercase' as const,
  },
  panelMetrics: {
    display: 'flex',
    flexDirection: 'column' as const,
    gap: 12,
  },
  panelFooter: {
    marginTop: 16,
    paddingTop: 12,
    borderTop: '1px solid rgba(255, 255, 255, 0.06)',
    fontSize: 10,
    color: '#52525b',
    fontFamily: "'JetBrains Mono', monospace",
  },

  // Metric row
  metric: {},
  metricHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: 4,
  },
  metricLabel: {
    fontSize: 11,
    color: '#71717a',
  },
  metricValue: {
    fontSize: 12,
    fontWeight: 700,
    fontFamily: "'JetBrains Mono', monospace",
  },
  barOuter: {
    height: 3,
    borderRadius: 2,
    background: 'rgba(255, 255, 255, 0.06)',
    overflow: 'hidden',
  },
  barInner: {
    height: '100%',
    borderRadius: 2,
    transition: 'width 300ms ease, background 300ms ease',
  },
};

export default TiltIndicator;

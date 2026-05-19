/**
 * [GEKTOR v15.0] LockoutOverlay — Full-Screen Circuit Breaker.
 *
 * When the Tilt-Breaker activates, this component renders an IMPENETRABLE
 * overlay that blocks ALL mouse events to execution controls.
 *
 * Visual Design:
 *   - Full viewport coverage with glassmorphism backdrop
 *   - Breathing pulse animation (red → dark red)
 *   - Countdown timer during COOLDOWN phase
 *   - Tilt score breakdown for operator self-awareness
 *
 * Security: The overlay uses pointer-events: all + z-index: 9999.
 * Even if the operator uses DevTools to remove the overlay,
 * the server-side CognitiveSentinel will reject any execution attempt
 * because is_execution_allowed() returns false.
 */

import React, { useEffect, useState, useRef } from 'react';
import { TiltSnapshot, TiltState } from '../../engine/TiltBreakerEngine';

interface LockoutOverlayProps {
  snapshot: TiltSnapshot;
}

export const LockoutOverlay: React.FC<LockoutOverlayProps> = ({ snapshot }) => {
  const [countdown, setCountdown] = useState<number>(0);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Countdown timer for COOLDOWN state
  useEffect(() => {
    if (
      (snapshot.state === TiltState.LOCKED || snapshot.state === TiltState.COOLDOWN) &&
      snapshot.lockedUntilEpoch > 0
    ) {
      const updateCountdown = () => {
        const remaining = Math.max(0, snapshot.lockedUntilEpoch - Date.now() / 1000);
        setCountdown(Math.ceil(remaining));
      };

      updateCountdown();
      intervalRef.current = setInterval(updateCountdown, 100);

      return () => {
        if (intervalRef.current) clearInterval(intervalRef.current);
      };
    } else {
      setCountdown(0);
    }
  }, [snapshot.state, snapshot.lockedUntilEpoch]);

  // Only render for LOCKED, COOLDOWN, CRITICAL, or BLIND states
  const isActive = [
    TiltState.LOCKED,
    TiltState.COOLDOWN,
    TiltState.CRITICAL,
    TiltState.BLIND,
  ].includes(snapshot.state);

  if (!isActive) return null;

  const isBlind = snapshot.state === TiltState.BLIND;

  return (
    <div style={styles.overlay} id="tilt-lockout-overlay">
      <div style={styles.container}>
        {/* Pulsing icon */}
        <div style={styles.iconContainer}>
          <div style={isBlind ? styles.blindIcon : styles.lockIcon}>
            {isBlind ? '👁️' : '🔒'}
          </div>
        </div>

        {/* Title */}
        <h1 style={styles.title}>
          {isBlind
            ? 'СОЕДИНЕНИЕ ПОТЕРЯНО'
            : 'КОГНИТИВНАЯ ИЗОЛЯЦИЯ'}
        </h1>

        {/* Subtitle */}
        <p style={styles.subtitle}>
          {isBlind
            ? 'WebSocket-канал молчит. Все исполнения заблокированы до восстановления связи.'
            : 'Система детектировала эмоциональную деградацию. Исполнение заблокировано.'}
        </p>

        {/* Score breakdown */}
        {!isBlind && (
          <div style={styles.metricsGrid}>
            <MetricCard
              label="Тилт-Скор"
              value={`${(snapshot.score * 100).toFixed(0)}%`}
              severity={snapshot.score >= 0.7 ? 'critical' : 'warning'}
            />
            <MetricCard
              label="Дрифт реакции"
              value={`${(snapshot.reactionDrift * 100).toFixed(0)}%`}
              severity={snapshot.reactionDrift > 1.5 ? 'critical' : 'normal'}
            />
            <MetricCard
              label="Серия ошибок"
              value={`${snapshot.errorStreak}`}
              severity={snapshot.errorStreak >= 4 ? 'critical' : 'normal'}
            />
            <MetricCard
              label="Спам-клики"
              value={snapshot.spamIntensity.toFixed(1)}
              severity={snapshot.spamIntensity > 3 ? 'critical' : 'normal'}
            />
          </div>
        )}

        {/* Countdown */}
        {countdown > 0 && (
          <div style={styles.countdownContainer}>
            <div style={styles.countdownLabel}>Разблокировка через</div>
            <div style={styles.countdownValue}>{countdown}с</div>
            <div style={styles.progressBarOuter}>
              <div
                style={{
                  ...styles.progressBarInner,
                  width: `${Math.max(0, (countdown / 60) * 100)}%`,
                }}
              />
            </div>
          </div>
        )}

        {/* Warning */}
        <p style={styles.warningText}>
          ⚠️ Любая попытка взаимодействия с терминалом перезапустит таймер.
        </p>
      </div>

      {/* Inject keyframes animation via style tag */}
      <style>{`
        @keyframes tilt-pulse {
          0%, 100% { opacity: 0.85; }
          50% { opacity: 0.95; }
        }
        @keyframes tilt-breathe {
          0%, 100% { transform: scale(1); }
          50% { transform: scale(1.05); }
        }
        @keyframes tilt-glow {
          0%, 100% { box-shadow: 0 0 20px rgba(220, 38, 38, 0.3); }
          50% { box-shadow: 0 0 60px rgba(220, 38, 38, 0.6); }
        }
      `}</style>
    </div>
  );
};

// ─── MetricCard Sub-Component ────────────────────────────────────

interface MetricCardProps {
  label: string;
  value: string;
  severity: 'normal' | 'warning' | 'critical';
}

const MetricCard: React.FC<MetricCardProps> = ({ label, value, severity }) => {
  const severityColor = {
    normal: '#94a3b8',
    warning: '#f59e0b',
    critical: '#ef4444',
  }[severity];

  return (
    <div style={styles.metricCard}>
      <div style={{ ...styles.metricValue, color: severityColor }}>{value}</div>
      <div style={styles.metricLabel}>{label}</div>
    </div>
  );
};

// ─── Styles (inline for zero-dependency) ─────────────────────────

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed',
    inset: 0,
    zIndex: 9999,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    background: 'rgba(7, 7, 15, 0.92)',
    backdropFilter: 'blur(12px)',
    WebkitBackdropFilter: 'blur(12px)',
    animation: 'tilt-pulse 3s ease-in-out infinite',
    cursor: 'not-allowed',
    userSelect: 'none',
  },
  container: {
    maxWidth: 520,
    padding: '48px 40px',
    borderRadius: 24,
    background: 'linear-gradient(135deg, rgba(30, 10, 10, 0.95), rgba(15, 5, 5, 0.98))',
    border: '1px solid rgba(220, 38, 38, 0.3)',
    textAlign: 'center' as const,
    animation: 'tilt-glow 3s ease-in-out infinite',
  },
  iconContainer: {
    marginBottom: 24,
  },
  lockIcon: {
    fontSize: 72,
    animation: 'tilt-breathe 2s ease-in-out infinite',
    display: 'inline-block',
  },
  blindIcon: {
    fontSize: 72,
    opacity: 0.5,
    animation: 'tilt-breathe 4s ease-in-out infinite',
    display: 'inline-block',
  },
  title: {
    margin: '0 0 12px 0',
    fontSize: 28,
    fontWeight: 800,
    color: '#ef4444',
    letterSpacing: '0.05em',
    textTransform: 'uppercase' as const,
    fontFamily: "'Inter', 'SF Pro Display', system-ui, sans-serif",
  },
  subtitle: {
    margin: '0 0 32px 0',
    fontSize: 15,
    color: '#a1a1aa',
    lineHeight: 1.6,
    fontFamily: "'Inter', system-ui, sans-serif",
  },
  metricsGrid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: 12,
    marginBottom: 32,
  },
  metricCard: {
    padding: '16px 12px',
    borderRadius: 12,
    background: 'rgba(255, 255, 255, 0.03)',
    border: '1px solid rgba(255, 255, 255, 0.06)',
  },
  metricValue: {
    fontSize: 24,
    fontWeight: 700,
    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
  },
  metricLabel: {
    fontSize: 11,
    color: '#71717a',
    marginTop: 4,
    textTransform: 'uppercase' as const,
    letterSpacing: '0.08em',
    fontFamily: "'Inter', system-ui, sans-serif",
  },
  countdownContainer: {
    marginBottom: 24,
  },
  countdownLabel: {
    fontSize: 13,
    color: '#71717a',
    marginBottom: 8,
    fontFamily: "'Inter', system-ui, sans-serif",
  },
  countdownValue: {
    fontSize: 56,
    fontWeight: 800,
    color: '#f59e0b',
    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
    lineHeight: 1,
    marginBottom: 16,
  },
  progressBarOuter: {
    height: 4,
    borderRadius: 2,
    background: 'rgba(255, 255, 255, 0.06)',
    overflow: 'hidden',
  },
  progressBarInner: {
    height: '100%',
    borderRadius: 2,
    background: 'linear-gradient(90deg, #ef4444, #f59e0b)',
    transition: 'width 100ms linear',
  },
  warningText: {
    fontSize: 12,
    color: '#52525b',
    margin: 0,
    fontFamily: "'Inter', system-ui, sans-serif",
  },
};

export default LockoutOverlay;

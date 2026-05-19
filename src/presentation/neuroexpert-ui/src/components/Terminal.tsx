import React, { useEffect, useState, useRef } from 'react';
import { SignalArmedEvent, CapsuleStatus, OperatorResponse } from './types';
import { deserializeSignal, serializeResponse } from './transport';

const WS_URL = process.env.REACT_APP_GEKTOR_WS_URL || 'ws://localhost:8000/v5/radar';
const CAPSULE_TTL_MS = 5000;

export const GektorTerminal: React.FC = () => {
  const [activeSignal, setActiveSignal] = useState<SignalArmedEvent | null>(null);
  const [lastVerdict, setLastVerdict] = useState<string>('IDLE');
  
  const wsRef = useRef<WebSocket | null>(null);
  const activeSignalRef = useRef<SignalArmedEvent | null>(null);
  
  const timerBarRef = useRef<HTMLDivElement | null>(null);
  const timerTextRef = useRef<HTMLSpanElement | null>(null);
  const animationFrameRef = useRef<number | null>(null);

  useEffect(() => {
    activeSignalRef.current = activeSignal;
  }, [activeSignal]);

  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    ws.binaryType = 'arraybuffer';
    wsRef.current = ws;

    ws.onopen = () => console.log('🟢 [WS] Connected to GEKTOR Core.');
    ws.onmessage = (event: MessageEvent) => {
      try {
        if (event.data instanceof ArrayBuffer) {
          const signal = deserializeSignal(event.data);
          setActiveSignal(signal);
          setLastVerdict('ARMED');
          startCountdown(signal.created_at);
        }
      } catch (err) {
        console.error('💥 [WS] Message parsing failure:', err);
      }
    };
    ws.onclose = () => console.warn('🔴 [WS] Disconnected from GEKTOR Core.');
    ws.onerror = (err) => console.error('🔌 [WS] Socket transport error:', err);

    return () => {
      ws.close();
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, []);

  const startCountdown = (createdAt: number) => {
    if (animationFrameRef.current) {
      cancelAnimationFrame(animationFrameRef.current);
    }

    const tick = () => {
      const elapsed = Date.now() - createdAt;
      const remaining = CAPSULE_TTL_MS - elapsed;

      if (remaining <= 0) {
        handleDecision('EXPIRED');
      } else {
        const progressPct = Math.max(0, (remaining / CAPSULE_TTL_MS) * 100);
        if (timerBarRef.current) {
          timerBarRef.current.style.width = `${progressPct}%`;
          timerBarRef.current.style.backgroundColor = progressPct < 30 ? '#ef4444' : '#f59e0b';
        }
        if (timerTextRef.current) {
          timerTextRef.current.innerText = `${(remaining / 1000).toFixed(3)}s`;
        }
        animationFrameRef.current = requestAnimationFrame(tick);
      }
    };

    animationFrameRef.current = requestAnimationFrame(tick);
  };

  const handleDecision = (status: CapsuleStatus) => {
    const signal = activeSignalRef.current;
    if (!signal) return;

    if (animationFrameRef.current) {
      cancelAnimationFrame(animationFrameRef.current);
    }

    const response: OperatorResponse = {
      signal_id: signal.capsule.signal_id,
      status: status,
      clicked_at: Date.now(),
    };

    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      const payload = serializeResponse(response);
      wsRef.current.send(payload);
      console.log(`📤 [OPERATOR_GATE] Transmitted ${status} for ${signal.symbol}`);
    }

    setActiveSignal(null);
    setLastVerdict(status);
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (!activeSignalRef.current) return;
      
      if (e.key === 'Enter') {
        e.preventDefault();
        handleDecision('ACK');
      } else if (e.key === ' ') {
        e.preventDefault();
        handleDecision('NACK');
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>GEKTOR // NEUROEXPERT TERMINAL</h1>
      
      {activeSignal ? (
        <div style={styles.card}>
          <div style={styles.header}>
            <span style={styles.symbol}>{activeSignal.symbol}</span>
            <span style={{
              ...styles.side,
              color: activeSignal.side === 'BUY' ? '#10b981' : '#ef4444'
            }}>
              {activeSignal.side}
            </span>
          </div>

          <div style={styles.grid}>
            <div style={styles.metric}>
              <span style={styles.label}>VPIN Toxicity</span>
              <span style={styles.value}>{activeSignal.vpin_toxicity.toFixed(4)}</span>
            </div>
            <div style={styles.metric}>
              <span style={styles.label}>MSQ Quantity</span>
              <span style={styles.value}>{activeSignal.msq_qty.toFixed(4)}</span>
            </div>
            <div style={styles.metric}>
              <span style={styles.label}>Execution Price</span>
              <span style={styles.value}>${activeSignal.safe_price.toFixed(2)}</span>
            </div>
          </div>

          <div style={styles.timerContainer}>
            <div style={styles.timerTrack}>
              <div ref={timerBarRef} style={styles.timerBar} />
            </div>
            <span ref={timerTextRef} style={styles.timerText}>5.000s</span>
          </div>

          <div style={styles.btnGroup}>
            <button 
              onClick={() => handleDecision('ACK')} 
              style={{ ...styles.btn, ...styles.btnAck }}
            >
              [ENTER] ИСПОЛНИТЬ (ACK)
            </button>
            <button 
              onClick={() => handleDecision('NACK')} 
              style={{ ...styles.btn, ...styles.btnNack }}
            >
              [SPACE] ОТКЛОНИТЬ (NACK)
            </button>
          </div>
        </div>
      ) : (
        <div style={styles.standby}>
          <div style={styles.pulse} />
          <span>RADAR ACTIVE. WAITING FOR INTENT CAPSULE...</span>
          <div style={styles.verdictText}>LAST STATUS: {lastVerdict}</div>
        </div>
      )}
    </div>
  );
};

const styles: Record<string, React.CSSProperties> = {
  container: {
    backgroundColor: '#0a0a0c',
    color: '#d1d5db',
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    fontFamily: 'monospace',
    padding: '20px',
  },
  title: {
    fontSize: '1.2rem',
    color: '#6b7280',
    letterSpacing: '2px',
    marginBottom: '40px',
  },
  card: {
    backgroundColor: '#111115',
    border: '1px solid #1f2937',
    borderRadius: '4px',
    padding: '30px',
    width: '100%',
    maxWidth: '550px',
    boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.5)',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '25px',
    borderBottom: '1px solid #1f2937',
    paddingBottom: '15px',
  },
  symbol: {
    fontSize: '2rem',
    fontWeight: 'bold',
  },
  side: {
    fontSize: '1.5rem',
    fontWeight: 'bold',
    letterSpacing: '1px',
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(3, 1fr)',
    gap: '20px',
    marginBottom: '30px',
  },
  metric: {
    display: 'flex',
    flexDirection: 'column',
  },
  label: {
    fontSize: '0.75rem',
    color: '#9ca3af',
    marginBottom: '5px',
  },
  value: {
    fontSize: '1.1rem',
    fontWeight: 'bold',
    color: '#f3f4f6',
  },
  timerContainer: {
    display: 'flex',
    alignItems: 'center',
    gap: '15px',
    marginBottom: '30px',
  },
  timerTrack: {
    flex: 1,
    height: '6px',
    backgroundColor: '#1f2937',
    borderRadius: '3px',
    overflow: 'hidden',
  },
  timerBar: {
    width: '100%',
    height: '100%',
    backgroundColor: '#f59e0b',
    transition: 'none',
  },
  timerText: {
    fontSize: '0.9rem',
    fontWeight: 'bold',
    width: '60px',
    textAlign: 'right',
  },
  btnGroup: {
    display: 'flex',
    gap: '15px',
  },
  btn: {
    flex: 1,
    padding: '15px',
    border: 'none',
    borderRadius: '2px',
    fontFamily: 'monospace',
    fontWeight: 'bold',
    fontSize: '0.9rem',
    cursor: 'pointer',
    transition: 'background-color 0.1s ease',
  },
  btnAck: {
    backgroundColor: '#047857',
    color: '#ecfdf5',
  },
  btnNack: {
    backgroundColor: '#b91c1c',
    color: '#fef2f2',
  },
  standby: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    gap: '15px',
    color: '#4b5563',
  },
  pulse: {
    width: '8px',
    height: '8px',
    backgroundColor: '#10b981',
    borderRadius: '50%',
  },
  verdictText: {
    marginTop: '20px',
    fontSize: '0.8rem',
  }
};

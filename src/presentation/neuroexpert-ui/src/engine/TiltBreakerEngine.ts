/**
 * [GEKTOR v15.0] TiltBreakerEngine — Client-Side Cognitive FSM.
 *
 * This is the LOCAL enforcement layer. It runs independently of the server
 * to provide instant feedback (no round-trip latency for UI lockout).
 *
 * The server is the Source of Truth. This engine SHADOWS the server state
 * and can ONLY escalate (lock), never de-escalate (unlock) without server approval.
 *
 * Design: O(1) amortized per operation. Zero allocations in hot path.
 * Ring buffers pre-allocated. No closures in tight loops.
 */

// ─── Types ───────────────────────────────────────────────────────

export enum TiltState {
  CLEAR = 'CLEAR',
  ELEVATED = 'ELEVATED',
  CRITICAL = 'CRITICAL',
  LOCKED = 'LOCKED',
  COOLDOWN = 'COOLDOWN',
  BLIND = 'BLIND',
}

export interface TiltSnapshot {
  state: TiltState;
  score: number;           // [0, 1]
  reactionDrift: number;   // [0, ∞)
  errorStreak: number;     // [0, ∞)
  spamIntensity: number;   // [0, ∞)
  generation: number;
  lockedUntilEpoch: number; // 0 if not locked
  isExecutionAllowed: boolean;
}

export interface HeartbeatPayload {
  type: 'heartbeat';
  reaction_ms: number;
  clicks: number;
  errors: number;
  successes: number;
}

export type TiltStateChangeCallback = (snapshot: TiltSnapshot) => void;

// ─── Ring Buffer (pre-allocated, O(1)) ───────────────────────────

class FloatRingBuffer {
  private buffer: Float64Array;
  private head: number = 0;
  private count: number = 0;

  constructor(private capacity: number) {
    this.buffer = new Float64Array(capacity);
  }

  push(value: number): void {
    this.buffer[this.head] = value;
    this.head = (this.head + 1) % this.capacity;
    if (this.count < this.capacity) this.count++;
  }

  getLast(): number {
    if (this.count === 0) return 0;
    return this.buffer[(this.head - 1 + this.capacity) % this.capacity];
  }

  getCount(): number {
    return this.count;
  }
}

// ─── Click Tracker (client-side spam detection) ──────────────────

class ClickTracker {
  private timestamps: number[] = [];
  private readonly MAX_TRACKED = 50;

  recordClick(): void {
    const now = performance.now();
    this.timestamps.push(now);
    if (this.timestamps.length > this.MAX_TRACKED) {
      this.timestamps.shift();
    }
  }

  /**
   * Count clicks within the last `windowMs` milliseconds.
   */
  countInWindow(windowMs: number): number {
    const now = performance.now();
    const cutoff = now - windowMs;
    let count = 0;
    for (let i = this.timestamps.length - 1; i >= 0; i--) {
      if (this.timestamps[i] >= cutoff) count++;
      else break;
    }
    return count;
  }

  /**
   * Detect panic burst: >3 clicks within 500ms.
   */
  hasPanicBurst(): boolean {
    return this.countInWindow(500) > 3;
  }

  getClicksSinceMs(sinceMs: number): number {
    return this.countInWindow(sinceMs);
  }

  reset(): void {
    this.timestamps.length = 0;
  }
}

// ─── Main Engine ─────────────────────────────────────────────────

export class TiltBreakerEngine {
  // State
  private state: TiltState = TiltState.CLEAR;
  private generation: number = 0;
  private lockedUntilEpoch: number = 0;
  private score: number = 0;

  // Detectors
  private reactionBuffer: FloatRingBuffer;
  private reactionEwma: number = 0;
  private readonly reactionAlpha: number = 0.15;
  private clickTracker: ClickTracker;
  private errorStreak: number = 0;

  // Heartbeat tracking
  private lastSignalShownMs: number = 0;
  private lastHeartbeatMs: number = 0;
  private clicksSinceLastHeartbeat: number = 0;
  private errorsSinceLastHeartbeat: number = 0;
  private successesSinceLastHeartbeat: number = 0;

  // Callbacks
  private onStateChange: TiltStateChangeCallback | null = null;

  // WS reference
  private ws: WebSocket | null = null;
  private heartbeatIntervalId: ReturnType<typeof setInterval> | null = null;
  private blindCheckIntervalId: ReturnType<typeof setInterval> | null = null;

  // Constants
  private readonly HEARTBEAT_INTERVAL_MS = 1000;
  private readonly BLIND_THRESHOLD_MS = 3000; // 3 missed heartbeats
  private readonly MIN_REACTION_SAMPLES = 5;

  constructor() {
    this.reactionBuffer = new FloatRingBuffer(20);
    this.clickTracker = new ClickTracker();
    this.lastHeartbeatMs = performance.now();
  }

  // ─── Public API ──────────────────────────────────────────────

  /**
   * Connect to the WebSocket signal stream and start monitoring.
   */
  bind(ws: WebSocket, onChange: TiltStateChangeCallback): void {
    this.ws = ws;
    this.onStateChange = onChange;

    // Listen for server-side tilt state updates
    ws.addEventListener('message', this.handleServerMessage);

    // Start heartbeat loop
    this.heartbeatIntervalId = setInterval(
      () => this.sendHeartbeat(),
      this.HEARTBEAT_INTERVAL_MS,
    );

    // Start blind detection (client-side)
    this.blindCheckIntervalId = setInterval(
      () => this.checkBlind(),
      1000,
    );
  }

  /**
   * Cleanup on unmount or WS close.
   */
  destroy(): void {
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = null;
    }
    if (this.blindCheckIntervalId) {
      clearInterval(this.blindCheckIntervalId);
      this.blindCheckIntervalId = null;
    }
    if (this.ws) {
      this.ws.removeEventListener('message', this.handleServerMessage);
      this.ws = null;
    }
  }

  /**
   * Called when an APPROVED_EXECUTION signal is displayed to the operator.
   * Starts the reaction time clock.
   */
  onSignalDisplayed(): void {
    this.lastSignalShownMs = performance.now();
  }

  /**
   * Called when the operator clicks an execution button.
   * Records reaction time and click event.
   *
   * Returns false if execution is BLOCKED (tilt lock active).
   */
  onExecutionClick(): boolean {
    // Record click for spam detection
    this.clickTracker.recordClick();
    this.clicksSinceLastHeartbeat++;

    // Measure reaction time if a signal was displayed
    if (this.lastSignalShownMs > 0) {
      const reactionMs = performance.now() - this.lastSignalShownMs;
      this.reactionBuffer.push(reactionMs);

      // EWMA update
      if (this.reactionEwma === 0) {
        this.reactionEwma = reactionMs;
      } else {
        this.reactionEwma =
          this.reactionAlpha * reactionMs +
          (1 - this.reactionAlpha) * this.reactionEwma;
      }

      this.lastSignalShownMs = 0; // Reset — one measurement per signal
    }

    // Local tilt check (immediate feedback, no round-trip)
    this.evaluateLocal();

    // Gate check
    return this.isExecutionAllowed();
  }

  /**
   * Called when a trade result is known (P&L resolved).
   */
  onTradeResult(isProfitable: boolean): void {
    if (isProfitable) {
      this.errorStreak = Math.max(0, this.errorStreak - 2);
      this.successesSinceLastHeartbeat++;
    } else {
      this.errorStreak++;
      this.errorsSinceLastHeartbeat++;
    }

    this.evaluateLocal();
  }

  /**
   * O(1) gate check.
   */
  isExecutionAllowed(): boolean {
    return this.state === TiltState.CLEAR || this.state === TiltState.ELEVATED;
  }

  /**
   * Get current snapshot for UI rendering.
   */
  getSnapshot(): TiltSnapshot {
    return {
      state: this.state,
      score: this.score,
      reactionDrift: this.getReactionDrift(),
      errorStreak: this.errorStreak,
      spamIntensity: this.getSpamIntensity(),
      generation: this.generation,
      lockedUntilEpoch: this.lockedUntilEpoch,
      isExecutionAllowed: this.isExecutionAllowed(),
    };
  }

  // ─── Private: Server Message Handler ─────────────────────────

  private handleServerMessage = (event: MessageEvent): void => {
    try {
      const data = JSON.parse(event.data);

      if (data.type === 'tilt_state') {
        this.syncFromServer(data);
      } else if (data.type === 'tilt_lock') {
        this.handleLockFromServer(data);
      }
    } catch {
      // Non-JSON or non-tilt message — ignore
    }
  };

  private syncFromServer(data: Record<string, unknown>): void {
    const serverGeneration = data.generation as number;

    // Server is Source of Truth — always accept if generation is newer
    if (serverGeneration > this.generation) {
      const serverState = data.state as string;
      const newState = (TiltState as Record<string, string>)[serverState] as TiltState | undefined;

      if (newState !== undefined) {
        const oldState = this.state;
        this.state = newState;
        this.generation = serverGeneration;
        this.score = data.score as number;
        this.lockedUntilEpoch = (data.locked_until_epoch as number) || 0;

        if (oldState !== newState) {
          this.notifyStateChange();
        }
      }
    }
  }

  private handleLockFromServer(data: Record<string, unknown>): void {
    this.state = TiltState.LOCKED;
    this.lockedUntilEpoch = (data.locked_until_epoch as number) || Date.now() / 1000 + 60;
    this.score = (data.score as number) || 1.0;
    this.generation++;
    this.notifyStateChange();
  }

  // ─── Private: Local Evaluation ───────────────────────────────

  private evaluateLocal(): void {
    const reactionDrift = this.getReactionDrift();
    const spamIntensity = this.getSpamIntensity();

    // Weighted composite (mirrors server formula)
    const composite =
      0.40 * Math.min(reactionDrift / 2.0, 1.0) +
      0.35 * Math.min(this.errorStreak / 4.0, 1.0) +
      0.25 * Math.min(spamIntensity / 5.0, 1.0);

    this.score = Math.min(1.0, Math.max(0.0, composite));

    // Local escalation only (never de-escalate without server)
    const oldState = this.state;

    if (this.state === TiltState.CLEAR || this.state === TiltState.ELEVATED) {
      if (this.score >= 0.7) {
        this.state = TiltState.LOCKED;
        this.lockedUntilEpoch = Date.now() / 1000 + 60;
      } else if (this.score >= 0.4) {
        this.state = TiltState.ELEVATED;
      }
      // Note: NO de-escalation from ELEVATED → CLEAR locally
      // That requires server confirmation
    }

    if (oldState !== this.state) {
      this.generation++;
      this.notifyStateChange();
    }
  }

  private getReactionDrift(): number {
    if (this.reactionBuffer.getCount() < this.MIN_REACTION_SAMPLES) return 0;
    const current = this.reactionBuffer.getLast();
    if (this.reactionEwma <= 0) return 0;
    return Math.abs(current - this.reactionEwma) / this.reactionEwma;
  }

  private getSpamIntensity(): number {
    const burstFactor = this.clickTracker.hasPanicBurst() ? 3.0 : 0.0;
    const clicksIn10s = this.clickTracker.getClicksSinceMs(10_000);
    const sustainedFactor = Math.max(0, clicksIn10s - 5) / 5.0;
    return burstFactor + sustainedFactor;
  }

  // ─── Private: Heartbeat ──────────────────────────────────────

  private sendHeartbeat(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const payload: HeartbeatPayload = {
      type: 'heartbeat',
      reaction_ms: this.reactionBuffer.getCount() > 0 ? this.reactionBuffer.getLast() : 0,
      clicks: this.clicksSinceLastHeartbeat,
      errors: this.errorsSinceLastHeartbeat,
      successes: this.successesSinceLastHeartbeat,
    };

    try {
      this.ws.send(JSON.stringify(payload));
    } catch {
      // WS send failed — blind check will catch this
    }

    // Reset deltas
    this.clicksSinceLastHeartbeat = 0;
    this.errorsSinceLastHeartbeat = 0;
    this.successesSinceLastHeartbeat = 0;
    this.lastHeartbeatMs = performance.now();
  }

  private checkBlind(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      if (this.state !== TiltState.BLIND) {
        this.state = TiltState.BLIND;
        this.generation++;
        this.notifyStateChange();
      }
    }
  }

  // ─── Private: Notification ───────────────────────────────────

  private notifyStateChange(): void {
    if (this.onStateChange) {
      this.onStateChange(this.getSnapshot());
    }
  }
}

/**
 * [GEKTOR v15.0] TiltBreaker Barrel Export.
 *
 * Usage in SignalBoard or App component:
 *
 *   import { useTiltBreaker, LockoutOverlay, TiltIndicator } from './TiltBreaker';
 *
 *   function SignalBoard({ ws }) {
 *     const { snapshot, onSignalDisplayed, onExecutionClick, onTradeResult } = useTiltBreaker(ws);
 *
 *     const handleExecute = () => {
 *       if (!onExecutionClick()) {
 *         // Execution BLOCKED by Tilt-Breaker
 *         return;
 *       }
 *       // ... proceed with execution
 *     };
 *
 *     return (
 *       <>
 *         <LockoutOverlay snapshot={snapshot} />
 *         <TiltIndicator snapshot={snapshot} />
 *         {/* ... rest of UI */}
 *       </>
 *     );
 *   }
 */

export { LockoutOverlay } from './LockoutOverlay';
export { TiltIndicator } from './TiltIndicator';
export { useTiltBreaker } from '../../hooks/useTiltBreaker';
export type { TiltSnapshot, TiltState } from '../../engine/TiltBreakerEngine';

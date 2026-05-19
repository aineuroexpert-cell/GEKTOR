import { Decimal } from 'decimal.js';

export interface IntentCapsule {
  signal_id: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  tilt_generation: number;
  nonce: string;
  issued_at_mono: number;
  signature: string;
}

export interface SignalArmedEvent {
  signal_id: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  vpin_toxicity: Decimal;
  msq_qty: Decimal;
  safe_price: Decimal;
  capsule: IntentCapsule;
  created_at: number;
}

export type CapsuleStatus = 'ACK' | 'NACK' | 'EXPIRED';

export interface OperatorResponse {
  signal_id: string;
  status: CapsuleStatus;
  clicked_at: number;
}

import { encode, decode } from '@msgpack/msgpack';
import { Decimal } from 'decimal.js';
import { SignalArmedEvent, OperatorResponse } from './types';

Decimal.set({ precision: 20, rounding: Decimal.ROUND_HALF_UP });

export function deserializeSignal(buffer: ArrayBuffer): SignalArmedEvent {
  const raw = decode(buffer) as any;

  if (!raw.signal_id || !raw.capsule) {
    throw new Error('[PARSER] Invalid frame signature received.');
  }

  return {
    signal_id: String(raw.signal_id),
    symbol: String(raw.symbol),
    side: raw.side === 'BUY' ? 'BUY' : 'SELL',
    vpin_toxicity: new Decimal(String(raw.vpin_toxicity)),
    msq_qty: new Decimal(String(raw.msq_qty)),
    safe_price: new Decimal(String(raw.safe_price)),
    created_at: Number(raw.created_at),
    capsule: {
      signal_id: String(raw.capsule.signal_id),
      symbol: String(raw.capsule.symbol),
      side: raw.capsule.side === 'BUY' ? 'BUY' : 'SELL',
      tilt_generation: Number(raw.capsule.tilt_generation),
      nonce: String(raw.capsule.nonce),
      issued_at_mono: Number(raw.capsule.issued_at_mono),
      signature: String(raw.capsule.signature),
    }
  };
}

export function serializeResponse(response: OperatorResponse): Uint8Array {
  return encode({
    signal_id: response.signal_id,
    status: response.status,
    clicked_at: response.clicked_at,
  });
}

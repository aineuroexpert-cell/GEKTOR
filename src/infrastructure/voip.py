# src/infrastructure/voip.py
import asyncio
import aiohttp
from loguru import logger
from typing import List
from enum import Enum

class CallStatus(Enum):
    QUEUED = "queued"
    RINGING = "ringing"
    ANSWERED = "in-progress"
    FAILED = "failed"
    NO_ANSWER = "no-answer"

class EscalationMatrix:
    def __init__(self, target_phones: List[str]):
        self.targets = target_phones
        self.current_index = 0

    def get_next_target(self) -> str:
        if not self.targets: return ""
        target = self.targets[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.targets)
        return target

class VoIPGuardianProtocol:
    """
    [GEKTOR APEX] GUARDIAN PROTOCOL (Paging).
    Ensures the operator is woken up for critical ABORT MISSION alerts.
    Asynchronous REST calls to Twilio to prevent Event Loop Starvation.
    """
    def __init__(self, account_sid: str, auth_token: str, from_number: str):
        self.url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls.json"
        self.auth = aiohttp.BasicAuth(account_sid, auth_token)
        self.from_number = from_number
        self._session: aiohttp.ClientSession | None = None

    async def initialize(self):
        self._session = aiohttp.ClientSession(auth=self.auth)
        logger.info("📞 [Guardian] VoIP Session established. Paging ARMED.")

    async def trigger_alarm(self, escalation: EscalationMatrix, message_id: str):
        if not self._session:
             logger.error("❌ [Guardian] VoIP session not initialized.")
             return

        target_number = escalation.get_next_target()
        if not target_number: return

        twiml = (
            "<Response>"
            "<Pause length='1'/>"
            "<Say voice='alice' language='en-US' loop='3'>"
            "Critical Alert. Gektor Apex. Abort Mission. Premise invalidated. Wake up. Check Telegram."
            "</Say>"
            "</Response>"
        )
        
        data = {
            "To": target_number,
            "From": self.from_number,
            "Twiml": twiml,
            "Timeout": "20"
        }

        try:
            async with self._session.post(self.url, data=data, timeout=5.0) as resp:
                if resp.status in (200, 201):
                    logger.critical(f"📞 [GUARDIAN] Escalated Paging to {target_number} (Alert {message_id})")
                else:
                    err = await resp.text()
                    logger.error(f"🚨 [Guardian] Twilio Error: {err}")
        except Exception as e:
            logger.error(f"🚨 [Guardian] VoIP Paging failed: {e}")

    async def close(self):
        if self._session:
            await self._session.close()

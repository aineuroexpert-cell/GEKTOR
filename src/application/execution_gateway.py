"""
File deprecated.
Autonomous execution is strictly prohibited by GEKTOR doctrine.
"""

class ExecutionGateway:
    def __init__(self, *args, **kwargs):
        pass

    async def execute_liquidation(self, *args, **kwargs):
        raise RuntimeError("Autonomous execution is strictly prohibited by GEKTOR doctrine.")

from dataclasses import dataclass

@dataclass
class SirensModule:
    ETL: str = "input"
    THREAT_HUNT: str = "threat_hunting"
    THREAT_INTEL: str = "threat_intel"
    RISK: str = "risk"

def _get_modules():
    return SirensModule()

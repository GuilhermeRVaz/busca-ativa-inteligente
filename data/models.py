from dataclasses import dataclass, field
from typing import Any


@dataclass
class MessageRecord:
    conversation_id: str
    direction: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CampaignEvent:
    campaign_id: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

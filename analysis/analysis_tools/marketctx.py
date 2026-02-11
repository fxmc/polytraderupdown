import datetime as dt

from dataclasses import dataclass, field


@dataclass
class MarketContext:
    clob_token_ids: list[str]
    end_date: dt.datetime
    event_start_time: dt. datetime
    market_id: int
    order_min_size: float
    order_price_min_tick_size: float
    outcome_prices: list[float]
    outcomes: list[str]
    question: str
    condition_id: str
    slug: str
    uma_resolution_status: str
    uma_resolution_statuses: list[str]
    coin: str | None = None
    outcome_name: dict[str, str] = field(init=False)

    def __post_init__(self):
        self.outcome_name = {
            self.clob_token_ids[0]: "UP",
            self.clob_token_ids[1]: "DOWN"
        }

    def get_token_name(self, token_id: str | int) -> str:
        if isinstance(token_id, int):
            token_id = str(token_id)

        return self.outcome_name[token_id]

"""Pipeline parameters. These are the rules the data platform applies, not the simulator's fault settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    # Reports use local plant time; storage stays in UTC.
    timezone: str = "Europe/Berlin"
    # A meter silent for longer than this is a gap (dropout or outage).
    gap_threshold_s: int = 300
    # An event that reaches the platform later than this after its measurement counts as late.
    late_threshold_s: int = 300
    # Plausibility: a reading above max_power_kw is rejected, and so is a single reading spike_ratio times above both
    # neighbours (and above spike_min_kw, so small idle fluctuations don't count).
    max_power_kw: float = 20.0
    spike_ratio: float = 3.0
    spike_min_kw: float = 2.0
    # Heat-up overrun rule: longer than baseline * factor (the 2019 alert).
    heatup_baseline_h: float = 2.0
    heatup_overrun_factor: float = 1.3
    # Time-of-use tariff (assumed prices).
    currency: str = "EUR"
    peak_price_per_kwh: float = 0.24
    off_peak_price_per_kwh: float = 0.17
    peak_weekdays: tuple[int, ...] = (1, 2, 3, 4, 5)  # ISO: 1 = Monday
    peak_from_hour: int = 7
    peak_to_hour: int = 20

    @property
    def heatup_threshold_h(self) -> float:
        return self.heatup_baseline_h * self.heatup_overrun_factor

    @classmethod
    def from_assumptions(cls, path: str | Path, **overrides) -> PipelineConfig:
        """Takes the tariff from config/assumptions.yaml so local runs and the simulator agree."""
        import yaml

        t = yaml.safe_load(Path(path).read_text())["tariff"]
        return cls(
            currency=t["currency"],
            peak_price_per_kwh=t["peak"]["price_per_kwh"],
            off_peak_price_per_kwh=t["off_peak"]["price_per_kwh"],
            peak_weekdays=tuple(t["peak"]["weekdays"]),
            peak_from_hour=t["peak"]["from_hour"],
            peak_to_hour=t["peak"]["to_hour"],
            **overrides,
        )

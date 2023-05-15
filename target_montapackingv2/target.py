"""Montapackingv2 target class."""
from target_montapackingv2.sinks import (
    InboundForecastSink,
    UpdateInventory
)

from target_hotglue.target import TargetHotglue

class TargetMontapacking(TargetHotglue):
    SINK_TYPES = [InboundForecastSink, UpdateInventory]
    MAX_PARALLELISM = 10
    name = "target-montapackingv2"


if __name__ == "__main__":
    TargetMontapacking.cli()


"""Pure source resolvers for the static bond issuer FF17 attribute.

The canonical SIC definition is Kenneth French's official ``Siccodes17.txt``.
Ranges are encoded here (not inferred from names or six-character CUSIPs), so
an absent SIC remains an honest unresolved sector rather than FF17 Other.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import re
from typing import Iterable

from src.bonds.errors import BondError

FF17_SOURCE_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes17.zip"
FF17_SOURCE_VERSION = "Siccodes17.txt downloaded 2026-08-08"

_CUSIP9_RE = re.compile(r"^[0-9A-Z]{9}$")

# Official Siccodes17.txt ranges, compacted only where adjacent official ranges
# have the same FF17 bucket.  Tuple order preserves the official bucket order.
_OFFICIAL_RANGES = {
    1: "100-299,700-799,900-999,2000-2048,2050-2068,2070-2080,2082-2087,2090-2092,2095-2099,5140-5159,5180-5182,5191-5191",
    2: "1000-1049,1060-1069,1080-1099,1200-1299,1400-1499,5050-5052",
    3: "1300-1300,1310-1329,1380-1382,1389-1389,2900-2912,5170-5172",
    4: "2200-2284,2290-2399,3020-3021,3100-3111,3130-3131,3140-3151,3963-3965,5130-5139",
    5: "2510-2519,2590-2599,3060-3099,3630-3639,3650-3652,3860-3861,3870-3873,3910-3911,3914-3915,3930-3931,3940-3949,3960-3962,5020-5023,5064-5064,5094-5094,5099-5099",
    6: "2800-2829,2860-2879,2890-2899,5160-5169",
    7: "2100-2199,2830-2831,2833-2834,2840-2844,5120-5122,5194-5194",
    8: "800-899,1500-1511,1520-1549,1600-1799,2400-2459,2490-2499,2850-2859,2950-2952,3200-3200,3210-3211,3240-3241,3250-3259,3261-3261,3264-3264,3270-3275,3280-3281,3290-3293,3420-3433,3440-3442,3446-3446,3448-3452,5030-5039,5070-5078,5198-5198,5210-5211,5230-5231,5250-5251",
    9: "3300-3300,3310-3317,3320-3325,3330-3341,3350-3357,3360-3369,3390-3399",
    10: "3410-3412,3443-3444,3460-3499",
    11: "3510-3536,3540-3582,3585-3586,3589-3600,3610-3613,3620-3629,3670-3695,3699-3699,3810-3812,3820-3827,3829-3839,3950-3955,5060-5060,5063-5063,5065-5065,5080-5081",
    12: "3710-3711,3714-3714,3716-3716,3750-3751,3792-3792,5010-5015,5510-5521,5530-5531,5560-5561,5570-5571,5590-5599",
    13: "3713-3713,3715-3715,3720-3721,3724-3725,3728-3728,3730-3732,3740-3743,3760-3769,3790-3790,3795-3795,3799-3799,4000-4013,4100-4100,4110-4121,4130-4131,4140-4142,4150-4151,4170-4173,4190-4200,4210-4231,4400-4700,4710-4712,4720-4742,4780-4780,4783-4783,4785-4785,4789-4789",
    14: "4900-4900,4910-4911,4920-4925,4930-4932,4939-4942",
    15: "5260-5261,5270-5271,5300-5300,5310-5311,5320-5320,5330-5331,5334-5334,5390-5400,5410-5412,5420-5421,5430-5431,5440-5441,5450-5451,5460-5461,5490-5499,5540-5541,5550-5551,5600-5700,5710-5722,5730-5736,5750-5750,5800-5813,5890-5890,5900-5900,5910-5912,5920-5921,5930-5932,5940-5949,5960-5963,5980-5990,5992-5995,5999-5999",
    16: "6010-6023,6025-6026,6028-6036,6040-6062,6080-6082,6090-6100,6110-6112,6120-6129,6140-6163,6172-6172,6199-6300,6310-6312,6320-6324,6330-6331,6350-6351,6360-6361,6370-6371,6390-6411,6500-6500,6510-6510,6512-6515,6517-6519,6530-6532,6540-6541,6550-6553,6611-6611,6700-6700,6710-6726,6730-6733,6790-6790,6792-6792,6794-6795,6798-6799",
    17: "2520-2549,2600-2659,2661-2661,2670-2761,2770-2771,2780-2799,2835-2836,2990-3000,3010-3011,3041-3041,3050-3053,3160-3161,3170-3172,3190-3199,3220-3221,3229-3231,3260-3260,3262-3263,3269-3269,3295-3299,3537-3537,3640-3649,3660-3666,3669-3669,3840-3851,3991-3991,3993-3993,3995-3996,4810-4813,4820-4822,4830-4841,4890-4892,4899-4899,4950-4961,4970-4971,4991-4991,5040-5049,5082-5088,5090-5093,5100-5100,5110-5113,5199-5199,7000-7000,7010-7011,7020-7021,7030-7033,7040-7041,7200-7200,7210-7213,7215-7221,7230-7231,7240-7241,7250-7251,7260-7269,7290-7291,7299-7300,7310-7323,7330-7338,7340-7342,7349-7353,7359-7385,7389-7395,7397-7397,7399-7399,7500-7500,7510-7523,7530-7549,7600-7600,7620-7620,7622-7623,7629-7631,7640-7641,7690-7699,7800-7833,7840-7841,7900-7900,7910-7911,7920-7933,7940-7949,7980-7980,7990-8499,8600-8700,8710-8713,8720-8721,8730-8734,8740-8748,8800-8911,8920-8999",
}


def _ranges() -> tuple[tuple[int, int, int], ...]:
    result: list[tuple[int, int, int]] = []
    for ff17num, value in _OFFICIAL_RANGES.items():
        for fragment in value.split(","):
            start, end = fragment.split("-", 1)
            result.append((int(start), int(end), ff17num))
    return tuple(result)


FF17_RANGES = _ranges()


@dataclass(frozen=True)
class SectorResolution:
    ff17num: int | None
    reason: str | None
    disagreement_count: int = 0


def normalize_cusip9(value: object) -> str:
    text = str(value).strip().upper() if value is not None else ""
    if not _CUSIP9_RE.fullmatch(text):
        raise BondError("invalid_cusip9", {"cusip9": text or None})
    return text


def _valid_ff17(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if 1 <= number <= 17 else None


def resolve_modal_ff17(values: Iterable[object]) -> SectorResolution:
    """Resolve panel history by modal FF17; ties intentionally select lowest ID."""
    valid = [value for raw in values if (value := _valid_ff17(raw)) is not None]
    if not valid:
        return SectorResolution(None, "no_valid_ff17num")
    counts = Counter(valid)
    modal_count = max(counts.values())
    ff17num = min(value for value, count in counts.items() if count == modal_count)
    return SectorResolution(ff17num, None, len(valid) - modal_count)


def resolve_sic_to_ff17(value: object) -> SectorResolution:
    """Map only a valid, explicitly listed four-digit SIC to the canonical FF17."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return SectorResolution(None, "missing_sic")
    if isinstance(value, bool):
        return SectorResolution(None, "invalid_sic")
    try:
        sic = int(str(value).strip())
    except (TypeError, ValueError):
        return SectorResolution(None, "invalid_sic")
    if sic < 1 or sic > 9999:
        return SectorResolution(None, "invalid_sic")
    for start, end, ff17num in FF17_RANGES:
        if start <= sic <= end:
            return SectorResolution(ff17num, None)
    return SectorResolution(None, "sic_not_in_ff17_definition")

"""
LiveCold — SOP Parser
Reads the cold_chain_SOP.txt and extracts structured product temperature ranges.
Makes the SOP the single source of truth for the entire system.

Usage:
    from core.sop_parser import get_product_temp_ranges, get_product_profile

    ranges = get_product_temp_ranges()
    # {'Vaccines': (2, 8), 'Frozen_Meat': (-18, -12), 'Dairy': (2, 6), ...}

    profile = get_product_profile("Dairy")
    # {'name': 'Dairy', 'safe_min': 2, 'safe_max': 6}
"""

import os
import re
import logging

log = logging.getLogger("sop_parser")

# Path to the SOP file (relative to project root)
SOP_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "watched_docs",
    "cold_chain_SOP.txt",
)

# Hardcoded fallback — used ONLY if SOP file is missing or unparseable
_FALLBACK_RANGES = {
    "Vaccines":        (2, 8),
    "Frozen_Meat":     (-18, -12),
    "Dairy":           (2, 6),
    "Seafood":         (-1, 2),
    "Vegetables":      (7, 10),
    "Fruits":          (8, 12),
    "Pharmaceuticals": (2, 8),
    "Ice_Cream":       (-25, -18),
    "Flowers":         (2, 8),
}

# Cached result (parsed once, reused)
_cached_ranges = None


def _parse_sop_file(filepath: str) -> dict:
    """
    Parse the machine-readable PRODUCT_TEMP_RANGES block from the SOP file.

    Expected format inside the SOP:
        <!-- PRODUCT_TEMP_RANGES
        Dairy:2:6
        Frozen_Meat:-18:-12
        ...
        -->

    Returns dict: product_name -> (safe_min, safe_max)
    """
    try:
        with open(filepath, "r") as f:
            content = f.read()
    except FileNotFoundError:
        log.warning(f"SOP file not found: {filepath}")
        return None

    # Extract the PRODUCT_TEMP_RANGES block
    pattern = r"<!--\s*PRODUCT_TEMP_RANGES\s*\n(.*?)-->"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        log.warning("No PRODUCT_TEMP_RANGES block found in SOP file")
        return None

    block = match.group(1).strip()
    ranges = {}

    for line in block.split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split(":")
        if len(parts) != 3:
            log.warning(f"Skipping malformed SOP line: {line}")
            continue

        name = parts[0].strip()
        try:
            safe_min = float(parts[1].strip())
            safe_max = float(parts[2].strip())
            ranges[name] = (safe_min, safe_max)
        except ValueError:
            log.warning(f"Invalid temp values in SOP line: {line}")
            continue

    if not ranges:
        log.warning("No valid product ranges parsed from SOP")
        return None

    log.info(f"✅ Parsed {len(ranges)} product temp ranges from SOP")
    return ranges


def get_product_temp_ranges() -> dict:
    """
    Get all product temperature ranges.
    Reads from SOP file (cached after first call), falls back to hardcoded defaults.

    Returns:
        dict: product_name -> (safe_min, safe_max)
    """
    global _cached_ranges

    if _cached_ranges is not None:
        return _cached_ranges

    parsed = _parse_sop_file(SOP_PATH)
    if parsed:
        _cached_ranges = parsed
    else:
        log.warning("⚠️ Using fallback temperature ranges (SOP not available)")
        _cached_ranges = dict(_FALLBACK_RANGES)

    return _cached_ranges


def get_product_profile(product_name: str) -> dict:
    """
    Get temperature profile for a specific product.

    Args:
        product_name: Product name (case-insensitive, spaces converted to underscores)

    Returns:
        dict with keys: name, safe_min, safe_max
    """
    ranges = get_product_temp_ranges()

    # Normalize: "Frozen Meat" -> "Frozen_Meat", "frozen_meat" -> "Frozen_Meat"
    key = product_name.strip().replace(" ", "_")

    # Try exact match first
    if key in ranges:
        safe_min, safe_max = ranges[key]
        return {"name": key, "safe_min": safe_min, "safe_max": safe_max}

    # Try case-insensitive match
    key_lower = key.lower()
    for name, (smin, smax) in ranges.items():
        if name.lower() == key_lower:
            return {"name": name, "safe_min": smin, "safe_max": smax}

    # Fallback: default chilled range
    log.warning(f"Unknown product '{product_name}', defaulting to chilled (2-8°C)")
    return {"name": product_name, "safe_min": 2, "safe_max": 8}


def get_all_product_profiles() -> list:
    """
    Get all product profiles as a list (for use by shipment_factory).

    Returns:
        list of dicts with keys: name, safe_min, safe_max
    """
    ranges = get_product_temp_ranges()
    return [
        {"name": name, "safe_min": smin, "safe_max": smax}
        for name, (smin, smax) in ranges.items()
    ]


# ── Quick test when run directly ──────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("\n📄 SOP Parser — Product Temperature Ranges\n")

    ranges = get_product_temp_ranges()
    for product, (smin, smax) in ranges.items():
        print(f"  {product:20s}  {smin:>6}°C  →  {smax:>5}°C")

    print(f"\n  Total: {len(ranges)} products parsed from SOP\n")

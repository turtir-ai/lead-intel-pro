"""
AutoDiscover Engine - Autonomous Web Intelligence System

No LLM required. Pure Python heuristics + Playwright network sniffing.

Architecture:
1. Discovery (Brave API) → Find new potential sources
2. Diagnose (Playwright) → Capture network traffic, console, DOM
3. Analyze (Heuristics) → Pattern match without LLM
4. Extract (Auto-Adapter) → Generate scrapers automatically
5. Integrate (Pipeline) → Feed into lead_intel_v2

Author: Lead Intel v2 System
"""

from .discoverer import BraveDiscoverer
from .diagnoser import SiteDiagnoser
from .analyzer import PatternAnalyzer
from .adapter_generator import AdapterGenerator
from .engine import AutoDiscoverEngine

__all__ = [
    "BraveDiscoverer",
    "SiteDiagnoser", 
    "PatternAnalyzer",
    "AdapterGenerator",
    "AutoDiscoverEngine",
]

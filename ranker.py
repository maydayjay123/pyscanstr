"""Scoring and ranking system for meme candidates."""

from dataclasses import dataclass
from typing import Optional
from detector import SignalResult
from solana_coins import MemeCoin
from config import TOP_N


@dataclass
class RankedMeme:
    """A ranked meme with score and optional coin info."""
    rank: int
    signal: SignalResult
    coins: list[MemeCoin]  # Related Solana coins (if any)


def rank_candidates(candidates: list[SignalResult]) -> list[RankedMeme]:
    """Rank candidates by virality score."""
    # Sort by virality score
    sorted_candidates = sorted(candidates, key=lambda x: x.virality_score, reverse=True)

    # Take top N and assign ranks
    ranked = []
    for i, signal in enumerate(sorted_candidates[:TOP_N]):
        ranked.append(RankedMeme(
            rank=i + 1,
            signal=signal,
            coins=[],  # Will be filled in later
        ))

    return ranked


async def get_top_memes() -> list[RankedMeme]:
    """Main function: detect signals, rank, find coins, return top memes."""
    from detector import get_all_candidates, filter_promising
    from solana_coins import find_related_coins

    # Get all candidates
    candidates = get_all_candidates()

    # Filter promising ones
    promising = filter_promising(candidates)

    # Rank them
    ranked = rank_candidates(promising)

    # Find related coins for each
    for meme in ranked:
        coins = await find_related_coins(meme.signal.sound_name)
        meme.coins = coins

    return ranked

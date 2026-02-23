"""
Text analysis module for the Slither text analysis pipeline example.

Provides batch text analysis using only Python stdlib: word frequency,
readability scoring (simplified Flesch-Kincaid), and sentiment analysis
against a built-in lexicon.

CONCURRENCY NOTE:
Under free-threaded Python (no GIL), multiple threads calling analyze_batch
concurrently would race on _global_index[word] += count (lost updates) and
_doc_count += len(items) (torn counter). Slither avoids this by running each
worker in a separate OS process -- each has its own _global_index with zero
contention.
"""

import os
import re
import math
from collections import Counter


# ---------------------------------------------------------------------------
# Module-level shared mutable state (per-worker accumulator)
#
# Under free-threaded Python, concurrent threads mutating these structures
# would cause data corruption:
#   - _global_index[word] += count  ->  lost updates (read-modify-write race)
#   - _doc_count += 1               ->  torn counter
#   - _worker_id assignment         ->  identity confusion
#
# Slither runs each worker in a separate OS process, so each has its own
# copy of these globals with zero contention.
# ---------------------------------------------------------------------------

_global_index = {}    # word -> total frequency across ALL batches processed by this worker
_doc_count = 0        # total documents processed by this worker
_worker_id = None     # set on first call, identifies which worker we are


# ---------------------------------------------------------------------------
# Sentiment lexicons (~35-40 words each)
# ---------------------------------------------------------------------------

POSITIVE_WORDS = {
    "good", "great", "excellent", "amazing", "wonderful", "fantastic",
    "beautiful", "brilliant", "cheerful", "delightful", "enjoyable",
    "fabulous", "generous", "happy", "incredible", "joyful", "kind",
    "lovely", "magnificent", "nice", "outstanding", "perfect", "pleasant",
    "remarkable", "satisfied", "spectacular", "splendid", "superb",
    "terrific", "thrilling", "tremendous", "valuable", "vibrant",
    "warm", "worthy", "awesome", "blissful", "charming", "elegant",
    "grateful",
}

NEGATIVE_WORDS = {
    "bad", "terrible", "awful", "horrible", "dreadful", "disgusting",
    "annoying", "boring", "cruel", "disappointing", "dismal", "dull",
    "evil", "fearful", "gloomy", "harsh", "hideous", "hostile",
    "inferior", "miserable", "nasty", "offensive", "painful", "poor",
    "repulsive", "rude", "sad", "shocking", "stupid", "ugly",
    "unpleasant", "vile", "wicked", "worthless", "wretched", "angry",
    "desperate", "frustrating", "outrageous", "toxic",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_syllables(word):
    """
    Estimate syllable count using vowel-group heuristic.

    Counts groups of consecutive vowels, with adjustments for
    silent trailing 'e' and a minimum of 1 syllable per word.
    """
    word = word.lower()
    if len(word) <= 2:
        return 1

    # Count vowel groups
    vowel_groups = re.findall(r'[aeiouy]+', word)
    count = len(vowel_groups)

    # Subtract silent trailing 'e' (but not "le" endings like "bottle")
    if word.endswith('e') and not word.endswith('le') and count > 1:
        count -= 1

    # Common suffixes that add syllables
    if word.endswith('ed') and not word.endswith(('ted', 'ded')):
        count = max(count - 1, 1)

    return max(count, 1)


def _count_sentences(text):
    """Count sentences by splitting on sentence-ending punctuation."""
    sentences = re.split(r'[.!?]+', text)
    # Filter out empty strings from trailing punctuation
    sentences = [s.strip() for s in sentences if s.strip()]
    return max(len(sentences), 1)


def _accumulate(word_freqs):
    """
    Merge word frequencies from one document into the global index.

    This is the operation that would race under free-threaded Python:
    _global_index[word] += count is a read-modify-write on a shared dict.
    With N threads and no GIL, concurrent += on the same key loses updates.
    """
    global _global_index
    for word, count in word_freqs.items():
        # Under free-threaded Python with multiple threads, this line
        # is a race condition: read current value, add count, write back.
        # Two threads reading the same value before either writes = lost update.
        _global_index[word] = _global_index.get(word, 0) + count


def _analyze_one(item):
    """
    Analyze a single text item.

    Args:
        item: dict with keys "text" (str) and "stopwords" (list of str)

    Returns:
        dict with keys: word_count, unique_words, top_words, readability,
        sentiment, positive_count, negative_count
    """
    text = item.get("text", "")
    stopwords = set(item.get("stopwords", []))

    # Tokenize
    all_words = re.findall(r'\b\w+\b', text.lower())
    total_words = len(all_words)

    # Filter stopwords for frequency analysis
    content_words = [w for w in all_words if w not in stopwords]

    # Word frequency
    counter = Counter(content_words)
    top_words = dict(counter.most_common(10))
    unique_words = len(set(content_words))

    # Readability: simplified Flesch-Kincaid
    if total_words > 0:
        sentence_count = _count_sentences(text)
        total_syllables = sum(_count_syllables(w) for w in all_words)
        words_per_sentence = total_words / sentence_count
        syllables_per_word = total_syllables / total_words
        readability = 206.835 - 1.015 * words_per_sentence - 84.6 * syllables_per_word
        readability = round(readability, 2)
    else:
        readability = 0.0

    # Sentiment
    positive_count = sum(1 for w in all_words if w in POSITIVE_WORDS)
    negative_count = sum(1 for w in all_words if w in NEGATIVE_WORDS)

    if total_words > 0:
        sentiment = round((positive_count - negative_count) / total_words, 4)
    else:
        sentiment = 0.0

    return {
        "word_count": total_words,
        "unique_words": unique_words,
        "top_words": top_words,
        "readability": readability,
        "sentiment": sentiment,
        "positive_count": positive_count,
        "negative_count": negative_count,
    }


def analyze_batch(items):
    """
    Analyze a batch of text items, accumulating global statistics.

    Args:
        items: list of dicts with keys "text" (str) and "stopwords" (list of str)

    Returns:
        list of result dicts (SAME LENGTH as input) with keys:
            word_count, unique_words, top_words (dict), readability (float),
            sentiment (float -1 to 1), positive_count, negative_count

    Side-effect:
        Merges each document's word frequencies into _global_index and
        increments _doc_count. Under free-threaded Python, this side-effect
        would cause data corruption when called from multiple threads.
    """
    global _worker_id, _doc_count

    # Set worker identity on first call
    if _worker_id is None:
        _worker_id = os.getpid()

    results = []
    for item in items:
        result = _analyze_one(item)
        results.append(result)

        # Accumulate into global state -- this is the race condition.
        # Under free-threaded Python with N threads:
        #   Thread A reads _global_index["the"] = 5
        #   Thread B reads _global_index["the"] = 5
        #   Thread A writes _global_index["the"] = 5 + 3 = 8
        #   Thread B writes _global_index["the"] = 5 + 2 = 7  (Thread A's update is lost!)
        _accumulate(result["top_words"])

        # _doc_count += 1 is also a race: read, increment, write.
        # Two threads reading the same value = one increment is lost.
        _doc_count += 1

    return results


def get_worker_stats(items):
    """
    Return this worker's accumulated statistics.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    Each result contains the same worker stats snapshot.
    """
    stats = {
        "worker_id": _worker_id,
        "doc_count": _doc_count,
        "vocab_size": len(_global_index),
        "top_words": dict(sorted(_global_index.items(), key=lambda x: -x[1])[:20]),
        "total_word_occurrences": sum(_global_index.values()),
    }
    return [stats for _ in items]

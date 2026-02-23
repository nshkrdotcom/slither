"""
Image processing: generate test images, create thumbnails, get info.

Provides four public functions for the Slither image thumbnail example.
Each function receives a list of dicts and returns a list of dicts of the
SAME LENGTH. Requires Pillow (PIL).

Binary image data is transferred as base64-encoded strings since
SnakeBridge serializes through JSON.

Under free-threaded Python, Pillow's C internals (libImaging) are not
thread-safe -- concurrent Image.resize() with LANCZOS filter can produce
corrupted output or segfault. Without backpressure, N threads each loading
a 4K image simultaneously = N * 25MB memory spike. _memory_watermark
tracking via read-modify-write is a data race. Slither's WeightedBatch
strategy caps concurrent pixel processing, max_in_flight limits memory,
and process isolation makes Pillow safe.
"""

import base64
import io
import os


# ---------------------------------------------------------------------------
# Module-level state for memory tracking
#
# Under free-threaded Python these are shared across threads with no
# synchronization.  _memory_watermark = max(...) is a classic
# read-modify-write race.  _current_memory += ... from multiple threads
# yields incorrect tracking.  In Slither each Python worker is a separate
# OS process, so these are per-worker and race-free.
# ---------------------------------------------------------------------------

_memory_watermark = 0     # peak memory usage in bytes (approximate)
_images_processed = 0     # total images processed
_worker_id = None         # os.getpid(), set on first call
_current_memory = 0       # current estimated memory usage


def _init_worker():
    """Set the worker id on first call."""
    global _worker_id
    if _worker_id is None:
        _worker_id = os.getpid()


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------


def check_pillow(items):
    """
    Check whether Pillow is installed.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    """
    try:
        import PIL

        result = {"available": True, "version": PIL.__version__}
    except ImportError:
        result = {"available": False, "version": None}
    return [result for _ in items]


# ---------------------------------------------------------------------------
# Image generation
# ---------------------------------------------------------------------------


def generate_test_images(specs):
    """
    Generate solid-color test images with a text label.

    Args:
        specs: list of {"width": int, "height": int, "color": [r,g,b], "label": str}

    Returns:
        list of {
            "data": base64_str,
            "format": "png",
            "width": int,
            "height": int,
            "size_bytes": int,
            "label": str,
        }
    """
    from PIL import Image, ImageDraw

    _init_worker()

    results = []

    for spec in specs:
        w = spec["width"]
        h = spec["height"]
        color = tuple(spec.get("color", [128, 128, 128]))
        label = spec.get("label", "")

        # Create a solid-color image
        img = Image.new("RGB", (w, h), color)

        # Draw a label in the top-left corner
        if label:
            draw = ImageDraw.Draw(img)
            try:
                draw.text((10, 10), label, fill=(255, 255, 255))
            except Exception:
                pass

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        png_bytes = buf.getvalue()

        results.append(
            {
                "data": base64.b64encode(png_bytes).decode("ascii"),
                "format": "png",
                "width": w,
                "height": h,
                "size_bytes": len(png_bytes),
                "label": label,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Thumbnail generation
# ---------------------------------------------------------------------------


def generate_thumbnails(items):
    """
    Create thumbnails from base64-encoded images.

    Tracks per-worker memory to demonstrate that Slither's backpressure
    keeps peak memory bounded.  Under free-threaded Python the
    _current_memory and _memory_watermark updates are data races.

    Args:
        items: list of {
            "data": base64_str,
            "target_width": int (default 150),
            "quality": int (default 85, JPEG only),
            "output_format": "PNG" | "JPEG" (default "PNG"),
        }

    Returns:
        list of {
            "data": base64_str,
            "original_size": [w, h],
            "thumb_size": [w, h],
            "original_bytes": int,
            "thumb_bytes": int,
            "compression_ratio": float,
        }
    """
    global _memory_watermark, _images_processed, _current_memory

    from PIL import Image

    _init_worker()

    results = []

    for item in items:
        raw = base64.b64decode(item["data"])
        img = Image.open(io.BytesIO(raw))
        orig_w, orig_h = img.size

        target_w = item.get("target_width", 150)
        ratio = target_w / orig_w
        new_w = max(1, target_w)
        new_h = max(1, int(orig_h * ratio))

        # --- Memory tracking (before processing) ---
        # Estimate memory: width * height * channels (3 for RGB, 4 for RGBA)
        image_memory = orig_w * orig_h * 3 + new_w * new_h * 3
        _current_memory += image_memory
        _memory_watermark = max(_memory_watermark, _current_memory)
        _images_processed += 1

        # Image.resize with LANCZOS is NOT thread-safe in free-threaded
        # Python -- libImaging's internal buffers can be corrupted when
        # multiple threads resize concurrently.  Process isolation avoids
        # this entirely.
        thumb = img.resize((new_w, new_h), Image.LANCZOS)

        buf = io.BytesIO()
        fmt = item.get("output_format", "PNG")
        quality = item.get("quality", 85)

        if fmt.upper() == "JPEG":
            thumb = thumb.convert("RGB")
            thumb.save(buf, format="JPEG", quality=quality)
        else:
            thumb.save(buf, format="PNG", optimize=True)

        thumb_bytes = buf.getvalue()
        orig_bytes_len = len(raw)
        thumb_bytes_len = len(thumb_bytes)

        # --- Memory tracking (after encoding) ---
        # Release the estimate now that pixel buffers are freed
        _current_memory -= image_memory

        results.append(
            {
                "data": base64.b64encode(thumb_bytes).decode("ascii"),
                "original_size": [orig_w, orig_h],
                "thumb_size": [new_w, new_h],
                "original_bytes": orig_bytes_len,
                "thumb_bytes": thumb_bytes_len,
                "compression_ratio": round(
                    thumb_bytes_len / max(orig_bytes_len, 1), 4
                ),
            }
        )

    return results


# ---------------------------------------------------------------------------
# Image info
# ---------------------------------------------------------------------------


def get_image_info(items):
    """
    Return metadata for base64-encoded images.

    Args:
        items: list of {"data": base64_str}

    Returns:
        list of {
            "width": int,
            "height": int,
            "format": str,
            "mode": str,
            "size_bytes": int,
        }
    """
    from PIL import Image

    _init_worker()

    results = []

    for item in items:
        raw = base64.b64decode(item["data"])
        img = Image.open(io.BytesIO(raw))

        results.append(
            {
                "width": img.size[0],
                "height": img.size[1],
                "format": img.format or "unknown",
                "mode": img.mode,
                "size_bytes": len(raw),
            }
        )

    return results


# ---------------------------------------------------------------------------
# Memory statistics
# ---------------------------------------------------------------------------


def get_memory_stats(items):
    """
    Return this worker's memory and processing statistics.

    Returns one result per input item (Slither contract: len(output) == len(input)).
    Each result contains the same worker stats snapshot.
    """
    _init_worker()

    stats = {
        "worker_id": _worker_id,
        "images_processed": _images_processed,
        "peak_memory_bytes": _memory_watermark,
        "peak_memory_mb": round(_memory_watermark / (1024 * 1024), 2),
        "current_memory_bytes": _current_memory,
    }
    return [stats for _ in items]

defmodule Slither.Examples.ImagePipeline.ThumbnailDemo do
  @moduledoc """
  Demonstrates why process isolation beats free-threaded Python for image work.

  Generates 30 test images (up to 4K resolution) in Python via Pillow, then
  creates thumbnails using the `WeightedBatch` strategy where batch boundaries
  are determined by total pixel count.  After processing, collects per-worker
  memory statistics to show that Slither's backpressure kept peak memory bounded.

  ## Why This Exists

  Under free-threaded Python (PEP 703):
    - Pillow's C internals (libImaging) are not thread-safe; concurrent
      `Image.resize(LANCZOS)` can produce corrupted output or segfault
    - No built-in backpressure means 16 threads each loading a 4K image
      = 500MB+ memory spike
    - One thread's segfault kills ALL threads

  Under Slither:
    - `WeightedBatch` groups images by pixel count so large images get
      their own batch
    - `max_in_flight: 2` caps concurrent batches (backpressure)
    - Each Python worker is a separate OS process, so Pillow is safe
    - A crash in one worker does not affect others

  Run with:

      Slither.Examples.ImagePipeline.ThumbnailDemo.run_demo()
  """

  alias Slither.Dispatch
  alias Slither.Dispatch.Strategies.FixedBatch
  alias Slither.Dispatch.Strategies.WeightedBatch
  alias Slither.Item

  # 30 image specs: from tiny 100x100 to 4K, with duplicates of larger sizes
  # to create meaningful backpressure.
  @image_specs [
    # Small images
    %{width: 100, height: 100, color: [255, 100, 100], label: "tiny"},
    %{width: 200, height: 150, color: [100, 255, 100], label: "small-1"},
    %{width: 200, height: 200, color: [100, 100, 255], label: "small-2"},
    %{width: 320, height: 240, color: [200, 200, 100], label: "qvga"},
    %{width: 400, height: 300, color: [255, 255, 100], label: "medium-1"},
    # Medium images
    %{width: 500, height: 400, color: [100, 255, 255], label: "medium-2"},
    %{width: 640, height: 480, color: [255, 100, 255], label: "vga"},
    %{width: 800, height: 600, color: [200, 150, 100], label: "svga"},
    %{width: 1024, height: 768, color: [100, 200, 150], label: "xga"},
    %{width: 1280, height: 720, color: [150, 100, 200], label: "hd-720"},
    # Large images
    %{width: 1920, height: 1080, color: [180, 180, 100], label: "hd-1080"},
    %{width: 2560, height: 1440, color: [100, 180, 180], label: "qhd"},
    %{width: 3840, height: 2160, color: [180, 100, 180], label: "4k"},
    # Duplicates of large sizes to stress backpressure
    %{width: 1920, height: 1080, color: [160, 200, 100], label: "hd-1080-b"},
    %{width: 1920, height: 1080, color: [100, 160, 200], label: "hd-1080-c"},
    %{width: 2560, height: 1440, color: [200, 100, 160], label: "qhd-b"},
    %{width: 2560, height: 1440, color: [160, 100, 200], label: "qhd-c"},
    %{width: 3840, height: 2160, color: [200, 160, 100], label: "4k-b"},
    %{width: 3840, height: 2160, color: [100, 200, 160], label: "4k-c"},
    %{width: 3840, height: 2160, color: [140, 140, 200], label: "4k-d"},
    # More medium images for batch variety
    %{width: 640, height: 480, color: [220, 120, 120], label: "vga-b"},
    %{width: 800, height: 600, color: [120, 220, 120], label: "svga-b"},
    %{width: 1024, height: 768, color: [120, 120, 220], label: "xga-b"},
    %{width: 1280, height: 720, color: [220, 220, 120], label: "hd-720-b"},
    %{width: 1280, height: 720, color: [120, 220, 220], label: "hd-720-c"},
    # Extra large duplicates
    %{width: 1920, height: 1080, color: [200, 200, 200], label: "hd-1080-d"},
    %{width: 2560, height: 1440, color: [180, 140, 180], label: "qhd-d"},
    %{width: 3840, height: 2160, color: [140, 180, 140], label: "4k-e"},
    %{width: 1280, height: 720, color: [200, 140, 140], label: "hd-720-d"},
    %{width: 1920, height: 1080, color: [140, 200, 140], label: "hd-1080-e"}
  ]

  @max_weight 2_000_000
  @max_in_flight 2

  @doc """
  Run the full image thumbnail demo, printing results to stdout.
  """
  def run_demo do
    IO.puts("\n=== Slither Example: Image Thumbnail Pipeline ===\n")

    case check_pillow() do
      :ok ->
        do_run_demo()

      {:error, msg} ->
        IO.puts("  Skipping: #{msg}")
        IO.puts("  Run via: mix slither.example image_pipeline (auto-installs via Snakepit/uv)")
    end
  end

  # ---------------------------------------------------------------------------
  # Pillow availability check
  # ---------------------------------------------------------------------------

  defp check_pillow do
    item = Item.wrap(%{})

    case Dispatch.run([item],
           executor: Slither.Dispatch.Executors.SnakeBridge,
           module: "image_processor",
           function: "check_pillow",
           batch_size: 1,
           max_in_flight: 1
         ) do
      {:ok, [result]} ->
        if result.payload["available"], do: :ok, else: {:error, "Pillow not installed"}

      _ ->
        {:error, "Could not check Pillow availability"}
    end
  end

  # ---------------------------------------------------------------------------
  # Demo body
  # ---------------------------------------------------------------------------

  defp do_run_demo do
    images = generate_images()
    print_generated_images(images)
    {thumbnails, time} = create_thumbnails(images)
    print_thumbnail_results(thumbnails, time)
    memory_stats = collect_memory_stats()
    print_memory_stats(memory_stats)
    print_backpressure_analysis(thumbnails, memory_stats)
    print_why_this_matters()

    IO.puts("Done!")
  end

  # ---------------------------------------------------------------------------
  # Step 1: Generate test images (FixedBatch)
  # ---------------------------------------------------------------------------

  defp generate_images do
    IO.puts("Generating #{length(@image_specs)} test images...")

    spec_items = Item.wrap_many(@image_specs)

    {:ok, images} =
      Dispatch.run(spec_items,
        executor: Slither.Dispatch.Executors.SnakeBridge,
        module: "image_processor",
        function: "generate_test_images",
        strategy: FixedBatch,
        batch_size: 10,
        max_in_flight: 2
      )

    images
  end

  defp print_generated_images(images) do
    IO.puts("  Generated #{length(images)} images:")

    for img <- images do
      p = img.payload
      pixels = p["width"] * p["height"]

      IO.puts(
        "    #{p["label"]}: #{p["width"]}x#{p["height"]} (#{format_pixels(pixels)}, #{format_bytes(p["size_bytes"])})"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Step 2: Create thumbnails with WeightedBatch
  # ---------------------------------------------------------------------------

  defp create_thumbnails(images) do
    IO.puts(
      "\nCreating thumbnails with WeightedBatch (max_weight: #{format_pixels(@max_weight)})...\n"
    )

    thumb_items = build_thumb_items(images)

    {time, result} =
      :timer.tc(fn ->
        Dispatch.run(thumb_items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "image_processor",
          function: "generate_thumbnails",
          strategy: WeightedBatch,
          max_weight: @max_weight,
          weight_fn: fn item -> item.meta[:pixel_count] || 10_000 end,
          max_in_flight: @max_in_flight,
          on_error: :skip
        )
      end)

    case result do
      {:ok, thumbnails} -> {thumbnails, time}
      {:error, _reason, partial} -> {partial, time}
    end
  end

  defp build_thumb_items(images) do
    Enum.map(images, fn img ->
      p = img.payload
      pixel_count = p["width"] * p["height"]

      %{
        "data" => p["data"],
        "target_width" => 150,
        "output_format" => "PNG"
      }
      |> Item.wrap()
      |> Item.put_meta(%{pixel_count: pixel_count, label: p["label"]})
    end)
  end

  defp print_thumbnail_results(thumbnails, time) do
    IO.puts("  Thumbnails generated in #{div(time, 1000)}ms:")

    for thumb <- thumbnails do
      p = thumb.payload
      [ow, oh] = p["original_size"]
      [tw, th] = p["thumb_size"]

      IO.puts(
        "    #{ow}x#{oh} -> #{tw}x#{th} " <>
          "(#{format_bytes(p["original_bytes"])} -> #{format_bytes(p["thumb_bytes"])})"
      )
    end

    {total_original, total_thumb} = sum_bytes(thumbnails)
    ratio = Float.round(total_thumb / max(total_original, 1) * 100, 1)

    IO.puts(
      "\n  Total: #{format_bytes(total_original)} -> #{format_bytes(total_thumb)} " <>
        "(#{ratio}% of original)"
    )
  end

  # ---------------------------------------------------------------------------
  # Step 3: Collect per-worker memory statistics
  # ---------------------------------------------------------------------------

  defp collect_memory_stats do
    IO.puts("\n--- Per-Worker Memory Statistics ---\n")

    # Send a stats request to each worker in the pool.  We send multiple
    # items so that if the pool has >1 worker we hear from each one.
    stats_items = Item.wrap_many(for _ <- 1..4, do: %{})

    case Dispatch.run(stats_items,
           executor: Slither.Dispatch.Executors.SnakeBridge,
           module: "image_processor",
           function: "get_memory_stats",
           batch_size: 1,
           max_in_flight: 4,
           on_error: :skip
         ) do
      {:ok, results} ->
        results
        |> Enum.map(& &1.payload)
        |> Enum.uniq_by(& &1["worker_id"])

      _ ->
        []
    end
  end

  defp print_memory_stats([]) do
    IO.puts("  (no memory stats available)")
  end

  defp print_memory_stats(stats) do
    for s <- stats do
      IO.puts("  Worker #{s["worker_id"]}:")
      IO.puts("    Images processed:  #{s["images_processed"]}")

      IO.puts(
        "    Peak memory:       #{s["peak_memory_mb"]} MB (#{format_bytes(s["peak_memory_bytes"])})"
      )

      IO.puts("    Current memory:    #{format_bytes(s["current_memory_bytes"])}")
    end
  end

  # ---------------------------------------------------------------------------
  # Step 4: Backpressure analysis
  # ---------------------------------------------------------------------------

  defp print_backpressure_analysis(thumbnails, memory_stats) do
    IO.puts("\n--- Backpressure ---\n")

    total_images = length(thumbnails)
    avg_pixel_mem = avg_image_memory(thumbnails)

    theoretical_bytes = total_images * avg_pixel_mem
    theoretical_mb = Float.round(theoretical_bytes / (1024 * 1024), 1)

    IO.puts("  WeightedBatch max_weight: #{format_pixels(@max_weight)}")
    IO.puts("    -> Large images (4K = #{format_pixels(3840 * 2160)}) get their own batch")
    IO.puts("    -> Small images are grouped together efficiently")

    IO.puts(
      "  max_in_flight: #{@max_in_flight} (at most #{@max_in_flight} batches processing simultaneously)"
    )

    IO.puts("")

    IO.puts(
      "  Without backpressure: #{total_images} images * avg #{format_bytes(avg_pixel_mem)} = #{theoretical_mb} MB simultaneous"
    )

    peak_per_worker =
      memory_stats
      |> Enum.map(& &1["peak_memory_mb"])
      |> Enum.max(fn -> 0 end)

    IO.puts("  With Slither: peak per-worker was #{peak_per_worker} MB")
  end

  defp avg_image_memory(thumbnails) do
    if thumbnails == [] do
      0
    else
      total =
        Enum.reduce(thumbnails, 0, fn thumb, acc ->
          p = thumb.payload
          [ow, oh] = p["original_size"]
          [tw, th] = p["thumb_size"]
          acc + ow * oh * 3 + tw * th * 3
        end)

      div(total, length(thumbnails))
    end
  end

  # ---------------------------------------------------------------------------
  # Step 5: Why this matters
  # ---------------------------------------------------------------------------

  defp print_why_this_matters do
    IO.puts("\n--- Why This Matters ---\n")

    IO.puts("  Under free-threaded Python:")
    IO.puts("    - 16 threads * 4K image = 400MB+ spike, no backpressure")
    IO.puts("    - Pillow's Image.resize(LANCZOS) is not thread-safe in free-threaded builds")
    IO.puts("    - _memory_watermark = max(...) is a read-modify-write data race")
    IO.puts("    - One thread's segfault kills ALL threads")
    IO.puts("")
    IO.puts("  Under Slither:")
    IO.puts("    - WeightedBatch + max_in_flight = bounded memory, real backpressure")
    IO.puts("    - Process isolation = safe Pillow (each worker is a separate OS process)")
    IO.puts("    - A crash in one worker does not affect others")
    IO.puts("    - Per-worker memory stats are race-free (no shared mutable state)")
    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp sum_bytes(thumbnails) do
    Enum.reduce(thumbnails, {0, 0}, fn thumb, {acc_orig, acc_thumb} ->
      p = thumb.payload
      {acc_orig + p["original_bytes"], acc_thumb + p["thumb_bytes"]}
    end)
  end

  defp format_bytes(bytes) when is_number(bytes) and bytes < 1024, do: "#{bytes}B"

  defp format_bytes(bytes) when is_number(bytes) and bytes < 1_048_576,
    do: "#{Float.round(bytes / 1024, 1)}KB"

  defp format_bytes(bytes) when is_number(bytes),
    do: "#{Float.round(bytes / 1_048_576, 1)}MB"

  defp format_pixels(pixels) when pixels >= 1_000_000,
    do: "#{Float.round(pixels / 1_000_000, 1)}MP"

  defp format_pixels(pixels) when pixels >= 1_000,
    do: "#{Float.round(pixels / 1_000, 0)}KP"

  defp format_pixels(pixels), do: "#{pixels}P"
end

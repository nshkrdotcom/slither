defmodule Slither.Examples.ImagePipeline.ThumbnailDemo do
  @moduledoc """
  Demonstrates why process isolation beats free-threaded Python for image work.

  Generates 200 test images (50 small, 80 medium, 50 large 1080p, 20 XL 4K)
  in Python via Pillow, then creates thumbnails using `WeightedBatch` where
  batch boundaries are determined by total pixel count.

  After processing, collects per-worker memory statistics and reports
  megapixels/sec throughput.

  Run with `Slither.Examples.ImagePipeline.ThumbnailDemo.run_demo/0`.
  """

  alias Slither.Dispatch
  alias Slither.Dispatch.Strategies.FixedBatch
  alias Slither.Dispatch.Strategies.WeightedBatch
  alias Slither.Examples.Reporter
  alias Slither.Item

  @max_weight 2_000_000
  @max_in_flight 4

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
    image_specs = generate_image_specs()
    IO.puts("Generating #{length(image_specs)} test images (#{count_by_size(image_specs)})...")

    images = generate_images(image_specs)
    print_generated_images(images)

    {thumbnails, time} = create_thumbnails(images)
    print_thumbnail_results(thumbnails, time)
    print_megapixels_throughput(images, time)

    memory_stats = collect_memory_stats()
    print_memory_stats(memory_stats)
    print_backpressure_analysis(thumbnails, memory_stats)
    print_why_this_matters()

    IO.puts("Done!")
  end

  # ---------------------------------------------------------------------------
  # Image spec generation — 200 images
  # ---------------------------------------------------------------------------

  defp generate_image_specs do
    :rand.seed(:exsss, {42, 137, 256})

    # 50 small images (100-400px)
    small =
      for _ <- 1..50 do
        w = 100 + :rand.uniform(300)
        h = 100 + :rand.uniform(300)
        color = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
        %{width: w, height: h, color: color, label: "small"}
      end

    # 80 medium images (500-1280px)
    medium =
      for _ <- 1..80 do
        w = 500 + :rand.uniform(780)
        h = 400 + :rand.uniform(520)
        color = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
        %{width: w, height: h, color: color, label: "medium"}
      end

    # 50 large images (1080p)
    large =
      for _ <- 1..50 do
        color = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
        %{width: 1920, height: 1080, color: color, label: "large-1080p"}
      end

    # 20 XL images (4K)
    xl =
      for _ <- 1..20 do
        color = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
        %{width: 3840, height: 2160, color: color, label: "xl-4k"}
      end

    (small ++ medium ++ large ++ xl) |> Enum.shuffle()
  end

  defp count_by_size(specs) do
    counts = Enum.frequencies_by(specs, & &1.label)

    ["small", "medium", "large-1080p", "xl-4k"]
    |> Enum.map_join(", ", fn label -> "#{Map.get(counts, label, 0)} #{label}" end)
  end

  # ---------------------------------------------------------------------------
  # Step 1: Generate test images (FixedBatch)
  # ---------------------------------------------------------------------------

  defp generate_images(specs) do
    spec_items = Item.wrap_many(specs)

    {time, {:ok, images}} =
      :timer.tc(fn ->
        Dispatch.run(spec_items,
          executor: Slither.Dispatch.Executors.SnakeBridge,
          module: "image_processor",
          function: "generate_test_images",
          strategy: FixedBatch,
          batch_size: 20,
          max_in_flight: 8
        )
      end)

    Reporter.print_timing("Image generation", time)
    images
  end

  defp print_generated_images(images) do
    # Show distribution summary instead of each image
    sizes = Enum.map(images, fn img -> img.payload["width"] * img.payload["height"] end)
    total_pixels = Enum.sum(sizes)
    total_bytes = Enum.sum(Enum.map(images, fn img -> img.payload["size_bytes"] end))

    IO.puts("  Generated #{length(images)} images:")
    IO.puts("    Total pixels: #{format_pixels(total_pixels)}")
    IO.puts("    Total size: #{format_bytes(total_bytes)}")
    IO.puts("    Range: #{format_pixels(Enum.min(sizes))} - #{format_pixels(Enum.max(sizes))}")
  end

  # ---------------------------------------------------------------------------
  # Step 2: Create thumbnails with WeightedBatch
  # ---------------------------------------------------------------------------

  defp create_thumbnails(images) do
    IO.puts(
      "\nCreating thumbnails with WeightedBatch (max_weight: #{format_pixels(@max_weight)}, max_in_flight: #{@max_in_flight})...\n"
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
    Reporter.print_timing("Thumbnail creation", time)

    {total_original, total_thumb} = sum_bytes(thumbnails)
    ratio = Float.round(total_thumb / max(total_original, 1) * 100, 1)

    IO.puts(
      "  #{length(thumbnails)} thumbnails: #{format_bytes(total_original)} -> #{format_bytes(total_thumb)} " <>
        "(#{ratio}% of original)"
    )
  end

  defp print_megapixels_throughput(images, time_us) do
    total_pixels =
      Enum.sum(Enum.map(images, fn img -> img.payload["width"] * img.payload["height"] end))

    megapixels = total_pixels / 1_000_000
    seconds = time_us / 1_000_000
    mp_per_sec = Float.round(megapixels / max(seconds, 0.001), 1)

    IO.puts("  Throughput: #{mp_per_sec} megapixels/sec (#{Float.round(megapixels, 1)} MP total)")
    Reporter.print_throughput("Images/sec", {length(images), time_us})
    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Step 3: Collect per-worker memory statistics
  # ---------------------------------------------------------------------------

  defp collect_memory_stats do
    IO.puts("--- Per-Worker Memory Statistics ---\n")

    stats_items = Item.wrap_many(for _ <- 1..200, do: %{})

    case Dispatch.run(stats_items,
           executor: Slither.Dispatch.Executors.SnakeBridge,
           module: "image_processor",
           function: "get_memory_stats",
           batch_size: 1,
           max_in_flight: 16,
           on_error: :skip
         ) do
      {:ok, results} ->
        results
        |> Enum.map(& &1.payload)
        |> Enum.reject(fn s -> s["worker_id"] == nil end)
        |> Enum.uniq_by(& &1["worker_id"])

      _ ->
        []
    end
  end

  defp print_memory_stats([]) do
    IO.puts("  (no memory stats available)")
  end

  defp print_memory_stats(stats) do
    for s <- Enum.take(stats, 10) do
      IO.puts("  Worker #{s["worker_id"]}:")
      IO.puts("    Images processed:  #{s["images_processed"]}")

      IO.puts(
        "    Peak memory:       #{s["peak_memory_mb"]} MB (#{format_bytes(s["peak_memory_bytes"])})"
      )

      IO.puts("    Current memory:    #{format_bytes(s["current_memory_bytes"])}")
    end

    if length(stats) > 10 do
      IO.puts("  ... and #{length(stats) - 10} more workers")
    end

    IO.puts("\n  Workers reached: #{length(stats)} of 48")
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
    IO.puts("    - Naive thread pools have no built-in weighted backpressure")
    IO.puts("    - Unbounded image fan-out causes large transient memory spikes")
    IO.puts("    - Shared memory watermark updates are read-modify-write races")
    IO.puts("    - A process crash still takes down all threads in that process")
    IO.puts("")
    IO.puts("  Under Slither:")
    IO.puts("    - WeightedBatch + max_in_flight = bounded memory, real backpressure")
    IO.puts("    - 200 images across 48 workers = safe concurrency at scale")
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

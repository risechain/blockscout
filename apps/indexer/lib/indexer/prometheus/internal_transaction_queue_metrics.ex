defmodule Indexer.Prometheus.InternalTransactionQueueMetrics do
  @moduledoc """
  Periodically samples the in-memory state of the
  `Indexer.Fetcher.InternalTransaction` BufferedTask and exposes it as
  Prometheus gauges:

    * `internal_transactions_buffer_queue_size`
    * `internal_transactions_buffer_block_min` / `_max`

  Kept separate from `Indexer.Prometheus.Metrics` (which ticks every hour
  over a mix of heavy DB queries) so we can sample the queue at a cadence
  useful for catching starvation.
  """

  use GenServer

  require Logger

  alias Indexer.Fetcher.InternalTransaction, as: InternalTransactionFetcher
  alias Indexer.Prometheus.Instrumenter

  @default_interval :timer.seconds(30)

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(_) do
    if Application.get_env(:indexer, __MODULE__)[:enabled] do
      send(self(), :tick)
      {:ok, %{}}
    else
      :ignore
    end
  end

  @impl true
  def handle_info(:tick, state) do
    sample()
    schedule_next()
    {:noreply, state}
  end

  defp sample do
    sample_buffer_queue_size()
  rescue
    error ->
      Logger.warning(fn -> ["BufferedTask queue sampling failed: ", inspect(error)] end)
  end

  defp sample_buffer_queue_size do
    # `:sys.get_state` copies the entire BufferedTask state across to this
    # process. For a multi-million-item buffer that's expensive but only
    # happens once per tick (default 30s). If it ever shows up in profiles,
    # the fix is a dedicated BufferedTask handle_call that computes min/max
    # in-process and returns just the 3-tuple. try/catch covers the case
    # where the fetcher is disabled or restarting.
    case buffer_state() do
      {:ok, state} ->
        {count, min_block, max_block} = buffer_block_stats(state)
        Instrumenter.set_internal_transactions_buffer_queue_size(count)
        Instrumenter.set_internal_transactions_buffer_block_min(min_block)
        Instrumenter.set_internal_transactions_buffer_block_max(max_block)

      :error ->
        :ok
    end
  end

  defp buffer_state do
    case Process.whereis(InternalTransactionFetcher) do
      nil ->
        :error

      pid ->
        try do
          {:ok, :sys.get_state(pid, 5_000)}
        catch
          :exit, _ -> :error
        end
    end
  end

  # Walks all three in-memory buffers of the BufferedTask:
  #   - current_buffer / current_front_buffer hold *batches* (each element
  #     is a list of items waiting to be dispatched on the next flush)
  #   - bound_queue holds *individual items* (BoundQueue.push_back_until_
  #     maximum_size iterates and pushes each entry separately)
  # Items are either raw block_number integers (data_type: :block_number)
  # or %{block_number: _, ...} maps (data_type: :transaction_params).
  defp buffer_block_stats(state) do
    items =
      Enum.concat([
        Enum.concat(state.current_buffer),
        Enum.concat(state.current_front_buffer),
        :queue.to_list(state.bound_queue.queue)
      ])

    case items |> Enum.reduce({0, nil, nil}, &fold_block_stat/2) do
      {0, _, _} -> {0, 0, 0}
      {count, min_block, max_block} -> {count, min_block, max_block}
    end
  end

  defp fold_block_stat(item, {count, current_min, current_max}) do
    case extract_block_number(item) do
      nil ->
        {count, current_min, current_max}

      block_number ->
        {count + 1, min_or(current_min, block_number), max_or(current_max, block_number)}
    end
  end

  defp extract_block_number(n) when is_integer(n), do: n
  defp extract_block_number(%{block_number: bn}) when is_integer(bn), do: bn
  defp extract_block_number(_), do: nil

  defp min_or(nil, b), do: b
  defp min_or(a, b), do: min(a, b)

  defp max_or(nil, b), do: b
  defp max_or(a, b), do: max(a, b)

  defp schedule_next do
    Process.send_after(self(), :tick, interval())
  end

  defp interval do
    Application.get_env(:indexer, __MODULE__)[:interval] || @default_interval
  end
end

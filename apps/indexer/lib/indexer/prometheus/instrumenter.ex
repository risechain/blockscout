defmodule Indexer.Prometheus.Instrumenter do
  @moduledoc """
  Blockchain data fetch and import metrics for `Prometheus`.
  """

  use Prometheus.Metric
  use Utils.RuntimeEnvHelper, chain_type: [:explorer, :chain_type]

  alias EthereumJSONRPC.Utility.RangesHelper

  @rollups [:arbitrum, :zksync, :optimism, :polygon_zkevm, :scroll]

  # Duration metrics are shaped as (sum counter + count counter + last gauge)
  # rather than histograms. Bucket boundaries on histograms hide multi-minute
  # outliers in the +Inf bucket and bias arithmetic-mean readouts low; the
  # trio gives a bucket-independent average via rate(_sum)/rate(_count) and a
  # latest-value gauge that Grafana charts with max_over_time(). Caveat: the
  # `_last` gauge is sampled at scrape time, so single outliers between
  # scrapes may be missed — add a "slow event" counter if guaranteed tail
  # coverage is needed.
  @counter [
    name: :block_full_processing_duration_microseconds_sum,
    labels: [:fetcher],
    help: "Cumulative block full-processing time including fetch and import (µs)"
  ]
  @counter [
    name: :block_full_processing_duration_microseconds_count,
    labels: [:fetcher],
    help: "Number of block full-processing observations"
  ]
  @gauge [
    name: :block_full_processing_duration_microseconds_last,
    labels: [:fetcher],
    help: "Most recent block full-processing duration (µs)"
  ]

  @counter [
    name: :block_import_duration_microseconds_sum,
    labels: [:fetcher],
    help: "Cumulative block import time, per block (µs)"
  ]
  @counter [
    name: :block_import_duration_microseconds_count,
    labels: [:fetcher],
    help: "Number of block import observations"
  ]
  @gauge [
    name: :block_import_duration_microseconds_last,
    labels: [:fetcher],
    help: "Most recent block import duration, per block (µs)"
  ]

  @counter [
    name: :block_batch_fetch_request_duration_microseconds_sum,
    labels: [:fetcher],
    help: "Cumulative block-batch fetch request time (µs)"
  ]
  @counter [
    name: :block_batch_fetch_request_duration_microseconds_count,
    labels: [:fetcher],
    help: "Number of block-batch fetch request observations"
  ]
  @gauge [
    name: :block_batch_fetch_request_duration_microseconds_last,
    labels: [:fetcher],
    help: "Most recent block-batch fetch request duration (µs)"
  ]

  # Chain.import time per internal-tx batch.
  @counter [
    name: :internal_transactions_import_duration_microseconds_sum,
    labels: [:data_type],
    help: "Cumulative Chain.import time for internal-tx batches (µs)"
  ]
  @counter [
    name: :internal_transactions_import_duration_microseconds_count,
    labels: [:data_type],
    help: "Number of Chain.import calls for internal-tx batches (success or failure)"
  ]
  @gauge [
    name: :internal_transactions_import_duration_microseconds_last,
    labels: [:data_type],
    help: "Most recent Chain.import duration for an internal-tx batch (µs)"
  ]

  # Phase-1 timing — HTTP round-trip only, no gunzip, no Jason.decode. Should be
  # much smaller than `internal_transactions_fetch_duration` (which includes
  # decode) on the Geth two-phase path.
  @counter [
    name: :internal_transactions_raw_fetch_duration_microseconds_sum,
    labels: [:data_type],
    help: "Cumulative raw HTTP fetch time, no gunzip/decode (µs)"
  ]
  @counter [
    name: :internal_transactions_raw_fetch_duration_microseconds_count,
    labels: [:data_type],
    help: "Number of raw HTTP fetch observations"
  ]
  @gauge [
    name: :internal_transactions_raw_fetch_duration_microseconds_last,
    labels: [:data_type],
    help: "Most recent raw HTTP fetch duration (µs)"
  ]

  # Phase-2 timing — gunzip + Jason.decode + trace flattening + transform.
  # Combined with raw_fetch above, you can tell whether time is spent on the
  # wire or in memory.
  @counter [
    name: :internal_transactions_decode_duration_microseconds_sum,
    labels: [:data_type],
    help: "Cumulative decode + transform time under heavy-stage gate (µs)"
  ]
  @counter [
    name: :internal_transactions_decode_duration_microseconds_count,
    labels: [:data_type],
    help: "Number of decode observations"
  ]
  @gauge [
    name: :internal_transactions_decode_duration_microseconds_last,
    labels: [:data_type],
    help: "Most recent decode + transform duration (µs)"
  ]

  # Time a BufferedTask task spends parked at HeavyStageGate before acquiring a
  # permit. Sustained nonzero values mean the gate is the bottleneck.
  @counter [
    name: :internal_transactions_heavy_gate_wait_duration_microseconds_sum,
    help: "Cumulative time spent waiting on the InternalTransaction heavy-stage permit (µs)"
  ]
  @counter [
    name: :internal_transactions_heavy_gate_wait_duration_microseconds_count,
    help: "Number of heavy-stage gate wait observations"
  ]
  @gauge [
    name: :internal_transactions_heavy_gate_wait_duration_microseconds_last,
    help: "Most recent heavy-stage gate wait duration (µs)"
  ]

  # Full duration of the work executed under one HeavyStageGate permit. The
  # gate wraps decode + Chain.import + bookkeeping, so this is decode_duration
  # PLUS everything downstream until the permit is released. The gap between
  # this and decode_duration is the unmeasured work the gate is actually
  # serializing — usually Chain.import time.
  @counter [
    name: :internal_transactions_gate_hold_duration_microseconds_sum,
    help: "Cumulative HeavyStageGate hold time across all permits (µs)"
  ]
  @counter [
    name: :internal_transactions_gate_hold_duration_microseconds_count,
    help: "Number of HeavyStageGate permit acquisitions that completed (success or failure)"
  ]
  @gauge [
    name: :internal_transactions_gate_hold_duration_microseconds_last,
    help: "Most recent HeavyStageGate permit hold duration (µs)"
  ]

  # Current number of decode/import slots in use. Plateaus at the configured
  # permit count when the gate is saturated.
  @gauge [
    name: :internal_transactions_heavy_gate_in_use,
    help: "Number of InternalTransaction heavy-stage permits currently held"
  ]

  # Bytes of compressed (or uncompressed if gzip disabled) raw response body
  # returned by debug_traceBlockByNumber. Useful for sizing the gate vs box
  # memory — multiply by permit count for a rough peak-memory estimate.
  @counter [
    name: :internal_transactions_raw_body_bytes_sum,
    help: "Cumulative raw body size returned by debug_traceBlockByNumber (bytes)"
  ]
  @counter [
    name: :internal_transactions_raw_body_bytes_count,
    help: "Number of raw body size observations (one per HTTP round-trip)"
  ]
  @gauge [
    name: :internal_transactions_raw_body_bytes_last,
    help: "Most recent raw body size from debug_traceBlockByNumber (bytes)"
  ]

  # In-memory snapshot of the BufferedTask's queue for the InternalTransaction
  # fetcher: total items (sum of current_buffer + current_front_buffer +
  # bound_queue) and the block_number range of those items. If size sits at
  # zero while pending_block_operations in the DB is non-empty, the
  # BufferedTask is starved — workers have no work even though the DB has
  # plenty (typically because `poll: false` and async_fetch isn't being
  # called).
  @gauge [
    name: :internal_transactions_buffer_queue_size,
    help: "Total items in Indexer.Fetcher.InternalTransaction BufferedTask's in-memory state"
  ]
  @gauge [
    name: :internal_transactions_buffer_block_min,
    help: "Lowest block_number held in the InternalTransaction BufferedTask's in-memory state (0 if empty)"
  ]
  @gauge [
    name: :internal_transactions_buffer_block_max,
    help: "Highest block_number held in the InternalTransaction BufferedTask's in-memory state (0 if empty)"
  ]

  # Highest block_number observed in any successfully imported internal-tx
  # batch since process start. Updates per-batch on the import success path,
  # so a flat line means imports have stopped.
  @gauge [
    name: :internal_transactions_last_indexed_block,
    help: "Highest block_number from a successfully imported internal-tx batch"
  ]

  @gauge [name: :delay_from_last_node_block, help: "Delay from the last block on the node in seconds"]

  @counter [name: :import_errors_count, help: "Number of database import errors"]

  @counter [
    name: :transactions_imported_count,
    labels: [:fetcher],
    help: "Number of transactions imported (use rate() for tx/sec)"
  ]

  @counter [
    name: :internal_transactions_imported_count,
    help: "Number of internal transactions imported (use rate() for itx/sec)"
  ]

  @counter [
    name: :logs_imported_count,
    labels: [:fetcher],
    help: "Number of logs imported (use rate() for logs/sec)"
  ]

  @counter [
    name: :blocks_imported_count,
    labels: [:fetcher],
    help: "Number of blocks fully imported with their transactions/logs (use rate() for blocks/sec)"
  ]

  @counter [
    name: :blocks_internal_transactions_indexed_count,
    help: "Number of blocks whose internal transactions have been fully indexed (use rate() for blocks/sec)"
  ]

  # block_range_100k label is the block_number / 100_000 bucket, which keeps
  # cardinality bounded (~max_block / 100k) while still pointing at the failing
  # region of the chain. data_type distinguishes block-level vs per-transaction
  # tracing. stage is :rpc_fetch (JSON-RPC call failed) or :db_import (chain
  # write failed after a successful fetch) — both leave blocks stuck in
  # pending_block_operations, so the heatmap sums across stages by default.
  @counter [
    name: :internal_transactions_indexing_errors_count,
    labels: [:block_range_100k, :data_type, :stage],
    help:
      "Blocks affected by internal-tx indexing errors, bucketed by 100k-block range and broken down by failure stage. " <>
        "Each failing batch increments by the count of unique blocks it contains, per their 100k bucket."
  ]

  @gauge [name: :memory_consumed, labels: [:fetcher], help: "Amount of memory consumed by fetchers (MB)"]

  @gauge [name: :latest_block_number, help: "Latest block number"]

  @gauge [name: :latest_block_timestamp, help: "Latest block timestamp"]

  # metrics of indexing monitor
  @gauge [name: :missing_blocks_count, help: "Number of blocks missing in the chain"]
  @gauge [
    name: :missing_internal_transactions_count,
    help: "Number of blocks with not yet fetched internal transactions"
  ]
  @gauge [name: :missing_current_token_balances_count, help: "Number of missing current token balances"]
  @gauge [name: :missing_archival_token_balances_count, help: "Number of missing token balances in history"]
  @gauge [name: :unfetched_token_instances_count, help: "Number of unfetched token instances"]
  @gauge [name: :failed_token_instances_metadata_count, help: "Number of failed token instances metadata"]
  @gauge [name: :token_instances_not_uploaded_to_cdn_count, help: "Token instances not uploaded to CDN"]
  @gauge [name: :multichain_search_db_main_export_queue_count, help: "Size of the main multichain export queue"]
  @gauge [name: :multichain_search_db_export_balances_queue_count, help: "Size of the balances export queue"]
  @gauge [name: :multichain_search_db_export_counters_queue_count, help: "Size of the counters export queue"]
  @gauge [name: :multichain_search_db_export_token_info_queue_count, help: "Size of the token info export queue"]

  @spec setup() :: :ok
  def setup do
    min_blockchain_block_number =
      RangesHelper.get_min_block_number_from_range_string(Application.get_env(:indexer, :block_ranges))

    set_latest_block_number(min_blockchain_block_number)
    set_latest_block_timestamp(0)

    if chain_type() in @rollups do
      set_latest_batch_number(0)
      set_latest_batch_timestamp(0)
    end

    :ok
  end

  # Every duration setter below writes the same trio: cumulative-µs counter,
  # count counter, latest-value gauge. Use rate(_sum)/rate(_count) for avg and
  # max_over_time(_last[…]) for a windowed max approximation. The _last gauge
  # is sampled at scrape time, so very brief outliers may not register —
  # acceptable trade-off in exchange for not depending on bucket boundaries.

  @doc "Records the full processing time of a block (µs)."
  @spec set_block_full_process(time :: integer(), fetcher :: atom()) :: :ok
  def set_block_full_process(time, fetcher) do
    labels = [fetcher]
    Counter.inc([name: :block_full_processing_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :block_full_processing_duration_microseconds_count, labels: labels)
    Gauge.set([name: :block_full_processing_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records block import time (µs)."
  @spec set_block_import(time :: float(), fetcher :: atom()) :: :ok
  def set_block_import(time, fetcher) do
    labels = [fetcher]
    Counter.inc([name: :block_import_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :block_import_duration_microseconds_count, labels: labels)
    Gauge.set([name: :block_import_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records block batch fetch request time (µs)."
  @spec set_block_batch_fetch(time :: integer(), fetcher :: atom()) :: :ok
  def set_block_batch_fetch(time, fetcher) do
    labels = [fetcher]
    Counter.inc([name: :block_batch_fetch_request_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :block_batch_fetch_request_duration_microseconds_count, labels: labels)
    Gauge.set([name: :block_batch_fetch_request_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records the internal-tx Chain.import time (µs) for the given data_type."
  @spec set_internal_transactions_import(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_import(time, data_type) do
    labels = [data_type]
    Counter.inc([name: :internal_transactions_import_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :internal_transactions_import_duration_microseconds_count, labels: labels)
    Gauge.set([name: :internal_transactions_import_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records the internal-tx raw HTTP fetch time (µs), phase 1 of the two-phase pipeline."
  @spec set_internal_transactions_raw_fetch(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_raw_fetch(time, data_type) do
    labels = [data_type]
    Counter.inc([name: :internal_transactions_raw_fetch_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :internal_transactions_raw_fetch_duration_microseconds_count, labels: labels)
    Gauge.set([name: :internal_transactions_raw_fetch_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records decode + transform time (µs) under the heavy-stage gate, phase 2 of the two-phase pipeline."
  @spec set_internal_transactions_decode(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_decode(time, data_type) do
    labels = [data_type]
    Counter.inc([name: :internal_transactions_decode_duration_microseconds_sum, labels: labels], time)
    Counter.inc(name: :internal_transactions_decode_duration_microseconds_count, labels: labels)
    Gauge.set([name: :internal_transactions_decode_duration_microseconds_last, labels: labels], time)
  end

  @doc "Records how long a BufferedTask task waited on the heavy-stage gate before acquiring a permit (µs)."
  @spec set_internal_transactions_heavy_gate_wait(time :: integer()) :: :ok
  def set_internal_transactions_heavy_gate_wait(time) do
    Counter.inc([name: :internal_transactions_heavy_gate_wait_duration_microseconds_sum], time)
    Counter.inc(name: :internal_transactions_heavy_gate_wait_duration_microseconds_count)
    Gauge.set([name: :internal_transactions_heavy_gate_wait_duration_microseconds_last], time)
  end

  @doc "Records the full duration that one HeavyStageGate permit was held (µs)."
  @spec set_internal_transactions_gate_hold(time :: integer()) :: :ok
  def set_internal_transactions_gate_hold(time) do
    Counter.inc([name: :internal_transactions_gate_hold_duration_microseconds_sum], time)
    Counter.inc(name: :internal_transactions_gate_hold_duration_microseconds_count)
    Gauge.set([name: :internal_transactions_gate_hold_duration_microseconds_last], time)
  end

  @doc """
  Sets the gauge of heavy-stage gate permits currently in use.
  """
  @spec set_internal_transactions_heavy_gate_in_use(count :: non_neg_integer()) :: :ok
  def set_internal_transactions_heavy_gate_in_use(count) do
    Gauge.set([name: :internal_transactions_heavy_gate_in_use], count)
  end

  @doc """
  Records the size in bytes of a raw debug_traceBlockByNumber HTTP response
  body. Observed once per HTTP round-trip in the raw fetch phase.
  """
  @spec observe_internal_transactions_raw_body_bytes(bytes :: non_neg_integer()) :: :ok
  def observe_internal_transactions_raw_body_bytes(bytes) do
    Counter.inc([name: :internal_transactions_raw_body_bytes_sum], bytes)
    Counter.inc(name: :internal_transactions_raw_body_bytes_count)
    Gauge.set([name: :internal_transactions_raw_body_bytes_last], bytes)
  end

  @doc """
  Sets the total in-memory item count of the InternalTransaction BufferedTask.
  """
  @spec set_internal_transactions_buffer_queue_size(non_neg_integer()) :: :ok
  def set_internal_transactions_buffer_queue_size(value) do
    Gauge.set([name: :internal_transactions_buffer_queue_size], value)
  end

  @doc """
  Sets the lowest block_number currently held in the InternalTransaction
  BufferedTask's in-memory state.
  """
  @spec set_internal_transactions_buffer_block_min(non_neg_integer()) :: :ok
  def set_internal_transactions_buffer_block_min(value) do
    Gauge.set([name: :internal_transactions_buffer_block_min], value)
  end

  @doc """
  Sets the highest block_number currently held in the InternalTransaction
  BufferedTask's in-memory state.
  """
  @spec set_internal_transactions_buffer_block_max(non_neg_integer()) :: :ok
  def set_internal_transactions_buffer_block_max(value) do
    Gauge.set([name: :internal_transactions_buffer_block_max], value)
  end

  @doc """
  Sets the highest block_number from the most recent successful internal-tx import batch.
  """
  @spec set_internal_transactions_last_indexed_block(non_neg_integer()) :: :ok
  def set_internal_transactions_last_indexed_block(value) do
    Gauge.set([name: :internal_transactions_last_indexed_block], value)
  end

  @doc """
  Defines the metric for JSON-RPC node response delay (in seconds) during block import.
  """
  @spec set_json_rpc_node_delay(delay :: integer()) :: :ok
  def set_json_rpc_node_delay(delay) do
    Gauge.set([name: :delay_from_last_node_block], delay)
  end

  @doc """
  Defines the metric for the number of import errors encountered during block processing.
  """
  @spec set_import_errors_count(error_count :: integer()) :: :ok
  def set_import_errors_count(error_count \\ 1) do
    Counter.inc([name: :import_errors_count], error_count)
  end

  @doc """
  Increments the counter of imported transactions by `count`.
  """
  @spec inc_transactions_imported(count :: non_neg_integer(), fetcher :: atom()) :: :ok
  def inc_transactions_imported(0, _fetcher), do: :ok

  def inc_transactions_imported(count, fetcher) do
    Counter.inc([name: :transactions_imported_count, labels: [fetcher]], count)
  end

  @doc """
  Increments the counter of imported internal transactions by `count`.
  """
  @spec inc_internal_transactions_imported(count :: non_neg_integer()) :: :ok
  def inc_internal_transactions_imported(0), do: :ok

  def inc_internal_transactions_imported(count) do
    Counter.inc([name: :internal_transactions_imported_count], count)
  end

  @doc """
  Increments the counter of imported logs by `count`.
  """
  @spec inc_logs_imported(count :: non_neg_integer(), fetcher :: atom()) :: :ok
  def inc_logs_imported(0, _fetcher), do: :ok

  def inc_logs_imported(count, fetcher) do
    Counter.inc([name: :logs_imported_count, labels: [fetcher]], count)
  end

  @doc """
  Increments the counter of fully-imported blocks by `count`.
  """
  @spec inc_blocks_imported(count :: non_neg_integer(), fetcher :: atom()) :: :ok
  def inc_blocks_imported(0, _fetcher), do: :ok

  def inc_blocks_imported(count, fetcher) do
    Counter.inc([name: :blocks_imported_count, labels: [fetcher]], count)
  end

  @doc """
  Increments the counter of blocks whose internal transactions have been fully indexed by `count`.
  """
  @spec inc_blocks_internal_transactions_indexed(count :: non_neg_integer()) :: :ok
  def inc_blocks_internal_transactions_indexed(0), do: :ok

  def inc_blocks_internal_transactions_indexed(count) do
    Counter.inc([name: :blocks_internal_transactions_indexed_count], count)
  end

  @doc """
  Increments the internal-tx indexing-error counter by `count` for the given 100k-block bucket, data_type, and stage.
  """
  @spec inc_internal_transactions_indexing_errors(
          count :: non_neg_integer(),
          block_range_100k :: integer(),
          data_type :: atom(),
          stage :: atom()
        ) :: :ok
  def inc_internal_transactions_indexing_errors(0, _block_range_100k, _data_type, _stage), do: :ok

  def inc_internal_transactions_indexing_errors(count, block_range_100k, data_type, stage) do
    Counter.inc(
      [
        name: :internal_transactions_indexing_errors_count,
        labels: [block_range_100k, data_type, stage]
      ],
      count
    )
  end

  @doc """
  Defines the metric for memory consumed by a specific fetcher (in MB).
  """
  @spec set_memory_consumed(fetcher :: nil | atom() | String.t(), memory :: float()) :: :ok
  def set_memory_consumed(nil, _memory), do: :ok

  def set_memory_consumed(fetcher, memory) do
    Gauge.set([name: :memory_consumed, labels: [fetcher]], memory)
  end

  @spec set_latest_block_number(number :: integer()) :: :ok
  defp set_latest_block_number(number) do
    Gauge.set([name: :latest_block_number], number)
  end

  @spec set_latest_block_timestamp(timestamp :: integer()) :: :ok
  defp set_latest_block_timestamp(timestamp) do
    Gauge.set([name: :latest_block_timestamp], timestamp)
  end

  @doc """
  Generates the latest block number and timestamp Prometheus metrics.

  ## Parameters

    - `number`: The block number to set.
    - `timestamp`: The timestamp of the block as a `DateTime` struct.
  """
  @spec set_latest_block(number :: integer, timestamp :: DateTime.t()) :: :ok
  def set_latest_block(number, timestamp) do
    set_latest_block_number(number)
    set_latest_block_timestamp(DateTime.to_unix(timestamp))
  end

  @gauge [name: :latest_batch_number, help: "L2 latest batch number"]

  @gauge [name: :latest_batch_timestamp, help: "L2 latest batch timestamp"]

  defp set_latest_batch_number(number) do
    Gauge.set([name: :latest_batch_number], number)
  end

  defp set_latest_batch_timestamp(timestamp) do
    Gauge.set([name: :latest_batch_timestamp], timestamp)
  end

  @doc """
  Generates the latest batch number and timestamp Prometheus metrics.

  ## Parameters

    - `number`: The batch number to set.
    - `timestamp`: The timestamp of the batch as a `DateTime` struct.
  """
  @spec set_latest_batch(number :: integer, timestamp :: DateTime.t()) :: :ok
  def set_latest_batch(number, timestamp) do
    if chain_type() in @rollups do
      set_latest_batch_number(number)
      set_latest_batch_timestamp(DateTime.to_unix(timestamp))
    else
      :ok
    end
  end

  @doc """
  Defines the metric for the number of blocks missing in the chain.
  """
  @spec missing_blocks_count(integer()) :: :ok
  def missing_blocks_count(value), do: Gauge.set([name: :missing_blocks_count], value)

  @doc """
  Defines the metric for the number of blocks with not yet fetched internal transactions.
  """
  @spec missing_internal_transactions_count(integer()) :: :ok
  def missing_internal_transactions_count(value), do: Gauge.set([name: :missing_internal_transactions_count], value)

  @doc """
  Defines the metric for the number of missing current token balances.
  """
  @spec missing_current_token_balances_count(integer()) :: :ok
  def missing_current_token_balances_count(value),
    do: Gauge.set([name: :missing_current_token_balances_count], value)

  @doc """
  Defines the metric for the number of missing token balances in history.
  """
  @spec missing_archival_token_balances_count(integer()) :: :ok
  def missing_archival_token_balances_count(value), do: Gauge.set([name: :missing_archival_token_balances_count], value)

  @doc """
  Defines the metric for the number of unfetched token instances.
  """
  @spec unfetched_token_instances_count(integer()) :: :ok
  def unfetched_token_instances_count(value),
    do: Gauge.set([name: :unfetched_token_instances_count], value)

  @doc """
  Defines the metric for the number of failed token instances metadata.
  """
  @spec failed_token_instances_metadata_count(integer()) :: :ok
  def failed_token_instances_metadata_count(value),
    do: Gauge.set([name: :failed_token_instances_metadata_count], value)

  @doc """
  Defines the metric for the number of token instances not uploaded to CDN.
  """
  @spec token_instances_not_uploaded_to_cdn_count(integer()) :: :ok
  def token_instances_not_uploaded_to_cdn_count(value),
    do: Gauge.set([name: :token_instances_not_uploaded_to_cdn_count], value)

  @doc """
  Defines the metric for the size of the main multichain export queue.
  """
  @spec multichain_search_db_main_export_queue_count(integer()) :: :ok
  def multichain_search_db_main_export_queue_count(value),
    do: Gauge.set([name: :multichain_search_db_main_export_queue_count], value)

  @doc """
  Defines the metric for the size of the balances export queue.
  """
  @spec multichain_search_db_export_balances_queue_count(integer()) :: :ok
  def multichain_search_db_export_balances_queue_count(value),
    do: Gauge.set([name: :multichain_search_db_export_balances_queue_count], value)

  @doc """
  Defines the metric for the size of the counters export queue.
  """
  @spec multichain_search_db_export_counters_queue_count(integer()) :: :ok
  def multichain_search_db_export_counters_queue_count(value),
    do: Gauge.set([name: :multichain_search_db_export_counters_queue_count], value)

  @doc """
  Defines the metric for the size of the token info export queue.
  """
  @spec multichain_search_db_export_token_info_queue_count(integer()) :: :ok
  def multichain_search_db_export_token_info_queue_count(value),
    do: Gauge.set([name: :multichain_search_db_export_token_info_queue_count], value)
end

defmodule Indexer.Prometheus.Instrumenter do
  @moduledoc """
  Blockchain data fetch and import metrics for `Prometheus`.
  """

  use Prometheus.Metric
  use Utils.RuntimeEnvHelper, chain_type: [:explorer, :chain_type]

  alias EthereumJSONRPC.Utility.RangesHelper

  @rollups [:arbitrum, :zksync, :optimism, :polygon_zkevm, :scroll]

  @histogram [
    name: :block_full_processing_duration_microseconds,
    labels: [:fetcher],
    buckets: [1000, 5000, 10000, 100_000],
    duration_unit: :microseconds,
    help: "Block whole processing time including fetch and import"
  ]

  @histogram [
    name: :block_import_duration_microseconds,
    labels: [:fetcher],
    buckets: [1000, 5000, 10000, 100_000],
    duration_unit: :microseconds,
    help: "Block import time"
  ]

  @histogram [
    name: :block_batch_fetch_request_duration_microseconds,
    labels: [:fetcher],
    buckets: [1000, 5000, 10000, 100_000],
    duration_unit: :microseconds,
    help: "Block fetch batch request processing time"
  ]

  # Wider buckets than the block-fetcher histograms because trace_block / debug_trace
  # routinely take 100ms-seconds rather than tens of ms. data_type lets you see whether
  # block-level vs per-transaction tracing has different latency profiles on this node.
  @histogram [
    name: :internal_transactions_fetch_duration_microseconds,
    labels: [:data_type],
    buckets: [10_000, 100_000, 1_000_000, 10_000_000],
    duration_unit: :microseconds,
    help: "Internal transactions JSON-RPC fetch time (one observation per BufferedTask batch, success or failure)"
  ]

  @histogram [
    name: :internal_transactions_import_duration_microseconds,
    labels: [:data_type],
    buckets: [10_000, 100_000, 1_000_000, 10_000_000],
    duration_unit: :microseconds,
    help: "Internal transactions Chain.import time (one observation per batch, success or failure)"
  ]

  # Phase-1 timing — HTTP round-trip only, no gunzip, no Jason.decode. Should be
  # much smaller than the legacy `internal_transactions_fetch_duration` because
  # we no longer pay decode cost in this stage. data_type = :block_number for
  # the Geth two-phase path; other variants still use the single-phase fetch
  # and are recorded under `internal_transactions_fetch_duration` instead.
  @histogram [
    name: :internal_transactions_raw_fetch_duration_microseconds,
    labels: [:data_type],
    buckets: [10_000, 100_000, 1_000_000, 10_000_000],
    duration_unit: :microseconds,
    help: "Internal transactions raw HTTP fetch time (no gunzip/decode), one observation per batch"
  ]

  # Phase-2 timing — gunzip + Jason.decode + trace flattening + transform.
  # Combined with raw_fetch above, you can tell whether time is spent on the
  # wire or in memory.
  @histogram [
    name: :internal_transactions_decode_duration_microseconds,
    labels: [:data_type],
    buckets: [10_000, 100_000, 1_000_000, 10_000_000],
    duration_unit: :microseconds,
    help: "Internal transactions decode + transform time under heavy-stage gate, one observation per batch"
  ]

  # Time a BufferedTask task spends parked at HeavyStageGate before acquiring a
  # permit. Sustained nonzero values mean the gate is the bottleneck and you
  # may want to raise INDEXER_INTERNAL_TRANSACTIONS_HEAVY_PERMITS — at the cost
  # of higher peak memory.
  @histogram [
    name: :internal_transactions_heavy_gate_wait_duration_microseconds,
    buckets: [1_000, 100_000, 1_000_000, 10_000_000, 60_000_000],
    duration_unit: :microseconds,
    help: "Time spent waiting on the InternalTransaction heavy-stage permit"
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
  @histogram [
    name: :internal_transactions_raw_body_bytes,
    buckets: [100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000],
    help: "Raw body size returned by debug_traceBlockByNumber (bytes), one observation per HTTP round-trip"
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

  @doc """
  Defines the metric for the full processing time of a block (in microseconds).
  """
  @spec set_block_full_process(time :: integer(), fetcher :: atom()) :: :ok
  def set_block_full_process(time, fetcher) do
    Histogram.observe([name: :block_full_processing_duration_microseconds, labels: [fetcher]], time)
  end

  @doc """
  Defines the metric for the import time of a block (in microseconds).
  """
  @spec set_block_import(time :: float(), fetcher :: atom()) :: :ok
  def set_block_import(time, fetcher) do
    Histogram.observe([name: :block_import_duration_microseconds, labels: [fetcher]], time)
  end

  @doc """
  Defines the metric for the block batch fetch request time (in microseconds).
  """
  @spec set_block_batch_fetch(time :: integer(), fetcher :: atom()) :: :ok
  def set_block_batch_fetch(time, fetcher) do
    Histogram.observe([name: :block_batch_fetch_request_duration_microseconds, labels: [fetcher]], time)
  end

  @doc """
  Records the internal-tx JSON-RPC fetch time (in microseconds) for the given data_type.
  """
  @spec set_internal_transactions_fetch(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_fetch(time, data_type) do
    Histogram.observe([name: :internal_transactions_fetch_duration_microseconds, labels: [data_type]], time)
  end

  @doc """
  Records the internal-tx Chain.import time (in microseconds) for the given data_type.
  """
  @spec set_internal_transactions_import(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_import(time, data_type) do
    Histogram.observe([name: :internal_transactions_import_duration_microseconds, labels: [data_type]], time)
  end

  @doc """
  Records the internal-tx raw HTTP fetch time (in microseconds), phase 1 of
  the two-phase pipeline. No gunzip, no Jason.decode included.
  """
  @spec set_internal_transactions_raw_fetch(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_raw_fetch(time, data_type) do
    Histogram.observe([name: :internal_transactions_raw_fetch_duration_microseconds, labels: [data_type]], time)
  end

  @doc """
  Records the internal-tx decode + transform time (in microseconds), phase 2
  of the two-phase pipeline. Runs under the heavy-stage gate.
  """
  @spec set_internal_transactions_decode(time :: integer(), data_type :: atom()) :: :ok
  def set_internal_transactions_decode(time, data_type) do
    Histogram.observe([name: :internal_transactions_decode_duration_microseconds, labels: [data_type]], time)
  end

  @doc """
  Records how long a BufferedTask task waited on the heavy-stage gate before
  acquiring a permit (in microseconds).
  """
  @spec set_internal_transactions_heavy_gate_wait(time :: integer()) :: :ok
  def set_internal_transactions_heavy_gate_wait(time) do
    Histogram.observe([name: :internal_transactions_heavy_gate_wait_duration_microseconds], time)
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
    Histogram.observe([name: :internal_transactions_raw_body_bytes], bytes)
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

defmodule Indexer.Fetcher.InternalTransaction.HeavyStageGate do
  @moduledoc """
  Counting semaphore that caps how many `Indexer.Fetcher.InternalTransaction`
  tasks may simultaneously hold a decoded `debug_traceBlockByNumber` response in
  memory.

  RPC fetch concurrency stays high (network-bound); decode + import concurrency
  is capped at `:permits` (memory-bound). Tasks block on `with_permit/1` while
  their compressed body sits in memory — small enough that parking dozens of
  tasks is fine when `ETHEREUM_JSONRPC_HTTP_GZIP_ENABLED=true`.

      config :indexer, Indexer.Fetcher.InternalTransaction.HeavyStageGate,
        permits: 2
  """

  use GenServer

  alias Indexer.Prometheus.Instrumenter

  @default_permits 2

  @doc """
  Runs `fun` while holding a permit. Blocks (`:infinity`) until a permit is
  available; releases on the way out — even if `fun` raises or exits — via
  `try/after` (best case) or the process monitor we attach at grant time
  (fallback if the worker is killed mid-flight).
  """
  @spec with_permit((-> result)) :: result when result: var
  def with_permit(fun) when is_function(fun, 0) do
    wait_started_at = System.monotonic_time()
    {:ok, ref} = GenServer.call(__MODULE__, :acquire, :infinity)
    wait_us = System.convert_time_unit(System.monotonic_time() - wait_started_at, :native, :microsecond)
    Instrumenter.set_internal_transactions_heavy_gate_wait(wait_us)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, ref})
    end
  end

  # Supervision plumbing

  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :worker}

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl GenServer
  def init(_opts) do
    permits =
      case Application.get_env(:indexer, __MODULE__, [])[:permits] do
        n when is_integer(n) and n > 0 -> n
        _ -> @default_permits
      end

    state = %{permits: permits, available: permits, waiting: :queue.new(), holders: %{}}
    set_in_use_metric(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:acquire, from, %{available: a} = state) when a > 0 do
    ref = grant(from)
    {:reply, {:ok, ref}, %{state | available: a - 1, holders: Map.put(state.holders, ref, :ok)}}
  end

  def handle_call(:acquire, from, %{available: 0} = state) do
    # Park caller until a holder releases. We deliberately do not monitor the
    # waiter — a reply to a dead pid is a no-op, and the monitor we attach at
    # grant time will fire :DOWN and release the permit if the (now) holder
    # was dead at grant.
    {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
  end

  @impl GenServer
  def handle_cast({:release, ref}, state), do: {:noreply, release(ref, state)}

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state), do: {:noreply, release(ref, state)}

  # Internal

  # Hand the just-freed permit to the next waiter if any, otherwise bump
  # `available`. Stray releases (e.g. duplicate :DOWN after demonitor flush)
  # are absorbed by the `nil` branch.
  defp release(ref, %{holders: holders, waiting: waiting, available: a} = state) do
    case Map.pop(holders, ref) do
      {nil, ^holders} ->
        state

      {:ok, new_holders} ->
        Process.demonitor(ref, [:flush])

        case :queue.out(waiting) do
          {{:value, from}, new_waiting} ->
            new_ref = grant(from)
            new_state = %{state | holders: Map.put(new_holders, new_ref, :ok), waiting: new_waiting}
            GenServer.reply(from, {:ok, new_ref})
            set_in_use_metric(new_state)
            new_state

          {:empty, _} ->
            new_state = %{state | holders: new_holders, available: a + 1}
            set_in_use_metric(new_state)
            new_state
        end
    end
  end

  defp grant({pid, _tag}), do: Process.monitor(pid)

  defp set_in_use_metric(%{permits: permits, available: available}),
    do: Instrumenter.set_internal_transactions_heavy_gate_in_use(permits - available)
end

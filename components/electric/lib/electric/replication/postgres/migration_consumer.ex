defmodule Electric.Replication.Postgres.MigrationConsumer do
  @moduledoc """
  Holds state information about a postgres db instance, stored in tables within the db itself.
  """
  use GenStage

  import Electric.Postgres.Extension,
    only: [is_ddl_relation: 1, is_extension_relation: 1, is_perms_relation: 1]

  alias Electric.Postgres.{
    Extension,
    Extension.SchemaLoader,
    Extension.SchemaCache,
    OidDatabase,
    Schema
  }

  alias Electric.Replication.Changes.{NewRecord, Transaction}
  alias Electric.Replication.Connectors
  alias Electric.Replication.Postgres.Client

  alias Electric.Telemetry.Metrics

  require Logger

  @spec name(Connectors.config()) :: Electric.reg_name()
  def name(conn_config) when is_list(conn_config) do
    name(Connectors.origin(conn_config))
  end

  @spec name(Connectors.origin()) :: Electric.reg_name()
  def name(origin) when is_binary(origin) do
    Electric.name(__MODULE__, origin)
  end

  def start_link({conn_config, opts}) do
    start_link(conn_config, opts)
  end

  def start_link(conn_config, opts \\ []) do
    GenStage.start_link(__MODULE__, {conn_config, opts}, name: name(conn_config))
  end

  @impl GenStage
  def init({conn_config, opts}) do
    origin = Connectors.origin(conn_config)

    %{publication: publication, subscription: subscription} =
      Connectors.get_replication_opts(conn_config)

    Logger.metadata(pg_producer: origin)

    {:ok, producer} = Keyword.fetch(opts, :producer)

    :ok = await_producer(producer)

    {:ok, loader} =
      opts
      |> SchemaLoader.get(:backend, SchemaCache)
      |> SchemaLoader.connect(conn_config)

    refresh_sub? = Keyword.get(opts, :refresh_subscription, true)

    Logger.info("Starting #{__MODULE__} using #{elem(loader, 0)} backend")

    state = %{
      origin: origin,
      publication: publication,
      subscription: subscription,
      producer: producer,
      loader: loader,
      opts: opts,
      refresh_subscription: refresh_sub?,
      refresh_enum_types: Keyword.get(opts, :refresh_enum_types, true),
      conn_opts: Connectors.get_connection_opts(conn_config)
    }

    {:producer_consumer, state}
  end

  @impl GenStage
  def handle_info({:subscribe, pid}, state) do
    subscribe_producer(pid)

    {:noreply, [], state}
  end

  def handle_info({:gproc, _, :registered, {_stage, pid, _}}, state) do
    subscribe_producer(pid)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_events(transactions, _from, state) do
    {:noreply, filter_transactions(transactions), process_migrations(transactions, state)}
  end

  defp filter_transactions(transactions) do
    Enum.map(transactions, &filter_transaction/1)
  end

  # FIXME: we need this to prevent extension metadata tables from being
  # replicated between pg instances. Should be removed once we're only
  # replicating a subset of tables, rather than all
  defp filter_transaction(%Transaction{changes: changes} = tx) do
    filtered =
      Enum.filter(changes, fn
        %{relation: relation} when is_ddl_relation(relation) ->
          true

        %{relation: relation} when is_perms_relation(relation) ->
          false

        %{relation: relation} = change when is_extension_relation(relation) ->
          Logger.debug("---- Filtering #{inspect(change)}")
          false

        # TODO: VAX-680 remove this special casing of schema_migrations table
        # once we are selectivley replicating tables
        %{relation: {"public", "schema_migrations"}} ->
          false

        _change ->
          true
      end)

    %{tx | changes: filtered}
  end

  defp filter_transaction(change) do
    change
  end

  defp process_migrations(transactions, state) do
    {state, num_applied_migrations} =
      transactions
      |> Enum.flat_map(&transaction_changes_to_migrations(&1, state))
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.reduce({state, 0}, fn migration, {state, num_applied} ->
        {perform_migration(migration, state), num_applied + 1}
      end)

    if num_applied_migrations > 0 do
      refresh_subscription(state)
    else
      state
    end
  end

  defp transaction_changes_to_migrations(%Transaction{changes: changes}, state) do
    for %NewRecord{record: record, relation: relation} <- changes, is_ddl_relation(relation) do
      {:ok, version} = SchemaLoader.tx_version(state.loader, record)
      {:ok, sql} = Extension.extract_ddl_sql(record)
      {version, sql}
    end
  end

  defp perform_migration({version, stmts}, state) do
    {:ok, loader, schema_version} = apply_migration(version, stmts, state)

    Metrics.non_span_event(
      [:postgres, :migration],
      %{electrified_tables: Schema.num_electrified_tables(schema_version.schema)},
      %{migration_version: version}
    )

    %{state | loader: loader}
  end

  # update the subscription to add any new
  # tables (this only works when data has been added -- doing it at the
  # point of receiving the migration has no effect).
  defp refresh_subscription(%{refresh_subscription: refresh?} = state) do
    if refresh? do
      Logger.debug("#{__MODULE__} refreshing subscription '#{state.subscription}'")
      :ok = SchemaLoader.refresh_subscription(state.loader, state.subscription)
    end

    state
  end

  @impl GenStage
  def handle_cancel({:down, _}, _from, %{producer: producer} = state) do
    Logger.warning("producer is down: #{inspect(producer)}")
    await_producer(producer)
    {:noreply, [], state}
  end

  defp await_producer(producer) when is_pid(producer) do
    send(self(), {:subscribe, producer})
    :ok
  end

  defp await_producer({:via, :gproc, name}) do
    :gproc.nb_wait(name)
    :ok
  end

  defp subscribe_producer(producer) when is_pid(producer) do
    Logger.debug("request subscription to #{inspect(producer)}")

    :ok =
      GenStage.async_subscribe(self(),
        to: producer,
        cancel: :temporary,
        min_demand: 10,
        max_demand: 50
      )
  end

  @doc """
  Apply a migration, composed of a version and a list of DDL statements, to a schema
  using the given implementation of SchemaLoader.
  """
  @spec apply_migration(String.t(), [String.t()], map) ::
          {:ok, SchemaLoader.t(), SchemaLoader.Version.t()} | {:error, term()}
  def apply_migration(version, stmts, %{loader: loader} = state) when is_list(stmts) do
    {:ok, %{schema: schema} = schema_version} = SchemaLoader.load(loader)

    Logger.info("Migrating version #{schema_version.version || "<nil>"} -> #{version}")

    oid_loader = &SchemaLoader.relation_oid(loader, &1, &2, &3)

    old_enums = schema.enums

    schema =
      Enum.reduce(stmts, schema_version.schema, fn stmt, schema ->
        Logger.info("Applying migration #{version}: #{inspect(stmt)}")
        Schema.update(schema, stmt, oid_loader: oid_loader)
      end)
      |> Schema.add_shadow_tables(oid_loader: oid_loader)

    Logger.info("Saving schema version #{version} /#{inspect(loader)}/")

    {:ok, loader, schema_version} = SchemaLoader.save(loader, version, schema, stmts)

    if state.refresh_enum_types and schema.enums != old_enums do
      Client.with_conn(state.conn_opts, fn conn -> OidDatabase.update_oids(conn, [:ENUM]) end)
    end

    {:ok, loader, schema_version}
  end
end

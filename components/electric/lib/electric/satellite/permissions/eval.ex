defmodule Electric.Satellite.Permissions.Eval do
  alias Electric.Satellite.Auth

  alias Electric.Replication.Changes
  alias Electric.Replication.Eval
  alias Electric.Replication.Eval.Env
  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Parser.{Func, Ref}
  alias Electric.Replication.Eval.Runner

  alias Electric.Postgres.Extension.SchemaLoader
  require Record

  # allow for where clauses to refer to the current row as `ROW` or `THIS`
  @this ["this", "row"]
  @valid_ops [:insert, :delete, :update]
  @prefixes ~w(this row new old)
  @base_auth_refs %{
    ["auth", "user_id"] => :text
  }

  defstruct [:auth, tables: %{}]

  @type t() :: %__MODULE__{
          auth: Auth.t(),
          tables: %{Electric.Postgres.relation() => %{Electric.Postgres.name() => Env.pg_type()}}
        }

  defmodule ExpressionContext do
    defstruct [:auth, :relation, :columns, :expr]

    @type t() :: %__MODULE__{
            auth: Runner.val_map(),
            relation: Electric.Postgres.relation(),
            columns: %{Electric.Postgres.name() => Env.pg_type()},
            expr: %{
              insert: Eval.Expr.t(),
              delete: Eval.Expr.t(),
              update: Eval.Expr.t()
            }
          }
  end

  def new(%SchemaLoader.Version{} = schema_version, %Auth{} = auth) do
    Enum.reduce(schema_version.tables, %__MODULE__{auth: auth}, fn {relation, table_schema},
                                                                   eval ->
      columns =
        Map.new(table_schema.columns, fn column ->
          {column.name, String.to_atom(column.type.name)}
        end)

      Map.update!(eval, :tables, &Map.put(&1, relation, columns))
    end)
  end

  @doc """
  Permissions where clauses are always defined for a specific table.

  This pre-compiles the given expression for the given table using the table column type
  information.

  Because of the expansion of `ROW` (and `THIS`) expressions depending on the operation (`UPDATE`,
  `DELETE` etc) this compilation is done per operation and the resulting expressions stored in a
  lookup table.
  """
  def expression_context(evaluator, query, {_, _} = table) do
    with {:ok, refs} <- refs(evaluator, table),
         {:ok, expr} <- Parser.parse_and_validate_expression(query, refs),
         expr_cxt = new_expression_context(evaluator, table) do
      {:ok, struct(expr_cxt, expr: expand_row_aliases(expr))}
    end
  end

  def execute(
        %ExpressionContext{relation: rel} = expr_cxt,
        %Changes.UpdatedRecord{relation: rel} = change
      ) do
    values =
      Map.new(
        Enum.concat(
          Enum.map(change.record, fn {k, v} -> {["new", k], v} end),
          Enum.map(change.old_record, fn {k, v} -> {["old", k], v} end)
        )
      )

    execute_expr(expr_cxt, :update, values)
  end

  def execute(
        %ExpressionContext{relation: rel} = expr_cxt,
        %Changes.NewRecord{relation: rel} = change
      ) do
    values =
      Map.new(change.record, fn {k, v} -> {["new", k], v} end)

    execute_expr(expr_cxt, :insert, values)
  end

  def execute(
        %ExpressionContext{relation: rel} = expr_cxt,
        %Changes.DeletedRecord{relation: rel} = change
      ) do
    values =
      Map.new(change.old_record, fn {k, v} -> {["old", k], v} end)

    execute_expr(expr_cxt, :delete, values)
  end

  defp execute_expr(expr_cxt, op, values) do
    expr = Map.fetch!(expr_cxt.expr, op)
    values = Map.merge(values, expr_cxt.auth)
    Runner.execute(expr, values)
  end

  defp new_expression_context(%__MODULE__{auth: auth, tables: tables}, table) do
    struct(ExpressionContext,
      auth: %{["auth", "user_id"] => auth.user_id},
      relation: table,
      columns: Map.fetch!(tables, table)
    )
  end

  defp refs(%__MODULE__{} = evaluator, table) do
    with {:ok, table_refs} <- table_refs(evaluator, table),
         {:ok, auth_refs} <- auth_refs(evaluator) do
      {:ok, Map.merge(table_refs, auth_refs)}
    end
  end

  defp auth_refs(%__MODULE__{auth: %Auth{}} = _auth) do
    # TODO: add types of any claims in the auth struct
    {:ok, @base_auth_refs}
  end

  defp table_refs(%__MODULE__{tables: tables}, table) do
    with {:ok, table_columns} <- Map.fetch(tables, table) do
      refs =
        Enum.reduce(@prefixes, %{}, fn prefix, env ->
          Enum.reduce(table_columns, env, fn {column, type}, env ->
            Map.put(env, [prefix, column], type)
          end)
        end)

      {:ok, refs}
    end
  end

  def expand_row_aliases(expr) do
    Map.new(@valid_ops, fn op ->
      {op, expand_row_aliases(expr, op)}
    end)
  end

  def expand_row_aliases(%Eval.Expr{eval: ast, returns: :bool} = expr, action) do
    %{expr | eval: expand_expr(ast, alias_expansion(action))}
  end

  defp expand_expr(expr, mapping) do
    if uses_alias?(expr) do
      expand_references(expr, mapping)
    else
      expr
    end
  end

  defp expand_references(expr, [n]) do
    replace_alias(expr, n)
  end

  defp expand_references(expr, [n1, n2]) do
    %Func{
      args: [replace_alias(expr, n1), replace_alias(expr, n2)],
      type: :bool,
      name: "and",
      location: expr.location,
      implementation: &Kernel.and/2
    }
  end

  defp replace_alias(args, pre) when is_list(args) do
    Enum.map(args, &replace_alias(&1, pre))
  end

  defp replace_alias(%Ref{path: [this | rest]} = ref, base) when this in @this do
    %{ref | path: [base | rest]}
  end

  defp replace_alias(%Ref{} = ref, _pre) do
    ref
  end

  defp replace_alias(%Func{} = func, pre) do
    %{func | args: replace_alias(func.args, pre)}
  end

  defp uses_alias?(%Func{args: args}), do: Enum.any?(args, &uses_alias?/1)
  defp uses_alias?(%Ref{path: [this | _rest]}), do: this in @this
  defp uses_alias?(_), do: false

  defp alias_expansion(:update), do: ["new", "old"]
  defp alias_expansion(:delete), do: ["old"]
  defp alias_expansion(:insert), do: ["new"]
end

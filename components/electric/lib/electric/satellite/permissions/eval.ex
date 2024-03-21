defmodule Electric.Satellite.Permissions.Eval do
  alias Electric.Replication.Eval
  alias Electric.Replication.Eval.Parser.{Func, Ref}

  # allow for where clauses to refer to the current row as `ROW` or `THIS`
  @this ["this", "row"]

  # selects just use `this` or `row` directly
  def expand_row_aliases(expr, :select) do
    expr
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

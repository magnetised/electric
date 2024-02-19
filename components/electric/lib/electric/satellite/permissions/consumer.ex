defmodule Electric.Satellite.Permissions.Consumer do
  alias Electric.DDLX.Command
  alias Electric.Satellite.SatPerms
  alias Electric.Postgres.Extension.SchemaLoader
  alias Electric.Postgres.NameParser
  alias Electric.Replication.Changes
  alias Electric.Postgres.Extension

  @electric_roles Extension.roles_relation()
  @electric_ddlx Extension.ddlx_relation()

  def update(%Changes.Transaction{changes: changes} = tx, loader) do
    # group changes by relation -- this is really only to avoid churn on the global permissions
    # rules which is an expensive operation. by grouping on the relation we can transform a series
    # of ddlx permission commands into a single update to the global permissions struct
    {changes, loader} =
      changes
      |> Enum.chunk_by(& &1.relation)
      |> Enum.flat_map_reduce(loader, &apply_changes/2)

    {:ok, %{tx | changes: changes}, loader}
  end

  def update_global(%SatPerms.DDLX{} = ddlx, loader) do
    with {:ok, rules} <- SchemaLoader.global_permissions(loader) do
      case mutate_global(ddlx, rules) do
        {rules, 0} ->
          {:ok, 0, loader, rules}

        {rules, n} ->
          with {:ok, loader} <- SchemaLoader.save_global_permissions(loader, rules) do
            {:ok, n, loader, rules}
          end
      end
    end
  end

  defp apply_changes([%{relation: @electric_ddlx} | _] = changes, loader) do
    {:ok, rules} = SchemaLoader.global_permissions(loader)

    case Enum.reduce(changes, {rules, 0}, &apply_global_change/2) do
      {_rules, 0} ->
        {[], loader}

      {rules, _count} ->
        {:ok, loader} = SchemaLoader.save_global_permissions(loader, rules)

        {
          [updated_global_permissions(rules)],
          loader
        }
    end
  end

  defp apply_changes([%{relation: @electric_roles} | _] = changes, loader) do
    Enum.flat_map_reduce(changes, loader, &apply_user_change/2)
  end

  defp apply_changes(changes, loader) do
    {changes, loader}
  end

  # the ddlx table is insert-only
  defp apply_global_change(%Changes.NewRecord{} = change, {rules, count}) do
    %{record: %{"ddlx" => ddlx_bytes}} = change
    {:ok, ddlx} = Protox.decode(ddlx_bytes, SatPerms.DDLX)

    mutate_global(ddlx, rules, count)
  end

  defp apply_user_change(%Changes.NewRecord{} = change, loader) do
    %{record: %{"user_id" => user_id} = record} = change
    {:ok, loader, perms} = mutate_user_perms(record, user_id, loader, &insert_role/2)

    {
      [updated_user_permissions(user_id, perms)],
      loader
    }
  end

  defp apply_user_change(%Changes.DeletedRecord{} = change, loader) do
    %{old_record: %{"user_id" => user_id} = record} = change
    {:ok, loader, perms} = mutate_user_perms(record, user_id, loader, &delete_role/2)

    {
      [updated_user_permissions(user_id, perms)],
      loader
    }
  end

  defp apply_user_change(%Changes.UpdatedRecord{} = change, loader) do
    case change do
      %{old_record: %{"user_id" => user_id}, record: %{"user_id" => user_id} = record} ->
        {:ok, loader, perms} = mutate_user_perms(record, user_id, loader, &update_role/2)

        {
          [updated_user_permissions(user_id, perms)],
          loader
        }

      %{old_record: %{"user_id" => user_id1}, record: %{"user_id" => user_id2} = record} ->
        {:ok, loader, perms1} = mutate_user_perms(record, user_id1, loader, &delete_role/2)
        {:ok, loader, perms2} = mutate_user_perms(record, user_id2, loader, &insert_role/2)

        {
          [
            updated_user_permissions(user_id1, perms1),
            updated_user_permissions(user_id2, perms2)
          ],
          loader
        }
    end
  end

  def mutate_global(ddlx, rules, count \\ 0)

  def mutate_global(
        %SatPerms.DDLX{grants: [], revokes: [], assigns: [], unassigns: []},
        rules,
        count
      ) do
    {rules, count}
  end

  def mutate_global(%SatPerms.DDLX{} = ddlx, rules, count) do
    {apply_ddlx(rules, ddlx, count == 0), count + count_changes(ddlx)}
  end

  defp mutate_user_perms(record, user_id, loader, update_fun) do
    with {:ok, loader, perms} <- SchemaLoader.user_permissions(loader, user_id),
         {:ok, roles} <- update_fun.(perms, record) do
      {:ok, _loader, _perms} = SchemaLoader.save_user_permissions(loader, user_id, roles)
    end
  end

  defp insert_role(perms, record) do
    with {:ok, new_role, roles} <- load_roles(perms, record) do
      {:ok, Map.update!(roles, :roles, &[new_role | &1])}
    end
  end

  defp update_role(perms, record) do
    with {:ok, new_role, roles} <- load_roles(perms, record) do
      {:ok,
       Map.update!(
         roles,
         :roles,
         &Enum.map(&1, fn role -> if role.id == new_role.id, do: new_role, else: role end)
       )}
    end
  end

  defp delete_role(perms, record) do
    with {:ok, new_role, roles} <- load_roles(perms, record) do
      {:ok,
       Map.update!(
         roles,
         :roles,
         &Enum.reject(&1, fn role -> role.id == new_role.id end)
       )}
    end
  end

  defp load_roles(perms, record) do
    %{id: id, roles: role_list, rules: %{id: rules_id}} = perms

    with {:ok, role} <- role_from_record(record) do
      roles = %SatPerms.Roles{
        parent_id: id,
        rules_id: rules_id,
        roles: role_list
      }

      {:ok, role, roles}
    end
  end

  defp role_from_record(record) do
    %{
      "id" => id,
      "scope_table" => scope_table,
      "scope_id" => scope_id,
      "role" => role,
      "assignment_id" => assign_id,
      "user_id" => user_id
    } = record

    with {:ok, scope} <- role_scope(scope_table, scope_id) do
      {:ok,
       %SatPerms.Role{id: id, role: role, user_id: user_id, assign_id: assign_id, scope: scope}}
    end
  end

  defp role_scope(nil, _) do
    {:ok, nil}
  end

  defp role_scope(scope_table, scope_id) when is_binary(scope_table) do
    with {:ok, {schema, name}} = NameParser.parse(scope_table) do
      {:ok, %SatPerms.Scope{table: %SatPerms.Table{schema: schema, name: name}, id: scope_id}}
    end
  end

  @doc """
  the `%SatPerms.DDLX{}` struct contains multiple instances of say a `%SatPerms.Grant{}` but these
  multiple instances are the result of a single command (e.g. a `GRANT ALL...` will result in 4
  separate entries in the `grants` list but represent a single statement).

  Thus the order they are applied in a migration is preserved by the ordering of the arrival of
  the DDLX structs through the replication stream.

  Since each struct's id is fingerprint that specifies the a kind of primary key, we just need to
  operate on the existing rules keyed by this id.
  """
  @spec apply_ddlx(%SatPerms.Rules{}, %SatPerms.DDLX{}) :: %SatPerms.Rules{}
  def apply_ddlx(rules, ddlx, is_first? \\ true)

  def apply_ddlx(%SatPerms.Rules{} = rules, %SatPerms.DDLX{} = ddlx, is_first?) do
    rules
    |> update_grants(ddlx.grants)
    |> update_revokes(ddlx.revokes)
    |> update_assigns(ddlx.assigns)
    |> update_unassigns(ddlx.unassigns)
    |> increment_id(is_first?)
  end

  defp update_grants(rules, grants) do
    add_rules(rules, :grants, grants)
  end

  defp update_revokes(rules, revokes) do
    remove_rules(rules, :grants, revokes)
  end

  defp update_assigns(rules, assigns) do
    add_rules(rules, :assigns, assigns)
  end

  defp update_unassigns(rules, unassigns) do
    remove_rules(rules, :assigns, unassigns)
  end

  defp add_rules(rules, key, updates) do
    update_rules(rules, key, updates, fn update, existing ->
      Map.put(existing, update.id, update)
    end)
  end

  defp remove_rules(rules, key, updates) do
    update_rules(rules, key, updates, fn update, existing ->
      Map.delete(existing, update.id)
    end)
  end

  defp update_rules(rules, key, updates, update_fun) do
    Map.update!(rules, key, fn existing ->
      existing = Map.new(existing, &{&1.id, &1})

      # be absolutely sure that every permission struct has an id set
      updates
      |> Stream.map(&Command.put_id/1)
      |> Enum.reduce(existing, update_fun)
      |> Map.values()
    end)
  end

  defp increment_id(%{id: id} = rules, true) do
    %{rules | id: id + 1, parent_id: id}
  end

  defp increment_id(rules, false) do
    rules
  end

  defp count_changes(ddlx) do
    [:grants, :revokes, :assigns, :unassigns]
    |> Enum.reduce(0, fn key, count ->
      count + length(Map.fetch!(ddlx, key))
    end)
  end

  defp updated_user_permissions(user_id, permissions) do
    %Changes.UpdatedPermissions{
      type: :user,
      permissions: %Changes.UpdatedPermissions.UserPermissions{
        user_id: user_id,
        permissions: permissions
      }
    }
  end

  defp updated_global_permissions(permissions) do
    %Changes.UpdatedPermissions{
      type: :global,
      permissions: %Changes.UpdatedPermissions.GlobalPermissions{
        permissions_id: permissions.id
      }
    }
  end
end

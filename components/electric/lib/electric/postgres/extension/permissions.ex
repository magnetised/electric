defmodule Electric.Postgres.Extension.Permissions do
  alias Electric.Postgres.Extension
  alias Electric.Satellite.SatPerms

  @global_perms_table Extension.global_perms_table()
  @user_perms_table Extension.user_perms_table()

  @current_global_query """
    SELECT "id", "parent_id", "rules"
        FROM #{@global_perms_table}
        ORDER BY "id" DESC
        LIMIT 1
  """

  @shared_user_query """
    SELECT u.id,
           u.parent_id,
           u.roles,
           g.rules
        FROM #{@user_perms_table} u
        INNER JOIN #{@global_perms_table} g ON g.id = u.global_perms_id
  """

  @current_user_query """
    #{@shared_user_query}
        WHERE (u.user_id = $1)
        ORDER BY u.id DESC
        LIMIT 1
  """

  @specific_user_query """
    #{@shared_user_query}
        WHERE (u.user_id = $1) AND (u.id = $2)
        LIMIT 1
  """

  @save_global_query """
    INSERT INTO #{@global_perms_table} ("id", "parent_id", "rules")
        VALUES ($1, $2, $3)
  """

  @create_user_query """
    WITH global_perms AS (
        SELECT id, rules
        FROM #{@global_perms_table}
        ORDER BY id DESC
        LIMIT 1
    ), user_perms AS (
        INSERT INTO #{@user_perms_table} (user_id, parent_id, roles, global_perms_id)
        SELECT $1, $2, $3, id FROM global_perms
        RETURNING id
    )
    SELECT user_perms.id AS user_id,
           global_perms.id AS global_id,
           global_perms.rules
        FROM user_perms
        CROSS JOIN global_perms
  """

  def global(conn) do
    with {:ok, _cols, [row]} <- :epgsql.equery(conn, @current_global_query, []),
         {_id, _parent_id, bytes} = row do
      Protox.decode(bytes, SatPerms.Rules)
    end
  end

  def save_global(conn, %SatPerms.Rules{id: id, parent_id: parent_id} = rules) do
    with {:ok, iodata} <- Protox.encode(rules),
         bytes = IO.iodata_to_binary(iodata),
         {:ok, 1} <- :epgsql.equery(conn, @save_global_query, [id, parent_id, bytes]) do
      :ok
    end
  end

  def user(conn, user_id) do
    load_user_perms(conn, user_id, @current_user_query, [user_id], fn conn ->
      insert_user(conn, user_id)
    end)
  end

  def user(conn, user_id, perms_id) do
    load_user_perms(conn, user_id, @specific_user_query, [user_id, perms_id], fn _conn ->
      {:error, "no user permissions found for user=#{user_id} id=#{perms_id}"}
    end)
  end

  def save_user(conn, user_id, %SatPerms.Roles{} = roles) do
    insert_user(conn, user_id, roles)
  end

  defp load_user_perms(conn, user_id, query, binds, not_found_fun) do
    case :epgsql.equery(conn, query, binds) do
      {:ok, _, [{id, _parent_id, roles_bytes, rules_bytes}]} ->
        with {:ok, roles} <- Protox.decode(roles_bytes, SatPerms.Roles),
             {:ok, rules} <- Protox.decode(rules_bytes, SatPerms.Rules) do
          {:ok, %SatPerms{id: id, user_id: user_id, rules: rules, roles: roles.roles}}
        end

      {:ok, _, []} ->
        not_found_fun.(conn)

      error ->
        error
    end
  end

  defp insert_user(conn, user_id, roles \\ %SatPerms.Roles{}) do
    encoded_roles =
      roles |> Protox.encode!() |> IO.iodata_to_binary()

    with {:ok, _, [row]} <-
           :epgsql.equery(conn, @create_user_query, [user_id, roles.parent_id, encoded_roles]),
         {id, _global_perms_id, rules} = row,
         {:ok, rules} = Protox.decode(rules, SatPerms.Rules) do
      {:ok, %SatPerms{id: id, user_id: user_id, rules: rules, roles: roles.roles}}
    end
  end
end

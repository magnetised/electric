defmodule Electric.Satellite.Permissions.WhereClauseTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Runner
  alias Electric.Satellite.Permissions

  @auth_types %{
    ["auth", "user_id"] => :text,
    ["auth", "claims", "context"] => :text
  }
  @user_id "b2ce289a-3d2d-4ff7-9892-d446d5866f74"
  @not_user_id "ec61ba28-7195-47a2-8d93-e71068dc7160"

  setup do
    table_columns = %{
      "id" => :uuid,
      "user_id" => :uuid,
      "parent_id" => :uuid,
      "name" => :text,
      "value" => :text,
      "amount" => :integer,
      "valid" => :bool,
      "inserted_at" => :timestamptz
    }

    auth = %{
      ["auth", "user_id"] => @user_id
    }

    {:ok, table_columns: table_columns, auth: auth}
  end

  def execute(cxt, stmt, op, attrs) when op in [:update, :delete, :insert, :select] do
    prefixes =
      case op do
        :update -> ["new", "old", "row", "this"]
        :select -> ["row", "this"]
        :insert -> ["new", "row", "this"]
        :delete -> ["old", "row", "this"]
      end

    env =
      Enum.reduce(prefixes, %{}, fn prefix, env ->
        Enum.reduce(cxt.table_columns, env, fn {column, type}, env ->
          Map.put(env, [prefix, column], type)
        end)
      end)

    types =
      Enum.reduce(@auth_types, env, fn {column, type}, env ->
        Map.put(env, column, type)
      end)

    env =
      [:new, :old]
      |> Enum.reduce(%{}, fn assign, env ->
        record = Keyword.get(attrs, assign, %{})

        Enum.reduce(record, env, fn {column, value}, env ->
          Map.put(env, [to_string(assign), column], value)
        end)
      end)
      |> Map.merge(cxt.auth)

    stmt
    |> Parser.parse_and_validate_expression!(types)
    |> Permissions.Eval.expand_row_aliases(op)
    |> Runner.execute(env)
  end

  describe "UPDATE" do
    test "with NEW reference", cxt do
      assert {:ok, true} =
               execute(cxt, "NEW.user_id::text = AUTH.user_id", :update,
                 new: %{"user_id" => @user_id},
                 old: %{"user_id" => @not_user_id}
               )
    end

    test "with OLD reference", cxt do
      assert {:ok, true} =
               execute(cxt, "OLD.user_id::text = auth.user_id", :update,
                 new: %{"user_id" => @user_id},
                 old: %{"user_id" => @user_id}
               )

      assert {:ok, false} =
               execute(cxt, "OLD.user_id::text = auth.user_id", :update,
                 new: %{"user_id" => @user_id},
                 old: %{"user_id" => @not_user_id}
               )
    end

    test "with ROW reference", cxt do
      stmt = "ROW.user_id::text = auth.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id},
                 old: %{"user_id" => @user_id}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id},
                 old: %{"user_id" => @not_user_id}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @not_user_id},
                 old: %{"user_id" => @user_id}
               )
    end

    test "multi-clause ROW/THIS reference", cxt do
      stmt = "(ROW.user_id::text = auth.user_id) AND this.valid"

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => true},
                 old: %{"user_id" => @user_id, "valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => true},
                 old: %{"user_id" => @user_id, "valid" => false}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => true},
                 old: %{"user_id" => @not_user_id, "valid" => true}
               )
    end

    test "mixed row and NEW references", cxt do
      stmt = "(ROW.user_id::text = auth.user_id) AND NOT new.valid"

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => false},
                 old: %{"user_id" => @user_id, "valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => true},
                 old: %{"user_id" => @user_id, "valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"user_id" => @user_id, "valid" => false},
                 old: %{"user_id" => @not_user_id, "valid" => true}
               )
    end

    test "with NEW reference to bool column", cxt do
      stmt = "new.valid"

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => true},
                 old: %{"valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => false},
                 old: %{"valid" => true}
               )
    end

    test "with NOT(ROW) reference to bool column", cxt do
      stmt = "NOT row.valid"

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => true},
                 old: %{"valid" => true}
               )

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => false},
                 old: %{"valid" => false}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => true},
                 old: %{"valid" => false}
               )
    end

    test "with THIS reference to bool column", cxt do
      stmt = "this.valid"

      assert {:ok, true} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => true},
                 old: %{"valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => false},
                 old: %{"valid" => true}
               )

      assert {:ok, false} =
               execute(cxt, stmt, :update,
                 new: %{"valid" => true},
                 old: %{"valid" => false}
               )
    end
  end

  describe "INSERT" do
  end

  describe "DELETE" do
  end

  describe "SELECT" do
  end
end

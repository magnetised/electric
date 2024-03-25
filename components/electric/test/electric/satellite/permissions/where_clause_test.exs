defmodule Electric.Satellite.Permissions.WhereClauseTest do
  use ExUnit.Case, async: true

  alias ElectricTest.PermissionsHelpers
  alias ElectricTest.PermissionsHelpers.Chgs

  alias Electric.Satellite.Auth
  alias Electric.Satellite.Permissions

  @user_id "b2ce289a-3d2d-4ff7-9892-d446d5866f74"
  @not_user_id "ec61ba28-7195-47a2-8d93-e71068dc7160"
  @table {"public", "lotsoftypes"}

  setup do
    {:ok, schema_version} = PermissionsHelpers.Schema.load()
    auth = %Auth{user_id: @user_id}

    evaluator = Permissions.Eval.new(schema_version, auth)

    {:ok, auth: auth, schema_version: schema_version, evaluator: evaluator}
  end

  def execute(cxt, stmt, change) do
    assert {:ok, expr_cxt} = Permissions.Eval.expression_context(cxt.evaluator, stmt, @table)

    Permissions.Eval.execute(expr_cxt, change)
  end

  def update(base, changes \\ %{}) do
    Chgs.update(@table, base, changes)
  end

  def insert(record) do
    Chgs.insert(@table, record)
  end

  def delete(record) do
    Chgs.delete(@table, record)
  end

  def change(f, r) do
    apply(__MODULE__, f, [r])
  end

  describe "UPDATE" do
    test "with NEW reference", cxt do
      stmt = "NEW.user_id::text = AUTH.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))
    end

    test "with OLD reference", cxt do
      stmt = "OLD.user_id::text = auth.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))
    end

    test "with ROW reference", cxt do
      stmt = "ROW.user_id::text = auth.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}, %{"user_id" => @not_user_id}))
    end

    test "multi-clause ROW/THIS reference", cxt do
      stmt = "(ROW.user_id::text = auth.user_id) AND this.valid"

      assert {:ok, true} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => false}, %{"valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @not_user_id, "valid" => true}, %{"user_id" => @user_id})
               )
    end

    test "mixed row and NEW references", cxt do
      stmt = "(ROW.user_id::text = auth.user_id) AND NOT new.valid"

      assert {:ok, true} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true}, %{"valid" => false})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @not_user_id, "valid" => true}, %{
                   "user_id" => @user_id,
                   "valid" => false
                 })
               )
    end

    test "with NEW reference to bool column", cxt do
      stmt = "new.valid"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}))
    end

    test "with NOT(ROW) reference to bool column", cxt do
      stmt = "NOT row.valid"

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => false}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}, %{"valid" => true}))
    end

    test "with THIS reference to bool column", cxt do
      stmt = "this.valid"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => true}, %{"valid" => false}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}, %{"valid" => true}))
    end
  end

  for {change_fun, name, ref} <- [
        {:insert, "INSERT", "NEW"},
        {:delete, "DELETE", "OLD"}
      ] do
    describe name do
      test "with #{ref} reference", cxt do
        stmt = "#{unquote(ref)}.user_id::text = AUTH.user_id"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @user_id}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @not_user_id}))
      end

      test "with ROW reference", cxt do
        stmt = "ROW.user_id::text = auth.user_id"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @user_id}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @not_user_id}))
      end

      test "multi-clause ROW/THIS reference", cxt do
        stmt = "(ROW.user_id::text = auth.user_id) AND this.valid"

        assert {:ok, true} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @user_id, "valid" => true})
                 )

        assert {:ok, false} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @user_id, "valid" => false})
                 )

        assert {:ok, false} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @not_user_id, "valid" => true})
                 )
      end

      test "with #{ref} reference to bool column", cxt do
        stmt = "#{unquote(ref)}.valid"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end

      test "with NOT(ROW) reference to bool column", cxt do
        stmt = "NOT row.valid"

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end

      test "with THIS reference to bool column", cxt do
        stmt = "this.valid"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end
    end
  end
end

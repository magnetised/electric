defmodule Electric.Satellite.Permissions.ConsumerTest do
  use Electric.Extension.Case, async: false

  alias Electric.DDLX
  alias Electric.DDLX.Command
  alias Electric.Postgres.Extension.SchemaLoader
  alias Electric.Satellite.Permissions.Consumer
  alias Electric.Satellite.SatPerms
  alias ElectricTest.PermissionsHelpers.{Chgs, Proto}
  alias Electric.Replication.Changes
  alias Electric.Postgres.Extension

  def update(rules \\ %SatPerms.Rules{}, cmds) do
    Consumer.apply_ddlx(rules, Command.ddlx(cmds))
  end

  def new(cmds) do
    update(cmds)
  end

  def parse_ddlx(ddlx) do
    ddlx
    |> Enum.map(&DDLX.Parser.parse/1)
    |> Enum.map(&elem(&1, 1))
    |> Enum.map(fn %{cmds: %SatPerms.DDLX{} = cmd} -> cmd end)
  end

  describe "update/2" do
    test "ASSIGN" do
      assign =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_name: "admin"
        )

      assert %SatPerms.Rules{id: 1, parent_id: 0} = rules = update(assigns: [assign])

      assert [^assign] = rules.assigns
    end

    test "ASSIGN, UNASSIGN" do
      rules =
        new(
          assigns: [
            Proto.assign(
              table: Proto.table("my_default", "admin_users"),
              user_column: "user_id",
              role_name: "admin"
            )
          ]
        )

      updated =
        update(
          rules,
          unassigns: [
            Proto.unassign(
              table: Proto.table("my_default", "admin_users"),
              user_column: "user_id",
              role_name: "admin"
            )
          ]
        )

      assert updated.id == 2
      assert updated.assigns == []
    end

    test "ASSIGN ... IF, UNASSIGN" do
      rules =
        new(
          assigns: [
            Proto.assign(
              table: Proto.table("my_default", "admin_users"),
              user_column: "user_id",
              role_name: "admin",
              if: "something()"
            )
          ]
        )

      updated =
        update(
          rules,
          unassigns: [
            Proto.unassign(
              table: Proto.table("my_default", "admin_users"),
              user_column: "user_id",
              role_name: "admin"
            )
          ]
        )

      assert updated.id == 2
      assert updated.assigns == []
    end

    test "ASSIGN, ASSIGN, UNASSIGN" do
      assign1 =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_name: "admin",
          scope: Proto.scope("projects")
        )

      assign2 =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_column: "role_name"
        )

      rules = new(assigns: [assign1, assign2])

      rules =
        update(rules,
          unassigns: [
            Proto.unassign(
              table: Proto.table("my_default", "admin_users"),
              user_column: "user_id",
              role_name: "admin",
              scope: Proto.scope("projects")
            )
          ]
        )

      assert rules.id == 2
      assert [^assign2] = rules.assigns
    end

    test "ASSIGN, re-ASSIGN" do
      assign1 =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_name: "admin",
          scope: Proto.scope("projects")
        )

      assign2 =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_name: "admin",
          scope: Proto.scope("projects"),
          if: "some_test()"
        )

      rules = new(assigns: [assign1])

      rules = update(rules, assigns: [assign2])

      assert rules.id == 2
      assert [^assign2] = rules.assigns
    end

    test "GRANT" do
      grant =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects")
        )

      rules = update(grants: [grant])

      assert rules.id == 1
      assert [^grant] = rules.grants
    end

    test "GRANT, REVOKE" do
      grant =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects")
        )

      rules = new(grants: [grant])

      updated =
        update(
          rules,
          revokes: [
            Proto.revoke(
              table: Proto.table("issues"),
              role: Proto.role("editor"),
              privilege: :INSERT,
              scope: Proto.scope("projects")
            )
          ]
        )

      assert updated.grants == []
    end

    test "GRANT ... CHECK, REVOKE" do
      grant =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects"),
          check: "something()"
        )

      rules = new(grants: [grant])

      updated =
        update(
          rules,
          revokes: [
            Proto.revoke(
              table: Proto.table("issues"),
              role: Proto.role("editor"),
              privilege: :INSERT,
              scope: Proto.scope("projects")
            )
          ]
        )

      assert updated.grants == []
    end

    test "GRANT, GRANT, REVOKE" do
      grant1 =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects")
        )

      grant2 =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :UPDATE,
          scope: Proto.scope("projects")
        )

      rules = new(grants: [grant1, grant2])

      updated =
        update(
          rules,
          revokes: [
            Proto.revoke(
              table: Proto.table("issues"),
              role: Proto.role("editor"),
              privilege: :INSERT,
              scope: Proto.scope("projects")
            )
          ]
        )

      assert updated.grants == [grant2]
    end

    test "GRANT, re-GRANT" do
      grant1 =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects")
        )

      grant2 =
        Proto.grant(
          table: Proto.table("issues"),
          role: Proto.role("editor"),
          privilege: :INSERT,
          scope: Proto.scope("projects"),
          check: "some_check()"
        )

      rules = new(grants: [grant1])

      updated =
        update(rules, grants: [grant2])

      assert updated.grants == [grant2]
    end

    test "update with DDLX" do
      ddlx = [
        ~S[ELECTRIC ASSIGN (projects, members.role_name) TO members.user_id],
        ~S[ELECTRIC ASSIGN (projects, members.role_name) TO members.user_id IF (some_check_passes())],
        ~S[ELECTRIC GRANT ALL ON issues TO (projects, 'editor')],
        ~S[ELECTRIC GRANT READ ON issues TO (projects, 'editor') WHERE ((ROW.user_id = AUTH.user_id) AND (ROW.value > 3))],
        ~S[ELECTRIC REVOKE DELETE ON issues FROM (projects, 'editor')]
      ]

      rules =
        ddlx
        |> parse_ddlx()
        |> Enum.reduce(%SatPerms.Rules{}, &Consumer.apply_ddlx(&2, &1))

      assert rules == %SatPerms.Rules{
               id: 5,
               parent_id: 4,
               assigns: [
                 Proto.assign(
                   scope: Proto.scope("projects"),
                   table: Proto.table("members"),
                   user_column: "user_id",
                   role_column: "role_name",
                   if: "some_check_passes()"
                 )
               ],
               grants: [
                 Proto.grant(
                   privilege: :UPDATE,
                   scope: Proto.scope("projects"),
                   table: Proto.table("issues"),
                   role: Proto.role("editor")
                 ),
                 Proto.grant(
                   privilege: :SELECT,
                   scope: Proto.scope("projects"),
                   table: Proto.table("issues"),
                   role: Proto.role("editor"),
                   check: "(ROW.user_id = AUTH.user_id) AND (ROW.value > 3)"
                 ),
                 Proto.grant(
                   privilege: :INSERT,
                   scope: Proto.scope("projects"),
                   table: Proto.table("issues"),
                   role: Proto.role("editor")
                 )
               ]
             }

      ddlx = [
        ~S[ELECTRIC UNASSIGN (projects, members.role_name) FROM members.user_id],
        ~S[ELECTRIC REVOKE UPDATE ON issues FROM (projects, 'editor')],
        ~S[ELECTRIC REVOKE READ ON issues FROM (projects, 'editor')],
        ~S[ELECTRIC REVOKE INSERT ON issues FROM (projects, 'editor')]
      ]

      rules =
        ddlx
        |> parse_ddlx()
        |> Enum.reduce(rules, &Consumer.apply_ddlx(&2, &1))

      assert rules == %SatPerms.Rules{
               id: 9,
               parent_id: 8,
               assigns: [],
               grants: []
             }
    end
  end

  def loader_with_global_perms(conn, cxt) do
    loader = loader(conn, cxt)

    ddlx =
      Command.ddlx(
        grants: [
          Proto.grant(
            privilege: :INSERT,
            table: Proto.table("issues"),
            role: Proto.role("editor"),
            scope: Proto.scope("projects")
          )
        ],
        assigns: [
          Proto.assign(
            table: Proto.table("project_memberships"),
            scope: Proto.scope("projects"),
            user_column: "user_id",
            role_name: "editor"
          )
        ]
      )

    assert {:ok, 2, loader, rules} = Consumer.update_global(ddlx, loader)

    {loader, rules}
  end

  def loader(conn, cxt) do
    {:ok, loader} = SchemaLoader.connect({cxt.loader_impl, []}, __connection__: conn)
    loader
  end

  for loader_impl <- [SchemaLoader.Epgsql, Electric.Postgres.MockSchemaLoader] do
    setup do
      {:ok, loader_impl: unquote(loader_impl)}
    end

    describe "global rules serialisation #{loader_impl}" do
      test_tx "is initialised with empty state", fn conn, cxt ->
        loader = loader(conn, cxt)

        assert {:ok, %SatPerms.Rules{id: 1, assigns: [], grants: []}} =
                 SchemaLoader.global_permissions(loader)
      end

      test_tx "can update its state", fn conn, cxt ->
        loader = loader(conn, cxt)

        assign1 =
          Proto.assign(
            table: Proto.table("my_default", "admin_users"),
            user_column: "user_id",
            role_name: "admin"
          )

        ddlx = Command.ddlx(assigns: [assign1])

        tx =
          Chgs.tx([
            Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
            Chgs.ddlx(ddlx)
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert tx.changes == [
                 Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
                 %Changes.UpdatedPermissions{
                   type: :global,
                   permissions: %Changes.UpdatedPermissions.GlobalPermissions{
                     permissions_id: 2
                   }
                 }
               ]

        assert {:ok, rules} = SchemaLoader.global_permissions(loader)
        assert %SatPerms.Rules{id: 2, parent_id: 1, assigns: [^assign1]} = rules

        assign2 =
          Proto.assign(
            table: Proto.table("my_default", "admin_users"),
            user_column: "user_id",
            role_name: "admin2"
          )

        ddlx = Command.ddlx(assigns: [assign2])

        tx =
          Chgs.tx([
            Chgs.ddlx(ddlx),
            Chgs.insert({"public", "kittens"}, %{"size" => "cute"})
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :global,
                   permissions: %Changes.UpdatedPermissions.GlobalPermissions{
                     permissions_id: 3
                   }
                 },
                 Chgs.insert({"public", "kittens"}, %{"size" => "cute"})
               ]

        assert {:ok, rules} = SchemaLoader.global_permissions(loader)
        assert %SatPerms.Rules{id: 3, parent_id: 2, assigns: [^assign1, ^assign2]} = rules
      end

      test_tx "sequential updates are coalesced", fn conn, cxt ->
        # we want to minimize permissions churn when possible
        loader = loader(conn, cxt)

        assign1 =
          Proto.assign(
            table: Proto.table("my_default", "admin_users"),
            user_column: "user_id",
            role_name: "admin"
          )

        ddlx1 = Command.ddlx(assigns: [assign1])

        assign2 =
          Proto.assign(
            table: Proto.table("project_memberships"),
            user_column: "user_id",
            scope: Proto.scope("projects"),
            role_column: "role"
          )

        ddlx2 = Command.ddlx(assigns: [assign2])

        assign3 =
          Proto.assign(
            table: Proto.table("team_memberships"),
            user_column: "user_id",
            scope: Proto.scope("teams"),
            role_column: "role"
          )

        ddlx3 = Command.ddlx(assigns: [assign3])

        tx =
          Chgs.tx([
            Chgs.ddlx(ddlx1),
            Chgs.ddlx(ddlx2),
            Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
            Chgs.insert({"public", "kittens"}, %{"fur" => "furry"}),
            Chgs.ddlx(ddlx3)
          ])

        assert {:ok, tx, _loader} = Consumer.update(tx, loader)

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :global,
                   permissions: %Changes.UpdatedPermissions.GlobalPermissions{
                     permissions_id: 2
                   }
                 },
                 Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
                 Chgs.insert({"public", "kittens"}, %{"fur" => "furry"}),
                 %Changes.UpdatedPermissions{
                   type: :global,
                   permissions: %Changes.UpdatedPermissions.GlobalPermissions{
                     permissions_id: 3
                   }
                 }
               ]
      end
    end

    @user_id "7a81b0d0-97bf-466d-9053-4612146c2b67"

    describe "user roles state #{loader_impl}" do
      test_tx "starts with empty state", fn conn, cxt ->
        {loader, rules} = loader_with_global_perms(conn, cxt)

        assert {:ok, _loader,
                %SatPerms{
                  id: 1,
                  user_id: @user_id,
                  rules: ^rules,
                  roles: []
                } = perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert {:ok, _loader, ^perms} =
                 SchemaLoader.user_permissions(loader, @user_id)
      end

      test_tx "can load a specific version", fn conn, cxt ->
        {loader, _rules} = loader_with_global_perms(conn, cxt)

        assert {:ok, loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert {:ok, ^perms} =
                 SchemaLoader.user_permissions(loader, @user_id, perms.id)

        assert {:ok, _loader, other_perms} =
                 SchemaLoader.user_permissions(loader, "7c9fe38c-895b-48f5-9b31-bb6ca992bf2b")

        refute other_perms.id == perms.id

        # attempting to load another user's perms by id
        assert {:error, _} =
                 SchemaLoader.user_permissions(loader, @user_id, other_perms.id)
      end

      test_tx "user roles are added via an insert to roles table", fn conn, cxt ->
        {loader, rules} = loader_with_global_perms(conn, cxt)

        %{assigns: [%{id: assign_id}]} = rules

        tx =
          Chgs.tx([
            Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert {:ok, loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert %{id: 2, user_id: @user_id, rules: %{id: 2}} = perms

        assert tx.changes == [
                 Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: @user_id,
                     permissions: perms
                   }
                 }
               ]

        assert perms.roles == [
                 %SatPerms.Role{
                   id: "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   assign_id: assign_id,
                   role: "editor",
                   user_id: @user_id,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "123"}
                 }
               ]

        tx =
          Chgs.tx([
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "5c0fd272-3fc2-4ae8-8574-92823c814096",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "222",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert {:ok, _loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: @user_id,
                     permissions: perms
                   }
                 }
               ]

        assert perms.roles == [
                 %SatPerms.Role{
                   id: "5c0fd272-3fc2-4ae8-8574-92823c814096",
                   assign_id: assign_id,
                   role: "editor",
                   user_id: @user_id,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "222"}
                 },
                 %SatPerms.Role{
                   id: "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   assign_id: assign_id,
                   role: "editor",
                   user_id: @user_id,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "123"}
                 }
               ]
      end

      test_tx "user roles are updated via an update to roles table", fn conn, cxt ->
        {loader, rules} = loader_with_global_perms(conn, cxt)

        %{assigns: [%{id: assign_id}]} = rules

        tx =
          Chgs.tx([
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, _tx, loader} = Consumer.update(tx, loader)

        tx =
          Chgs.tx([
            Chgs.update(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              },
              %{
                "role" => "manager"
              }
            )
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert {:ok, _loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: @user_id,
                     permissions: perms
                   }
                 }
               ]

        assert perms.roles == [
                 %SatPerms.Role{
                   id: "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   assign_id: assign_id,
                   role: "manager",
                   user_id: @user_id,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "123"}
                 }
               ]
      end

      test_tx "changes in role ownership are managed", fn conn, cxt ->
        {loader, rules} = loader_with_global_perms(conn, cxt)

        %{assigns: [%{id: assign_id}]} = rules

        user_id2 = "0c7afad3-213a-4158-9e89-312fc5e682e1"

        tx =
          Chgs.tx([
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, _tx, loader} = Consumer.update(tx, loader)

        tx =
          Chgs.tx([
            Chgs.update(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              },
              %{
                "user_id" => user_id2
              }
            )
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert {:ok, loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert {:ok, _loader, perms2} =
                 SchemaLoader.user_permissions(loader, user_id2)

        assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms
        assert %{id: 5, user_id: user_id2, rules: %{id: 2}} = perms2

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: @user_id,
                     permissions: perms
                   }
                 },
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: user_id2,
                     permissions: perms2
                   }
                 }
               ]

        assert perms.roles == []

        assert perms2.roles == [
                 %SatPerms.Role{
                   id: "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   assign_id: assign_id,
                   role: "editor",
                   user_id: user_id2,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "123"}
                 }
               ]
      end

      test_tx "user roles are deleted with deletes to roles table", fn conn, cxt ->
        {loader, rules} = loader_with_global_perms(conn, cxt)

        %{assigns: [%{id: assign_id}]} = rules

        tx =
          Chgs.tx([
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              }
            ),
            Chgs.insert(
              Extension.roles_relation(),
              %{
                "id" => "bc1e3de7-1be4-4d86-8c61-416a7a0a39f5",
                "role" => "manager",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "222",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, _tx, loader} = Consumer.update(tx, loader)

        tx =
          Chgs.tx([
            Chgs.delete(
              Extension.roles_relation(),
              %{
                "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                "role" => "editor",
                "user_id" => @user_id,
                "scope_table" => ~s["public"."projects"],
                "scope_id" => "123",
                "assignment_id" => assign_id
              }
            )
          ])

        assert {:ok, tx, loader} = Consumer.update(tx, loader)

        assert {:ok, _loader, perms} =
                 SchemaLoader.user_permissions(loader, @user_id)

        assert %{id: 4, user_id: @user_id, rules: %{id: 2}} = perms

        assert tx.changes == [
                 %Changes.UpdatedPermissions{
                   type: :user,
                   permissions: %Changes.UpdatedPermissions.UserPermissions{
                     user_id: @user_id,
                     permissions: perms
                   }
                 }
               ]

        assert perms.roles == [
                 %SatPerms.Role{
                   id: "bc1e3de7-1be4-4d86-8c61-416a7a0a39f5",
                   assign_id: assign_id,
                   role: "manager",
                   user_id: @user_id,
                   scope: %SatPerms.Scope{table: Proto.table("projects"), id: "222"}
                 }
               ]
      end
    end

    test_tx "#{loader_impl} sqlite ddlx messages are a no-op", fn conn, cxt ->
      loader = loader(conn, cxt)

      ddlx = Command.ddlx(sqlite: [Proto.sqlite("create table local (id primary key)")])

      tx =
        Chgs.tx([
          Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
          Chgs.ddlx(ddlx)
        ])

      assert {:ok, tx, _loader} = Consumer.update(tx, loader)

      assert tx.changes == [
               Chgs.insert({"public", "kittens"}, %{"size" => "cute"})
             ]
    end
  end
end

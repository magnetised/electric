defmodule Electric.Satellite.Permissions.ConsumerTest do
  use Electric.Extension.Case, async: true

  alias Electric.DDLX
  alias Electric.DDLX.Command
  alias Electric.Postgres.Extension.SchemaLoader
  alias Electric.Postgres.MockSchemaLoader
  alias Electric.Replication.Changes
  alias Electric.Satellite.Permissions.Consumer
  alias Electric.Satellite.SatPerms
  alias ElectricTest.PermissionsHelpers.{Chgs, Proto}

  def apply_ddlx(rules \\ %SatPerms.Rules{}, cmds) do
    Consumer.apply_ddlx(rules, Command.ddlx(cmds))
  end

  def new(cmds) do
    apply_ddlx(cmds)
  end

  def parse_ddlx(ddlx) do
    ddlx
    |> Enum.map(&DDLX.Parser.parse/1)
    |> Enum.map(&elem(&1, 1))
    |> Enum.map(fn %{cmds: %SatPerms.DDLX{} = cmd} -> cmd end)
  end

  @scoped_assign_relation {"public", "project_memberships"}
  @unscoped_assign_relation {"public", "site_admins"}

  describe "apply_ddlx/2" do
    test "ASSIGN" do
      assign =
        Proto.assign(
          table: Proto.table("my_default", "admin_users"),
          user_column: "user_id",
          role_name: "admin"
        )

      assert %SatPerms.Rules{id: 1, parent_id: 0} = rules = apply_ddlx(assigns: [assign])

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
        apply_ddlx(
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
        apply_ddlx(
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
        apply_ddlx(rules,
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

      rules = apply_ddlx(rules, assigns: [assign2])

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

      rules = apply_ddlx(grants: [grant])

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
        apply_ddlx(
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
        apply_ddlx(
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
        apply_ddlx(
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

      updated = apply_ddlx(rules, grants: [grant2])

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

  def loader_with_global_perms(cxt) do
    loader = loader(cxt)

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
            role_column: "project_role"
          ),
          Proto.assign(
            table: Proto.table("site_admins"),
            user_column: "user_id",
            role_column: "site_role"
          )
        ]
      )

    assert {:ok, 3, loader, rules} = Consumer.update_global(ddlx, loader)

    {loader, rules}
  end

  def loader(_cxt) do
    loader_spec =
      MockSchemaLoader.backend_spec(
        migrations: [
          {"01",
           [
             """
             create table projects (id uuid primary key)
             """,
             """
             create table users (id uuid primary key)
             """,
             """
             create table teams (id uuid primary key)
             """,
             """
             create table project_memberships (
                id uuid primary key,
                user_id uuid not null references users (id),
                project_id uuid not null references projects (id),
                project_role text not null
             )
             """,
             """
             create table team_memberships (
                id uuid primary key,
                user_id uuid not null references users (id),
                team_id uuid not null references teams (id),
                team_role text not null
             )
             """,
             """
             create table site_admins (
                id uuid primary key,
                user_id uuid not null references users (id),
                site_role text not null
             )
             """,
             """
             create table my_default.admin_users (
                id uuid primary key,
                user_id uuid not null references users (id)
             )
             """
           ]}
        ]
      )

    {:ok, loader} = SchemaLoader.connect(loader_spec, [])
    loader
  end

  describe "SchemaLoader.Epgsql" do
    def epgsql_loader(conn) do
      {:ok, loader} = SchemaLoader.connect({SchemaLoader.Epgsql, []}, __connection__: conn)
      loader
    end

    def epgsql_loader_with_rules(conn) do
      loader = epgsql_loader(conn)

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
              role_column: "project_role"
            ),
            Proto.assign(
              table: Proto.table("site_admins"),
              user_column: "user_id",
              role_column: "site_role"
            )
          ]
        )

      # not using the returned loader to **prove** it's a pg powered one
      assert {:ok, 3, _loader, %{id: 2} = _rules} = Consumer.update_global(ddlx, loader)

      loader
    end

    test_tx "can retrieve the current global rules", fn conn ->
      loader = epgsql_loader(conn)
      assert {:ok, %SatPerms.Rules{id: 1} = _rules} = SchemaLoader.global_permissions(loader)
    end

    test_tx "can retrieve the specific global rules", fn conn ->
      loader = epgsql_loader(conn)
      assert {:ok, %SatPerms.Rules{id: 1} = _rules} = SchemaLoader.global_permissions(loader, 1)
    end

    test_tx "can update the global rules", fn conn ->
      loader = epgsql_loader(conn)

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
              role_column: "project_role"
            ),
            Proto.assign(
              table: Proto.table("site_admins"),
              user_column: "user_id",
              role_column: "site_role"
            )
          ]
        )

      # not using the returned loader to **prove** it's a pg powered one
      assert {:ok, 3, _loader, %{id: 2} = rules} = Consumer.update_global(ddlx, loader)
      assert {:ok, %SatPerms.Rules{id: 2} = ^rules} = SchemaLoader.global_permissions(loader)
    end

    test_tx "retrieves an empty perms state for any user", fn conn ->
      loader = epgsql_loader_with_rules(conn)

      assert {:ok, _loader,
              %SatPerms{
                id: 1,
                user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                roles: [],
                rules: %SatPerms.Rules{id: 2}
              }} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, _loader,
              %SatPerms{
                id: 2,
                user_id: "11f03d43-09e9-483b-9e8c-1f0e117f20fe",
                roles: [],
                rules: %SatPerms.Rules{id: 2}
              }} =
               SchemaLoader.user_permissions(loader, "11f03d43-09e9-483b-9e8c-1f0e117f20fe")
    end

    test_tx "can retrieve a specific user perms version", fn conn ->
      loader = epgsql_loader_with_rules(conn)

      assert {:ok, _loader, %SatPerms{id: 1}} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, %SatPerms{id: 1}} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27", 1)
    end

    test_tx "can update a user's permissions", fn conn ->
      loader = epgsql_loader_with_rules(conn)

      assert {:ok, _loader, %SatPerms{id: 1, rules: %{id: rules_id}}} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, _loader, %SatPerms{id: 2, roles: [_]}} =
               SchemaLoader.save_user_permissions(
                 loader,
                 "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                 %SatPerms.Roles{
                   parent_id: 1,
                   rules_id: rules_id,
                   roles: [
                     %SatPerms.Role{
                       user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                       role: "editor"
                     }
                   ]
                 }
               )

      assert {:ok, _loader, %SatPerms{id: 2, roles: [_]}} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")
    end

    test_tx "updating the global rules migrates existing user roles", fn conn ->
      loader = epgsql_loader_with_rules(conn)

      assert {:ok, _loader,
              %SatPerms{
                id: 1,
                user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                roles: [],
                rules: %SatPerms.Rules{id: 2}
              }} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, _loader,
              %SatPerms{
                id: 2,
                user_id: "11f03d43-09e9-483b-9e8c-1f0e117f20fe",
                roles: [],
                rules: %SatPerms.Rules{id: 2}
              }} =
               SchemaLoader.user_permissions(loader, "11f03d43-09e9-483b-9e8c-1f0e117f20fe")

      assert {:ok, _loader, %SatPerms{id: 3, roles: [_]}} =
               SchemaLoader.save_user_permissions(
                 loader,
                 "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                 %SatPerms.Roles{
                   parent_id: 1,
                   rules_id: 2,
                   roles: [
                     %SatPerms.Role{
                       user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                       role: "editor"
                     }
                   ]
                 }
               )

      ddlx =
        Command.ddlx(
          grants: [
            Proto.grant(
              privilege: :INSERT,
              table: Proto.table("comments"),
              role: Proto.role("editor"),
              scope: Proto.scope("projects")
            )
          ]
        )

      assert {:ok, 1, _loader, %{id: 3} = rules} = Consumer.update_global(ddlx, loader)

      assert {:ok, _loader,
              %SatPerms{
                id: 5,
                user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                rules: ^rules
              }} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, _loader,
              %SatPerms{
                id: 4,
                user_id: "11f03d43-09e9-483b-9e8c-1f0e117f20fe",
                rules: ^rules
              }} =
               SchemaLoader.user_permissions(loader, "11f03d43-09e9-483b-9e8c-1f0e117f20fe")

      ddlx =
        Command.ddlx(
          grants: [
            Proto.grant(
              privilege: :DELETE,
              table: Proto.table("comments"),
              role: Proto.role("editor"),
              scope: Proto.scope("projects")
            )
          ]
        )

      assert {:ok, 1, _loader, %{id: 4} = rules} = Consumer.update_global(ddlx, loader)

      assert {:ok, _loader,
              %SatPerms{
                id: 7,
                user_id: "e815dfe6-f64d-472a-a322-bfc9e7993d27",
                rules: ^rules
              }} =
               SchemaLoader.user_permissions(loader, "e815dfe6-f64d-472a-a322-bfc9e7993d27")

      assert {:ok, _loader,
              %SatPerms{
                id: 6,
                user_id: "11f03d43-09e9-483b-9e8c-1f0e117f20fe",
                rules: ^rules
              }} =
               SchemaLoader.user_permissions(loader, "11f03d43-09e9-483b-9e8c-1f0e117f20fe")
    end
  end

  describe "global rules serialisation" do
    test "is initialised with empty state", cxt do
      loader = loader(cxt)

      assert {:ok, %SatPerms.Rules{id: 1, assigns: [], grants: []}} =
               SchemaLoader.global_permissions(loader)
    end

    test "can update its state", cxt do
      loader = loader(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

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

      assert {:ok, tx, consumer, loader} = Consumer.update(tx, consumer, loader)

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

      assert {:ok, tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

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

    test "sequential updates are coalesced", cxt do
      # we want to minimize permissions churn when possible
      loader = loader(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

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

      assert {:ok, tx, _consumer, _loader} = Consumer.update(tx, consumer, loader)

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

  describe "user roles state" do
    test "starts with empty state", cxt do
      {loader, rules} = loader_with_global_perms(cxt)

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

    test "can load a specific version", cxt do
      {loader, _rules} = loader_with_global_perms(cxt)

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

    test "scoped user roles are added via an insert to roles table", cxt do
      {loader, rules} = loader_with_global_perms(cxt)
      {:ok, consumer} = Consumer.new(loader)

      %{assigns: [%{id: assign_id1}, %{id: assign_id2}]} = rules

      # table: Proto.table("project_memberships"),
      # scope: Proto.scope("projects"),
      # user_column: "user_id",
      # role_name: "editor"
      tx =
        Chgs.tx([
          Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
          Chgs.insert(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            }
          )
        ])

      assert {:ok, tx, consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert %{id: 2, user_id: @user_id, rules: %{id: 2}} = perms

      assert tx.changes == [
               Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
               Chgs.insert(
                 @scoped_assign_relation,
                 %{
                   "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   "project_role" => "editor",
                   "user_id" => @user_id,
                   "project_id" => "123"
                 }
               ),
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
                 row_id: ["db87f03f-89e1-48b4-a5c3-6cdbafb2837d"],
                 assign_id: assign_id2,
                 role: "editor",
                 user_id: @user_id,
                 scope: %SatPerms.Scope{table: Proto.table("projects"), id: ["123"]}
               }
             ]

      tx =
        Chgs.tx([
          Chgs.insert(
            @unscoped_assign_relation,
            %{
              "id" => "5c0fd272-3fc2-4ae8-8574-92823c814096",
              "site_role" => "site_admin",
              "user_id" => @user_id
            }
          )
        ])

      assert {:ok, tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, _loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms

      assert tx.changes == [
               Chgs.insert(
                 @unscoped_assign_relation,
                 %{
                   "id" => "5c0fd272-3fc2-4ae8-8574-92823c814096",
                   "site_role" => "site_admin",
                   "user_id" => @user_id
                 }
               ),
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
                 row_id: ["5c0fd272-3fc2-4ae8-8574-92823c814096"],
                 assign_id: assign_id1,
                 role: "site_admin",
                 user_id: @user_id,
                 scope: nil
               },
               %SatPerms.Role{
                 row_id: ["db87f03f-89e1-48b4-a5c3-6cdbafb2837d"],
                 assign_id: assign_id2,
                 role: "editor",
                 user_id: @user_id,
                 scope: %SatPerms.Scope{table: Proto.table("projects"), id: ["123"]}
               }
             ]
    end

    test "new assign rules are used on changes in tx", cxt do
      {loader, _rules} = loader_with_global_perms(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

      assign =
        Proto.assign(
          table: Proto.table("team_memberships"),
          scope: Proto.scope("teams"),
          user_column: "user_id",
          role_column: "team_role"
        )

      ddlx = Command.ddlx(assigns: [assign])

      tx =
        Chgs.tx([
          Chgs.ddlx(ddlx),
          Chgs.insert(
            {"public", "team_memberships"},
            %{
              "id" => "b72c24b5-20b5-4eea-ab12-ec38d6adcab7",
              "team_role" => "team_owner",
              "user_id" => @user_id,
              "team_id" => "7dde618b-0cb2-44b5-8b12-b98c59338116"
            }
          )
        ])

      assert {:ok, _tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, _loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert Enum.filter(perms.roles, &(&1.assign_id == assign.id)) == [
               %SatPerms.Role{
                 row_id: ["b72c24b5-20b5-4eea-ab12-ec38d6adcab7"],
                 assign_id: assign.id,
                 role: "team_owner",
                 user_id: @user_id,
                 scope: %SatPerms.Scope{
                   table: Proto.table("teams"),
                   id: ["7dde618b-0cb2-44b5-8b12-b98c59338116"]
                 }
               }
             ]
    end

    test "user roles are updated via an update to roles table", cxt do
      {loader, rules} = loader_with_global_perms(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

      %{assigns: [_, %{id: assign_id}]} = rules

      tx =
        Chgs.tx([
          Chgs.insert(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            }
          )
        ])

      assert {:ok, _tx, consumer, loader} = Consumer.update(tx, consumer, loader)

      tx =
        Chgs.tx([
          Chgs.update(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            },
            %{
              "project_role" => "manager"
            }
          )
        ])

      assert {:ok, tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, _loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms

      assert tx.changes == [
               Chgs.update(
                 @scoped_assign_relation,
                 %{
                   "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   "project_role" => "editor",
                   "user_id" => @user_id,
                   "project_id" => "123"
                 },
                 %{
                   "project_role" => "manager"
                 }
               ),
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
                 row_id: ["db87f03f-89e1-48b4-a5c3-6cdbafb2837d"],
                 assign_id: assign_id,
                 role: "manager",
                 user_id: @user_id,
                 scope: %SatPerms.Scope{table: Proto.table("projects"), id: ["123"]}
               }
             ]
    end

    test "changes in role ownership are managed", cxt do
      {loader, rules} = loader_with_global_perms(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

      %{assigns: [_, %{id: assign_id}]} = rules

      user_id2 = "0c7afad3-213a-4158-9e89-312fc5e682e1"

      tx =
        Chgs.tx([
          Chgs.insert(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            }
          )
        ])

      assert {:ok, _tx, consumer, loader} = Consumer.update(tx, consumer, loader)

      tx =
        Chgs.tx([
          Chgs.update(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            },
            %{
              "user_id" => user_id2
            }
          )
        ])

      assert {:ok, tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert {:ok, _loader, perms2} =
               SchemaLoader.user_permissions(loader, user_id2)

      assert %{id: 3, user_id: @user_id, rules: %{id: 2}} = perms
      assert %{id: 5, user_id: user_id2, rules: %{id: 2}} = perms2

      assert tx.changes == [
               Chgs.update(
                 @scoped_assign_relation,
                 %{
                   "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   "project_role" => "editor",
                   "user_id" => @user_id,
                   "project_id" => "123"
                 },
                 %{
                   "user_id" => user_id2
                 }
               ),
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
                 row_id: ["db87f03f-89e1-48b4-a5c3-6cdbafb2837d"],
                 assign_id: assign_id,
                 role: "editor",
                 user_id: user_id2,
                 scope: %SatPerms.Scope{table: Proto.table("projects"), id: ["123"]}
               }
             ]
    end

    test "user roles are deleted with deletes to roles table", cxt do
      {loader, rules} = loader_with_global_perms(cxt)
      assert {:ok, consumer} = Consumer.new(loader)

      %{assigns: [_, %{id: assign_id}]} = rules

      tx =
        Chgs.tx([
          Chgs.insert(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            }
          ),
          Chgs.insert(
            @scoped_assign_relation,
            %{
              "id" => "5e41153f-eb42-4b97-8f42-85ca8f40fa1d",
              "project_role" => "viewer",
              "user_id" => @user_id,
              "project_id" => "234"
            }
          )
        ])

      assert {:ok, _tx, consumer, loader} = Consumer.update(tx, consumer, loader)

      tx =
        Chgs.tx([
          Chgs.delete(
            @scoped_assign_relation,
            %{
              "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
              "project_role" => "editor",
              "user_id" => @user_id,
              "project_id" => "123"
            }
          )
        ])

      assert {:ok, tx, _consumer, loader} = Consumer.update(tx, consumer, loader)

      assert {:ok, _loader, perms} =
               SchemaLoader.user_permissions(loader, @user_id)

      assert %{id: 4, user_id: @user_id, rules: %{id: 2}} = perms

      assert tx.changes == [
               Chgs.delete(
                 @scoped_assign_relation,
                 %{
                   "id" => "db87f03f-89e1-48b4-a5c3-6cdbafb2837d",
                   "project_role" => "editor",
                   "user_id" => @user_id,
                   "project_id" => "123"
                 }
               ),
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
                 row_id: ["5e41153f-eb42-4b97-8f42-85ca8f40fa1d"],
                 assign_id: assign_id,
                 role: "viewer",
                 user_id: @user_id,
                 scope: %SatPerms.Scope{table: Proto.table("projects"), id: ["234"]}
               }
             ]
    end
  end

  test "sqlite ddlx messages are a no-op", cxt do
    loader = loader(cxt)
    assert {:ok, consumer} = Consumer.new(loader)

    ddlx = Command.ddlx(sqlite: [Proto.sqlite("create table local (id primary key)")])

    tx =
      Chgs.tx([
        Chgs.insert({"public", "kittens"}, %{"size" => "cute"}),
        Chgs.ddlx(ddlx)
      ])

    assert {:ok, tx, _consumer, _loader} = Consumer.update(tx, consumer, loader)

    assert tx.changes == [
             Chgs.insert({"public", "kittens"}, %{"size" => "cute"})
           ]
  end
end

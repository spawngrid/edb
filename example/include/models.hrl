-compile({parse_transform, edb_model}).
-record('ex_User',
        {
          id = not_loaded :: not_loaded | integer(),
          email = not_loaded :: not_loaded | binary(),
          created_at = not_loaded :: not_loaded | calendar:datetime(),
          password = not_loaded :: not_loaded | binary()
        }).


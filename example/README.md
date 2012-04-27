Some usage examples (start app with ./start.sh):

```erlang
1> rr("include/models.hrl").
[ex_User]
2> User = ex_User:new([{email, <<"user@email.com">>},{password, <<"password">>}]).
#ex_User{id = not_loaded,email = <<"user@email.com">>,
         created_at = not_loaded,
         password = <<"DC2DB94E96A9C2BF9C851E4FB427F00495AA9303">>}
3> User:save().
#ex_User{id = 1,email = <<"user@email.com">>,
         created_at = not_loaded,
         password = <<"DC2DB94E96A9C2BF9C851E4FB427F00495AA9303">>}
4> ex_User:search().
#ex_User{id = 1,email = <<"user@email.com">>,
         created_at = {{2012,4,27},{2,28,9.93465}},
         password = <<"DC2DB94E96A9C2BF9C851E4FB427F00495AA9303">>}
5> ex_User:search([], [{return, list}]).
[#ex_User{id = 1,email = <<"user@email.com">>,
          created_at = {{2012,4,27},{2,28,9.93465}},
          password = <<"DC2DB94E96A9C2BF9C851E4FB427F00495AA9303">>}]
```
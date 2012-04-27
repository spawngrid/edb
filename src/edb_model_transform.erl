-module(edb_model_transform).
-export([parse_transform/2]).
-compile({parse_transform, seqbind}).

%% Parse transformation
parse_transform(Forms, Options) ->
    IsAModel = ([ true || {attribute, _, extends, edb_model} <- Forms ] == [true]),
    case IsAModel of
        true ->
            do_parse_transform(Forms, Options);
        false ->
            Forms
    end.
do_parse_transform(Forms@, Options) ->
    Module = parse_trans:get_module(Forms@),
    Fields = 
        lists:map(fun({record_field, Line, Name}) ->
                          {Name, {atom, Line, undefined}};
                     ({record_field, _, Name, Default}) ->
                          {Name, Default}
                  end,
                  [ Field || {attribute, _, record, {Name, Fields}} <- Forms@,
                             Name =:= Module,
                             Field <- Fields ]),
    RecordInfo = [Name||{Name,_Default}<-Fields],
    Indexed = lists:zip([N||{atom,_,N}<-RecordInfo],lists:seq(1,length(RecordInfo))),

    %% get/1 set/2
    GetSetForms =
        [
         begin
         Index = proplists:get_value(Name, Indexed),
             case Arity of
                 0 -> % getter
                     {function, 0, Name, 1,
                      [{clause, 0, [{var, 0, 'Tuple'}], [],
                        [ %% body
                          {call, 0, {remote, 0,
                                     {atom, 0, erlang},
                                     {atom, 0, element}},
                           [{integer, 0, Index + 1}, {var, 0, 'Tuple'}]}
                        ]}]};
                 1 -> % setter
                     {function, 0, Name, 2,
                      [{clause, 0, [{var, 0, 'Value'},{var, 0, 'Tuple'}], [],
                        [ %% body
                          {call, 0, {remote, 0,
                                     {atom, 0, erlang},
                                     {atom, 0, setelement}},
                           [{integer, 0, Index + 1}, {var, 0, 'Tuple'}, 
                            {var, 0, 'Value'}]}
                        ]}]}
             end
         end
         || {{atom, _, Name}, _} <- Fields,
            Arity <- [0,1],
            not parse_trans:function_exists(Name, Arity + 1, Forms@)
        ],
    Forms@ = parse_trans:do_insert_forms(below,
                                           GetSetForms,
                                           Forms@, parse_trans:initial_context(Forms@, Options)),
    Forms@ = lists:foldl(fun({function, _, Name, Arity, _cs},Acc) ->
                                 parse_trans:export_function(Name, Arity, Acc)
                end, Forms@,GetSetForms),

    %% new/0
    Forms@ = 
    case parse_trans:function_exists(new, 0, Forms@) of
        true -> %% do nothing, new/0 is overriden
            Forms@;
        false ->
                     add_function(new, 0,
                                  [
                                   {function, 0, new, 0, 
                                    [{clause, 0, [], [],
                                      [ %% body
                                        {call, 0, {remote, 0,
                                                % module
                                                   {atom, 0, Module},
                                                   {atom, 0, after_new}},
                                         [
                                          {tuple, 0, [{atom, 0, Module}|
                                                      [Default||{_Name,Default} <- Fields]]}
                                         ]}
                                      ]}]}
                                  ],
                                  Forms@, Options)
    end,

    %% new/1
    Forms@ = 
    case parse_trans:function_exists(new, 1, Forms@) of
        true -> %% do nothing, new/1 is overriden
            Forms@;
        false ->
                     add_function(new, 1, 
                                  [
                                   {function, 0, new, 1, 
                                    [{clause, 0, [{var, 0, 'PropList'}], [],
                                      [ %% body
                                        {call, 0, {remote, 0,
                                                   {atom, 0, Module},
                                                   {atom, 0, update}},
                                         [
                                 {var, 0, 'PropList'},
                                          {call, 0, {atom, 0, new}, []}
                                         ]
                                        }]}]}
                                  ],
                                  Forms@, Options)
    end,

    %% search/0
    Forms@ = 
    case parse_trans:function_exists(search, 0, Forms@) of
        true -> %% do nothing, search/0 is overriden
            Forms@;
        false ->
                     add_function(search, 0,
                                  [
                                   {function, 0, search, 0, 
                                    [{clause, 0, [], [],
                                      [ %% body
                                        {call, 0, {remote, 0,
                                                   {atom, 0, Module},
                                                   {atom, 0, find}},
                                         [
                                          {call, 0, {atom, 0, new}, []}
                                         ]
                                        }]}]}
                                  ], Forms@, Options)
    end,
    
    %% search/1
    Forms@ = 
    case parse_trans:function_exists(search, 1, Forms@) of
        true -> %% do nothing, search/1 is overriden
            Forms@;
        false ->
                     add_function(search, 1, 
                                  [
                                   {function, 0, search, 1, 
                                    [{clause, 0, [{var, 0, 'PropList'}], [],
                                      [ %% body
                                        {call, 0, {remote, 0,
                                                   {atom, 0, Module},
                                                   {atom, 0, find}},
                                         [
                                          {call, 0, {atom, 0, new}, [{var, 0, 'PropList'}]}
                                         ]
                                        }]}]}
                                  ],
                                  Forms@, Options)
    end,

    %% search/2
    Forms@ = 
    case parse_trans:function_exists(search, 2, Forms@) of
        true -> %% do nothing, search/2 is overriden
            Forms@;
        false ->
                     add_function(search, 2, 
                                  [
                                   {function, 0, search, 2, 
                                    [{clause, 0, [{var, 0, 'PropList'},{var, 0, 'Options'}], [],
                                      [ %% body
                                        {call, 0, {remote, 0,
                                                   {atom, 0, Module},
                                                   {atom, 0, find}},
                                         [
                                          {var, 0, 'Options'},
                                          {call, 0, {atom, 0, new}, [{var, 0, 'PropList'}]}
                                         ]
                                        }]}]}
                                  ], Forms@, Options)
    end,

    %% record_info/0
    case parse_trans:function_exists(record_info, 0, Forms@) of
        true -> %% do nothing, record_info is overriden
            Forms@;
        false ->
            F = [
                 {function, 0, record_info, 0, 
                  [{clause, 0, [], [],
                    [ %% body
                      list_to_cons(RecordInfo,0)
                    ]}]}
                ],
            add_function(record_info, 0, F, Forms@, Options)
    end.
% where
add_function(Function, Arity, F, Forms, Options) ->
    parse_trans:export_function(Function, Arity,
                                parse_trans:do_insert_forms(below, F, Forms, 
                                                             parse_trans:initial_context(Forms, Options))).

list_to_cons([],Line) ->
    {nil,Line};
list_to_cons([H|T],Line) ->
    {cons, Line, H, list_to_cons(T, Line)}.

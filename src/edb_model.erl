-module(edb_model).
-export([parse_transform/2]).
-export([after_new/1]).
-export([after_load/1,
         find_or_create/1, find_or_create/2,
         default_find_options/1,
         find/1,find/2,load/1]).
-export([update/2]).
-export([before_save/1,before_create/1,
         after_save/1,after_create/1,
         before_delete/1, after_delete/1,
         create/1,
         save/1,
         delete/1]).
-export([record_mapping/2, table_name/1, 
         attribute/2, attributes/1, with/2]).
-export([record_info/1,table/1, to_proplist/1, to_dict/1, empty/1]).
-export([whitelist/1, whitelisted/1, whitelisted/2]).
-compile({parse_transform, lager_transform}).
-compile({parse_transform, seqbind}).

-define(ATTRS,[belongs_to,has_one,has_many]).
-define(PREFIX, prefix()).

prefix() ->
    case application:get_env(edb, prefix) of
      undefined ->
        "edb_";
      {ok, Prefix} ->
        Prefix
    end.

remove_prefix(String) ->
    Prefix = ?PREFIX,
    case string:substr(String,1,length(Prefix)) of
      Prefix ->
        string:substr(String,length(Prefix) + 1, length(String));
      _ ->
        String
    end.         

table_name(Tuple) ->
    Module = element(1, Tuple),
    Model = remove_prefix(atom_to_list(Module)),
    inflector:tableize(Model).

record_mapping(Name, Tuple) ->
    case attribute(Name, Tuple) of
        not_found ->
            atom_to_list(Name);
        {belongs_to, Opts} ->
            proplists:get_value(foreign_key, Opts, atom_to_list(Name));
        _ ->
            undefined
    end.

record_info(Tuple) ->
    Module = element(1, Tuple),
    Module:record_info().

to_proplist(Tuple) ->
    lists:zip(Tuple:record_info(), tl(tuple_to_list(Tuple))).

to_dict(Tuple) ->
    dict:from_list(Tuple:to_proplist()).

empty(Tuple) ->
    lists:foldl(fun(Field, Acc) ->
                        Val = Acc:Field(),
                        if Val == not_loaded ->
                                Acc:Field(undefined);
                           true ->
                                Acc
                        end
                end, Tuple, Tuple:record_info()).

whitelist(Tuple) ->
    Module = element(1, Tuple),
    Attrs = Module:module_info(attributes),
    proplists:get_value(whitelist, Attrs, []).

whitelisted(Tuple) ->
    Module = element(1, Tuple),
    Tuple:whitelisted(Module:new()).

whitelisted(Prev, Tuple) ->
    Whitelist = Tuple:whitelist(),
    Prev:update(lists:zip(Whitelist,
                          [ Tuple:F() || F <- Whitelist ])).
    
%% QLC

table(Tuple) ->
    case Tuple:find() of
        T when is_tuple(T) ->
            Set = [T];
        Set when is_list(Set) ->
            ok
    end,
    TF = fun() ->
                 qlc_next(Set)
         end,
    InfoFun = fun(num_of_objects) ->
                      length(Set);
                 (keypos) ->
                      2;
                 (is_sorted_key) ->
                      false;
                 (is_unique_objects) -> 
                      true;
                 (_) ->
                      undefined
              end,
    qlc:table(TF, [{info_fun, InfoFun},{key_equality,'=='}]).
                      
qlc_next(none) ->
    [];
qlc_next([_|T]) ->
    T.

                 

%% Attributes

attribute(Field, Tuple) when is_atom(Field) ->
    Attributes = Tuple:attributes(),
    Attrs = [ {AttrName, Opts} || {AttrName,Attrs} <- Attributes,
                                  {AField,Opts} <- Attrs,
                                  AField =:= Field ],
    case Attrs of
        [] ->
            not_found;
        _ ->
            hd(Attrs)
    end.


attributes(Tuple) when is_tuple(Tuple) ->
    Module = element(1, Tuple),
    attributes(Module);
attributes(Module) when is_atom(Module) ->
    Attrs0 =[ {K,V} || {K,V} <- Module:module_info(attributes),
                        lists:member(K,?ATTRS) ],
    [ {K, [ normalize_attribute(Module, K, A) || A <- proplists:append_values(K, Attrs0) ]} 
      || {K, _} <- lists:ukeysort(1, Attrs0) ].
% where
normalize_attribute(M,T,A) when is_atom(A) ->
    normalize_attribute(M,T,{A, []});
normalize_attribute(M,T,{A}) when is_atom(A) ->
    normalize_attribute(M,T,{A, []});
normalize_attribute(_Module, belongs_to, {A,Opts}) when is_atom(A) andalso is_list(Opts) ->
    Model = list_to_atom(?PREFIX ++ inflector:camelize(atom_to_list(proplists:get_value(model, Opts, A)))),
    Defaults = [{model, Model}, 
                {table, (Model:new()):table_name()},
                {foreign_key, 
                 inflector:foreign_key(atom_to_list(A))
                }
               ],
    {A, lists:ukeymerge(1, 
                        lists:ukeysort(1,lists:keydelete(model, 1, Opts)),
                        lists:ukeysort(1,Defaults))};
normalize_attribute(Module,has_one, {A,Opts}) when is_atom(A) andalso is_list(Opts) ->
    Model = list_to_atom(?PREFIX ++ inflector:camelize(atom_to_list(proplists:get_value(model, Opts, A)))),

    Defaults = [{model, Model},
                {table, (Model:new()):table_name()},
                {foreign_key, 
                 begin
                     SModel = remove_prefix(atom_to_list(Module)),
                     inflector:foreign_key(SModel)
                 end
                },
                {column,
                 begin
                     SModel = remove_prefix(atom_to_list(Module)),
                     list_to_atom(inflector:underscore(SModel))
                 end
                }
               ],
    {A, lists:ukeymerge(1, 
                        lists:ukeysort(1,lists:keydelete(model, 1, Opts)),
                        lists:ukeysort(1,Defaults))};
normalize_attribute(Module,has_many, {A,Opts}) when is_atom(A) andalso is_list(Opts) ->
    Model = list_to_atom(?PREFIX ++ inflector:singularize(inflector:camelize(atom_to_list(proplists:get_value(model, Opts, A))))),
    Defaults = [{model, Model},
                {table, (Model:new()):table_name()},
                {foreign_key, 
                 begin
                     SModel = remove_prefix(atom_to_list(Module)),
                     inflector:foreign_key(SModel)
                 end
                },
                {column,
                 begin
                     SModel = remove_prefix(atom_to_list(Module)),
                     list_to_atom(inflector:underscore(SModel))
                 end
                }
               ],
    {A, lists:ukeymerge(1, 
                        lists:ukeysort(1,lists:keydelete(model, 1, Opts)),
                        lists:ukeysort(1,Defaults))}.

%% /Attributes

after_new(Tuple) ->
    Tuple.

update(PropList, Tuple) when is_list(PropList), is_tuple(Tuple) ->
    update(lists:foldl(fun({K,V},Acc) ->
                               Index = field_index(K, Tuple),
                               (Index == undefined) andalso throw({invalid_field, K}),
                               setelement(Index + 1,
                                          Acc, V)
                end, Tuple, normalize_proplist(PropList)), Tuple);

update(NewTuple, Tuple) when 
      is_tuple(NewTuple), is_tuple(Tuple),
      element(1, NewTuple) =:= element(1, Tuple) ->
    Module = element(1, NewTuple),
    update_1(NewTuple,Tuple, Module:record_info(),
             tl(tuple_to_list(NewTuple)),tl(tuple_to_list(Tuple))).
% where
update_1(_, Tuple, _, [],[]) ->
    Tuple;
update_1(NewTuple, Tuple, [_Name|Names], [H|T],[H|T1]) ->
    update_1(NewTuple,Tuple,Names,T,T1);
update_1(NewTuple, Tuple, [Name|Names], [H|T],[_|T1]) ->
    New = Tuple:Name(H),
    update_1(NewTuple,New,Names,T,T1).

normalize_proplist([]) ->
    [];
normalize_proplist([{K,V}|T]) ->
    [{normalize_proplist_1(K),V}|normalize_proplist(T)].

normalize_proplist_1(B) when is_binary(B) ->
    binary_to_atom(B, latin1);
normalize_proplist_1(S) when is_list(S) ->
    list_to_atom(S);
normalize_proplist_1(A) ->
    A.


%% Deleting
before_delete(Tuple) ->
    Tuple.
after_delete(Tuple) ->
    Tuple:id(not_loaded).

delete(Tuple0) ->
    Tuple = Tuple0:before_delete(),
    Id = Tuple:id(),
    Module = element(1, Tuple),
    Table = Tuple:table_name(),
    Query = "DELETE FROM " ++ Table ++ " WHERE " ++ safe_column_name(Table, "id") ++ " = $1",
    Vals = [Id],
    %%
    lager:debug([{edb_model, Module}],"~w:delete query: ~s, bindings: ~p",[Module,Query,Vals]),
    %%
    case edb:equery(Query, Vals) of
        {ok, 1} ->
            Tuple:after_delete();
        Error ->
            Error
    end.
    

%% Saving
before_save(Tuple) ->
    Tuple.
before_create(Tuple) ->
    Tuple.
after_create(Tuple) ->
    Tuple.
after_save(Tuple) ->
    Tuple.

create(Tuple0) ->
    Tuple = (Tuple0:before_save()):before_create(),

    Module = element(1, Tuple),
    IdNdx = field_index(id, Tuple),
    Table = Tuple:table_name(),
    Columns = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
    Values = coerce_values(tl(tuple_to_list(Tuple))),
    Inserts = lists:reverse(lists:foldl(fun (V,A) -> insert(V,A,Tuple) end,
                                        [],
                                        lists:zip(Values, Columns))),
    ColumnNamesClause = string:join([Name||{Name,_,_} <- Inserts],", "),
    Bindings = string:join([Binding||{_,Binding,_} <- Inserts],", "),
    Vals = [Val||{_,_,Val} <- Inserts],
    Query = 
        "INSERT INTO " ++ Table ++ " (" ++ ColumnNamesClause ++ ") "
        "VALUES (" ++ Bindings ++ ") RETURNING " ++ safe_column_name(Table, "id"),
    %%
    lager:debug([{edb_model, Module}],"~w:save (create) query: ~s, bindings: ~p",[Module,Query,Vals]),
    %%
    case edb:equery(Query, Vals) of
        {ok, 1, _, [{Id}]} ->
            ((setelement(IdNdx + 1, Tuple, Id)):after_create()):after_save();
        Error ->
            Error
    end.

save(Tuple) ->
    Id = element(field_index(id, Tuple) + 1, Tuple),
    save_or_create(Id, Tuple).

save_or_create(not_loaded, Tuple0) ->
    %% Create
    create(Tuple0);
save_or_create(_Id, Tuple0) ->
    Tuple = Tuple0:before_save(),

    Module = element(1, Tuple),
    IdNdx = field_index(id, Tuple),
    Id = element(IdNdx + 1, Tuple),
    Table = Tuple:table_name(),
    Columns = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
    Values = coerce_values(tl(tuple_to_list(Tuple))),
    Updates = lists:reverse(lists:foldl(fun (V,A) -> update(V,A,Tuple) end,
                                        [],
                                        lists:zip(Values, Columns))),
    ColumnNamesClause = string:join([Name ++ 
                                         " = " ++ Binding||{Name,Binding,_} <- Updates],", "),
    Vals = [Val||{_,_,Val} <- Updates] ++ [Id],
    Query = 
        "UPDATE " ++ Table ++ " SET " ++ ColumnNamesClause ++ " "
        "WHERE " ++ safe_column_name(Table, "id") ++ " = $" ++ integer_to_list(length(Updates) + 1), 
    %%
    lager:debug([{edb_model, Module}],"~w:save query: ~s, bindings: ~p",[Module,Query,Vals]),
    %%
    case edb:equery(Query, Vals) of
        {ok, N} when N == 0 orelse N == 1 -> %% 0 happens when nothing has changed
            Tuple:after_save();
        Error ->
            Error
    end.
% where
insert({virtual, _},L,_Tuple) ->
    L;
insert({not_loaded,_},L,_Tuple) ->
    L;
insert({List, {_Col, _Index}},L,_Tuple) when is_list(List) ->
    L; %% skip
insert({RelTuple,{Col,_Index}},L,Tuple) when is_tuple(RelTuple), is_atom(element(1, RelTuple)) ->
    case Tuple:attribute(Col) of
        {belongs_to, _Opts} ->
            RelIdNdx = field_index(id, RelTuple),
            RelId = relation_id(RelTuple, RelIdNdx),
            [{Tuple:record_mapping(Col),
              "$" ++ integer_to_list(length(L) + 1), RelId}|L];
        _ ->
            L
    end;
insert({Value,{Col,_Index}},L,Tuple) ->
    [{Tuple:record_mapping(Col),
      "$" ++ integer_to_list(length(L) + 1), Value}|L].
update({virtual, _},L,_Tuple) ->
    L;
update({not_loaded,_},L,_Tuple) ->
    L;
update({List, {_Col, _Index}},L,_Tuple) when is_list(List) ->
    L; %% skip
update({_, {id, _Index}},L,_Tuple) ->
    L; %% skip id
update({RelTuple,{Col,_Index}},L,Tuple) when is_tuple(RelTuple), is_atom(element(1, RelTuple)) ->
    case Tuple:attribute(Col) of
        {belongs_to, _Opts} ->
            RelIdNdx = field_index(id, RelTuple),
            RelId = relation_id(RelTuple, RelIdNdx),
            [{Tuple:record_mapping(Col),
              "$" ++ integer_to_list(length(L) + 1), RelId}|L];
        _ ->
            L
    end;
update({Value,{Col,_Index}},L,Tuple) ->
    [{Tuple:record_mapping(Col),
      "$" ++ integer_to_list(length(L) + 1), Value}|L].

relation_id(RelTuple, RelIdNdx) ->
    case element(RelIdNdx + 1, RelTuple) of
        not_loaded ->
            case RelTuple:save() of
                {error, _} = Error ->
                    RelId = undefined,
                    throw({invalid_field, element(1, RelTuple), Error});
                Relation ->
                    RelId = Relation:id()
            end;
        RelId ->
            ok
    end,
    RelId.

%% /Saving

after_load(Tuple) ->    
    Tuple.

load(Tuple) ->
    Module = element(1, Tuple),
    Emptie = Module:new(),
    Ndx = field_index(id, Tuple) + 1,
    (setelement(Ndx, Emptie, element(Ndx, Tuple))):find().

find_or_create(Tuple) ->
    Tuple:find_or_create([]).
find_or_create(Opts, Tuple) ->
    case Tuple:find(Opts) of
        not_found ->
            Tuple:create();
        [] ->
            Tuple:create();
        Result ->
            Result
    end.

default_find_options(Tuple) ->
    Module = element(1, Tuple),
    proplists:get_value(find,
                        Module:module_info(attributes),
                        []).

find(Tuple) ->
    Tuple:find([]).

find(Opts0, Tuple) ->
    Module = element(1, Tuple),
    Table = Tuple:table_name(),

    Opts = [Opts0|Tuple:default_find_options()],
    
    Select = case proplists:get_value(select, Opts, Module:record_info()) of
                 A when is_atom(A) ->
                     [A];
                 _Select ->
                     _Select
             end,

    OriginalColumns = lists:zip(Tuple:record_info(),lists:seq(1,length(Tuple:record_info()))),
    
    Columns = [ {C,I} || {C,I} <- OriginalColumns,
                         lists:member(C, Select) ],

    ColumnFields = [ {Col, Index} || {Col,Index} <- Columns,
                                     Tuple:record_mapping(Col) =/= undefined ],
    ColumnNames = [ safe_column_name(Table,Tuple:record_mapping(Col)) || 
                      {Col,_Index} <- Columns,
                      Tuple:record_mapping(Col) =/= undefined ],

    Includes = includes(proplists:get_value(include, Opts, []), Tuple),
    IncludesNames = [ begin
                          Rel = Mod:new(),
                          safe_column_name(Rel:table_name(),
                                           Rel:record_mapping(Col)) 
                      end
                      || 
                        {Mod,Cols} <- Includes,
                        {Col, _Index} <- Cols,
                        (Mod:new()):record_mapping(Col) =/= undefined ],

    ColumnNamesClause = string:join(ColumnNames ++ IncludesNames, ", "),
    
    Values = coerce_values(tl(tuple_to_list(Tuple))),
    
    Conditions@ = case proplists:get_value(conditions, Opts) of
                     undefined ->
                         [];
                     Cond ->
                         [Cond]
                 end,
    
    {_Ctr, Vals@, Where, Join, Conditions@} =
        prepare_find([], %% vals
                     OriginalColumns,
                     join(proplists:get_value(include, Opts, []), Tuple), 
                     Conditions@, 
                     Values, %% raw values
                     Tuple
                    ),

    Parameters = proplists:get_value(parameters, Opts, []),

    {Conditions@, Vals@} = lists:foldl(fun (_, {[], _}=Acc) ->
                                               Acc;
                                           ({Par, Val}, {[Cs], Vs}) ->
                                               Index = length(Vals@) + 1,
                                               Cs1 = re:replace(Cs,"\\$" ++ Par, "\\$" ++ integer_to_list(Index), 
                                                                [{return, list}]),
                                               {[Cs1], Vs ++ [Val]}
                                       end, {Conditions@, Vals@}, Parameters),

    Query = "SELECT " ++ ColumnNamesClause ++ " FROM " ++ Table ++
        Join ++ " " ++ proplists:get_value(join, Opts, "") ++ " " ++
        case Where of 
            [] -> [] ++ conditions(where, Conditions@);
            _ -> Where ++ conditions(conjunction, Conditions@)
        end ++
        order(proplists:get_value(order, Opts, [])) ++
        group(proplists:get_value(group, Opts, [])),
    %%
    lager:debug([{edb_model, Module}],"~w:load query: ~s, bindings: ~p",[Module,Query,Vals@]),
    %%
    Results = 
        case edb:equery(Query, Vals@) of
            {ok, _, L} when is_list(L) ->
                [ encode_finding([{Module, ColumnFields}|Includes], Tuple, ColumnNames ++ IncludesNames, 
                                 ensure_list(proplists:get_value(include, Opts, [])),
                                 T)
                  || T <- L ]
        end,
    Loaded = [ Result:after_load() || {_, Result} <- Results ],
    Reduced = reduce_findings(lists:concat([ F || {F, _} <- Results ]), Loaded),
    case {length(Reduced), proplists:get_value(return, Opts, adaptive)} of
        {0, adaptive} ->
            not_found;
        {1, adaptive} ->
            hd(Reduced);
        {_, adaptive} ->
            Reduced;
        {_, list} ->
            Reduced
    end.
           
% where
prepare_find(Vals, Columns, Join, Conditions, Values, Tuple) ->
    {Ctr, _Base, Vals1, Where1, Join1, Conditions1} =
    lists:foldl(fun(not_loaded, Acc) ->
                       setelement(1, Acc, element(1, Acc) + 1);
                   (virtual, Acc) ->
                       setelement(1, Acc, element(1, Acc) + 1);
                   (RelTuple, {XCtr, XBase, XVals, XWhere, XJoin, XConditions})
                      when is_tuple(RelTuple) andalso
                           is_atom(element(1, RelTuple)) ->
                        Attr = lists:nth(XCtr, Tuple:record_info()),
                        case Tuple:attribute(Attr) of
                            {belongs_to, _Opts} ->
                                RelModule = element(1, RelTuple),
                                RelColumns = lists:zip(RelModule:record_info()
                                                       ,lists:seq(1,length(RelModule:record_info()))),

                                {BaseIncr, RelWhereExpr, _XVals1} =
                                    where(XBase, 
                                          tl(tuple_to_list(RelTuple)),
                                          RelColumns,
                                          RelTuple),
                                RelJoin = join(Attr, Tuple),
                                {_, RelVals, _, _, _} =
                                    prepare_find([],
                                                 RelColumns,
                                                 RelJoin,
                                                 [],
                                                 tl(tuple_to_list(RelTuple)),
                                                 RelTuple),
                                {XCtr+1, 
                                 XBase + BaseIncr,
                                 lists:reverse(RelVals) ++ XVals, 
                                 XWhere ++ conditions(conjunction,[RelWhereExpr]),
                                 XJoin ++ RelJoin,
                                 XConditions
                                 };
                            _ ->
                                throw(not_sup)
                        end;
                   (_V, {XCtr, XBase, XVals, XWhere, XJoin, XConditions}) ->
                        Attr = lists:nth(XCtr, Tuple:record_info()),
                        {N, NWhere, XVals1} = where(XBase, [lists:nth(XCtr, Values)], [ {N,I} || {N,I} <- Columns, N == Attr],
                                            Tuple),
                        CWhere =
                        case XWhere of 
                            "" -> where;
                            _ -> conjunction
                        end,
                        {XCtr + 1, XBase + N, XVals1 ++ XVals,XWhere ++ conditions(CWhere, [NWhere]), XJoin, XConditions}
                end, {1, 0, Vals, "", Join, Conditions}, Values),
    {Ctr, lists:reverse(Vals1), Where1, Join1, Conditions1}.

where(Base, Values, Columns, Tuple) ->
    Table = Tuple:table_name(),
    {L0, NewVals} = lists:foldl(
                        fun({virtual, _},Acc) ->
                                Acc;
                           ({not_loaded,_},Acc) ->
                                Acc;
                           ({null,{Col,_Index}},{L,V}) ->
                                {[safe_column_name(Table,Tuple:record_mapping(Col)) ++ 
                                      " IS NULL"|L], V};
                           ({Val,{Col,_Index}},{L,V}) ->
                                {[safe_column_name(Table,Tuple:record_mapping(Col)) ++ 
                                     " = $" ++ 
                                     integer_to_list(length(L) + 1 + Base)|L],[Val|V]}
                        end, {[],[]}, lists:zip(Values, Columns)),
    L = lists:reverse(L0),
    {length(NewVals), string:join(L, " AND "), NewVals}.
conditions(_, []) ->
    [];
conditions(where, Conds) ->
    " WHERE " ++ string:join(Conds, " AND ");
conditions(conjunction, Conds) ->
    " AND " ++ string:join(Conds, " AND ").

includes([],_) ->
    [];
includes([H|T], Tuple) ->
    includes(H, Tuple) ++ includes(T, Tuple);
includes(Relation, Tuple) ->
    case Tuple:attribute(Relation) of
        {belongs_to, Opts} ->
            Module = proplists:get_value(model, Opts),
            Emptie = Module:new(),
            Columns = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
            [{Module, [ {Col, Index} || {Col,Index} <- Columns,
                                        Emptie:record_mapping(Col) =/= undefined ]}];
        {has_one, Opts} ->
            Module = proplists:get_value(model, Opts),
            Emptie = Module:new(),
            Columns = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
            [{Module, [ {Col, Index} || {Col,Index} <- Columns,
                                        Emptie:record_mapping(Col) =/= undefined ]}];
        {has_many, Opts} ->
            Module = proplists:get_value(model, Opts),
            Emptie = Module:new(),
            Columns = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
            [{Module, [ {Col, Index} || {Col,Index} <- Columns,
                                        Emptie:record_mapping(Col) =/= undefined ]}];
        _ ->
            []
    end.

join([],_) ->
    "";
join([H|T], Tuple) ->
    join(H, Tuple) ++ join(T, Tuple);
join(Relation, Tuple) ->
    Table = Tuple:table_name(),
    case Tuple:attribute(Relation) of
        {belongs_to, Opts} ->
            RelTable = proplists:get_value(table, Opts),
            FKey = proplists:get_value(foreign_key, Opts),
            " LEFT OUTER JOIN " ++ safe_table_name(RelTable) ++ " " ++
            "ON " ++ safe_column_name(RelTable,"id") ++ " = " ++ 
                safe_column_name(Table, FKey) ++ " ";
        {has_many, Opts} ->
            RelTable = proplists:get_value(table, Opts),
            FKey = proplists:get_value(foreign_key, Opts),
            " LEFT OUTER JOIN " ++ safe_table_name(RelTable) ++ " " ++
            "ON " ++ safe_column_name(RelTable,FKey) ++ " = " ++ 
                safe_column_name(Table, "id") ++ " ";
        {has_one, Opts} ->
            RelTable = proplists:get_value(table, Opts),
            FKey = proplists:get_value(foreign_key, Opts),
            " LEFT OUTER JOIN " ++ safe_table_name(RelTable) ++ " " ++
            "ON " ++ safe_column_name(RelTable,FKey) ++ " = " ++ 
                safe_column_name(Table, "id") ++ " ";
        _ ->
            ""
    end.

order([]) ->
    [];
order(Order) ->
    " ORDER BY " ++ Order ++ " ".

group([]) ->
    [];
group(Order) ->
    " GROUP BY " ++ Order ++ " ".

encode_finding(ColumnFields, Tuple, Cs, Includes, T) ->
    Module = element(1, Tuple),
    case proplists:get_value(Module, ColumnFields, []) of
        undefined ->
            false;
        ModuleColumnFields ->
            TablePrefix = safe_table_name(Tuple:table_name()) ++ ".",
            {_,L} = lists:unzip(lists:filter(fun({C,_Value}) ->
                                                     case string:substr(C,1,length(TablePrefix)) of
                                                         TablePrefix ->
                                                             true;
                                                         _ ->
                                                             false
                                                     end
                                             end,
                                             lists:zip(Cs, tuple_to_list(T)))),
            Instance = lists:foldl(fun({{_Field, Index}, Value}, Acc) ->
                                           setelement(Index + 1, Acc, Value)
                                   end, Tuple, lists:zip(ModuleColumnFields, L)),
            
            encode_relations(ColumnFields, Cs, T, Includes, Instance)
    end.
% where
encode_relations(ColumnFields, Cs, T, Includes, Tuple) ->
    Attributes = [ {Type, Attr} || {Type, Attributes} <- Tuple:attributes(),
                                   ({AttrName, _} = Attr) <- Attributes ,
                                   lists:member(AttrName, Includes) ],
    MainModel = element(1, Tuple),
    lists:foldl(fun
                    ({belongs_to, {Name, Opts}}, {ReduceFuns, Acc}) ->
                        Index = field_index(Name, Tuple),
                        Model = proplists:get_value(model, Opts),
                        case encode_finding(ColumnFields, Model:new(), Cs, [], T) of
                            {_, false} ->
                                {ReduceFuns, Acc};
                            {ReduceFuns1, Rel} ->
                                {ReduceFuns, setelement(Index + 1, Acc, reduce_findings(ReduceFuns1, Rel))}
                        end;
                    ({has_one, {Name, Opts}}, {ReduceFuns, Acc}) ->
                        Index = field_index(Name, Tuple),
                        Model = proplists:get_value(model, Opts),
                        case encode_finding(ColumnFields, Model:new(), Cs, [], T) of
                            {_, false} ->
                                {ReduceFuns, Acc};
                            {ReduceFuns1, Rel} ->
                                {ReduceFuns, setelement(Index + 1, Acc, reduce_findings(ReduceFuns1, Rel))}
                        end;
                    ({has_many, {Name, Opts}}, {ReduceFuns, Acc}) ->
                        Index = field_index(Name, Tuple),
                        Model = proplists:get_value(model, Opts),
                        case encode_finding(ColumnFields, Model:new(), Cs, [], T) of
                            {_, false} ->
                                {ReduceFuns, Acc};
                            {ReduceFuns1, Rel} ->
                                {[fun(L) -> has_many_reducer(Name, MainModel, L) end|ReduceFuns], setelement(Index + 1, Acc, reduce_findings(ReduceFuns1, Rel))}
                        end                
                end, 
                {[], Tuple}, Attributes).

has_many_reducer(Name, Model, L) ->
    lists:reverse(
      lists:foldl(fun (Tuple, Acc) ->
                          case element(1, Tuple) of
                              Model ->
                                  Index = field_index(id, Tuple) + 1,
                                  Id = Tuple:id(),
                                  NewVal = Tuple:Name(),
                                  case lists:keyfind(Id, Index, Acc) of
                                      false -> %% this can't be reduced
                                          [Tuple|Acc];
                                      Master ->
                                          Field = Master:Name(),
                                          NewField = lists:reverse(case Field of
                                                         List when is_list(List) ->
                                                             [NewVal|lists:delete(NewVal, List)];
                                                         virtual ->
                                                               [NewVal];
                                                         NewVal ->
                                                             [NewVal];
                                                         V ->
                                                             [NewVal,V]
                                                     end),
                                          [Tuple:Name(NewField)|lists:keyreplace(Id, Index, Acc, Master:Name(NewField))]
                                  end;
                              _ ->
                                  Acc
                          end
                end, [], L)).

reduce_findings(Funs, L) ->
    Aggregated = lists:foldl(fun(F,Acc) ->
                                     F(Acc)
                             end, L, lists:usort(Funs)),
    if 
        is_list(Aggregated) ->
            %% delete duplicates without sorting
            lists:reverse(lists:foldl(fun(E,Acc) ->
                                              [E|lists:keydelete(E:id(),field_index(id, E) + 1, Acc)]
                                      end, [], Aggregated));
        true ->
            Aggregated
    end.
                                    
with(Field, Tuple) ->
    case Tuple:attribute(Field) of
        not_found ->
            {error, not_a_relation};
        {Type, Opts} ->
            with(Type, Field, Opts, Tuple) 
    end.
% where
with(belongs_to, Field, Opts, Tuple) ->
    RelationId = element(field_index(Field,Tuple) + 1, Tuple),
    Emptie =(proplists:get_value(model, Opts)):new(),
    Relation = setelement(field_index(id, Emptie) + 1, Emptie, RelationId),
    Loaded = Relation:find(),
    setelement(field_index(Field,Tuple) + 1, Tuple, Loaded);
with(has_one, Field, Opts, Tuple) ->
    Id = element(field_index(id,Tuple) + 1, Tuple),
    Emptie = (proplists:get_value(model, Opts)):new(),
    Relation = setelement(field_index(proplists:get_value(column, Opts), Emptie) + 1, 
                          Emptie, Id),
    Loaded = Relation:find(),
    setelement(field_index(Field,Tuple) + 1, Tuple, Loaded);
with(has_many, Field, Opts, Tuple) ->
    Id = element(field_index(id,Tuple) + 1, Tuple),
    Emptie = (proplists:get_value(model, Opts)):new(),
    Relation = setelement(field_index(proplists:get_value(column, Opts), Emptie) + 1, 
                          Emptie, Id),
    case  Relation:find() of
        Loaded when is_list(Loaded) -> 
            ok;
        Record -> %% enforce list anyway
            Loaded = [Record]
    end,
    setelement(field_index(Field,Tuple) + 1, Tuple, Loaded).

            
%% Utilities
ensure_list(L) when is_list(L) ->
    L;
ensure_list(Val) ->
    [Val].

field_index(Field, Tuple) when is_tuple(Tuple) ->
    Module = element(1, Tuple),
    Indexed = lists:zip(Module:record_info(),lists:seq(1,length(Module:record_info()))),
    proplists:get_value(Field, Indexed).

safe_column_name(Table, Column) ->
    "\"" ++ Table ++ "\".\"" ++ Column ++ "\"".
safe_table_name(Table) ->
    "\"" ++ Table ++ "\"".

parse_transform(Forms, Options) ->
    edb_model_transform:parse_transform(Forms, Options).

coerce_values(Values) ->
    [ coerce_value(Value) || Value <- Values ].
coerce_value(undefined) ->
    null;
coerce_value(Other) ->
    Other.

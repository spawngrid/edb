-module(ex_User).
-include_lib("example/include/models.hrl").
-extends(edb_model).
-export([before_create/1, password/2]).
-export([encrypt_password/2]).


before_create(User) ->
    before_create_password(User).

before_create_password(#ex_User{ email = Email, password = Password } = User) ->
    User#ex_User{ password = encrypt_password(Email, Password) }.

password(_Password, #ex_User{ email = Email }) when not is_binary(Email) ->
    throw({invalid_field, email});
password(Password, #ex_User{ email = Email } = User) ->
    User#ex_User{ password = encrypt_password(Email, Password) }.    


encrypt_password(Email, Password) when is_binary(Password) andalso 
                                       size(Password) == 40 ->
    NotEncrypted = lists:any(fun(X) -> 
                                     not (lists:member(X,lists:seq($0,$9) ++ lists:seq($A,$F)))
                             end, binary_to_list(Password)),
    case NotEncrypted of
        false -> Password;
        true ->
            encrypt_password({Email, Password})
    end;

encrypt_password(Email, Password) ->
    encrypt_password({Email, Password}).

encrypt_password({Email, Password}) ->
    <<SHA:160>> = crypto:sha(list_to_binary([Email, Password])),
    iolist_to_binary(io_lib:format("~40.16.0B",[SHA])).


-module(dqe_idx_ddb).
-behaviour(dqe_idx).

%% API exports
-export([lookup/1, expand/2, init/0,
         add/5, add/6, add/7,
         delete/5, delete/6, delete/7]).

%%====================================================================
%% API functions
%%====================================================================


init() ->
    %% We do not need to initialize anything here.
    ok.

-spec lookup(dqe_idx:lqry()) ->
                    {ok, [{binary(), binary()}]}.
lookup({'in', B, M}) ->
    {ok, [{B, dproto:metric_from_list(M)}]};

lookup({'in', B, M, _Where}) ->
    {ok, [{B, dproto:metric_from_list(M)}]}.

-spec expand(dqe_idx:bucket(), [dqe_idx:glob_metric()]) ->
                    {ok, {dqe_idx:bucket(), [dqe_idx:metric()]}}.
expand(Bkt, Globs) ->
    Ps1 = lists:map(fun glob_prefix/1, Globs),
    Ps2 = compress_prefixes(Ps1),
    case Ps2 of
        all ->
            {ok, Ms} = ddb_connection:list(Bkt),
            {ok, {Bkt, Ms}};
        _ ->
            Ms1 = [begin
                       {ok, Ms} = ddb_connection:list(Bkt, P),
                       Ms
                   end || P <- Ps2],
            Ms2 = lists:usort(lists:flatten(Ms1)),
            {ok, {Bkt, Ms2}}
    end.


add(_, _, _, _, _) ->
    {ok, 0}.

add(_, _, _, _, _, _) ->
    {ok, 0}.

add(_, _, _, _, _, _, _) ->
    {ok, 0}.

delete(_, _, _, _, _) ->
    ok.

delete(_, _, _, _, _, _) ->
    ok.

delete(_, _, _, _, _, _, _) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

glob_prefix(G) ->
    glob_prefix(G, []).

glob_prefix([], Prefix) ->
    dproto:metric_from_list(lists:reverse(Prefix));
glob_prefix(['*' |_], Prefix) ->
    dproto:metric_from_list(lists:reverse(Prefix));
glob_prefix([E | R], Prefix) ->
    glob_prefix(R, [E | Prefix]).

compress_prefixes(Prefixes) ->
    compress_prefixes(lists:sort(Prefixes), []).

compress_prefixes([<<>> | _], _) ->
    all;
compress_prefixes([], R) ->
    R;
compress_prefixes([E], R) ->
    [E | R];
compress_prefixes([A, B | R], Acc) ->
    case binary:longest_common_prefix([A, B]) of
        L when L == byte_size(A) ->
            compress_prefixes([B | R], Acc);
        _ ->
            compress_prefixes([B | R], [A | Acc])
    end.

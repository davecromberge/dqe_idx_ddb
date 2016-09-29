-module(dqe_idx_ddb).
-behaviour(dqe_idx).

%% API exports
-export([init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2, metric_variants/2,
         add/4, add/5, update/5,
         delete/4, delete/5]).

%%====================================================================
%% API functions
%%====================================================================


init() ->
    %% We do not need to initialize anything here.
    ok.

lookup(Q, _G) ->
    {ok, [{B, M}]} = lookup(Q),
    {ok, [{B, M, []}]}.

lookup({'in', B, undefined}) ->
    {ok, lookup_all(B)};

lookup({'in', B, M}) ->
    {ok, [{B, dproto:metric_from_list(M)}]};

lookup({'in', B, undefined, _Where}) ->
    {ok, lookup_all(B)};

lookup({'in', B, M, _Where}) ->
    {ok, [{B, dproto:metric_from_list(M)}]}.

lookup_tags(_) ->
    {ok, []}.

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

-spec metric_variants(dqe_idx:collection(), Prefix::[dqe_idx:metric()]) ->
                    {ok, [Metric::dqe_idx:metric()]} |
                    {error, Error::term()}.

metric_variants(Collection, []) ->
    ddb_connection:list(Collection);
metric_variants(Collection, Prefix) when is_list(Prefix) ->
    N = length(Prefix),
    {ok, Metrics} = ddb_connection:list(Collection),
    Metrics1 = [dproto:metric_to_list(M) || M <- Metrics],
    Variants = [lists:nth(N + 1, M) || M <- Metrics1,
                                       length(M) > N,
                                       lists:prefix(Prefix, M)],
    {ok, lists:usort(Variants)}.

collections() ->
    ddb_connection:list().

metrics(Bucket) ->
    ddb_connection:list(Bucket).

namespaces(_) ->
    {ok, []}.

namespaces(_, _) ->
    {ok, []}.

tags(_, _) ->
    {ok, []}.

tags(_, _, _) ->
    {ok, []}.

values(_, _, _) ->
    {ok, []}.

values(_, _, _, _) ->
    {ok, []}.

add(_, _, _, _) ->
    {ok, 0}.

add(_, _, _, _, _) ->
    {ok, 0}.

update(_, _, _, _, _) ->
    {ok, 0}.

delete(_, _, _, _) ->
    ok.

delete(_, _, _, _, _) ->
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

lookup_all(Bucket) ->
    {ok, Ms} = ddb_connection:list(Bucket),
    [{Bucket, M} || M <- Ms].

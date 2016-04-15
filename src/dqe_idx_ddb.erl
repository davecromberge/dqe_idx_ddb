-module(dqe_idx_ddb).
-behaviour(dqe_idx).

%% API exports
-export([lookup/1]).

%%====================================================================
%% API functions
%%====================================================================

-spec lookup(dqe_idx:query()) -> {ok, {binary(), binary()}}.
lookup({B, M}) ->
    {ok, [{B, dproto:metric_from_list(M)}]};
lookup({B, M, _Where}) ->
    {ok, [{B, dproto:metric_from_list(M)}]}.

%%====================================================================
%% Internal functions
%%====================================================================

"""Refuse query parameters a v4 route does not declare (#1003).

FastAPI reads only the query parameters a route declares and drops the rest without a
word, so a misspelled or renamed parameter (``?threshold=`` for ``?min_score=``) used to
come back as a 200 carrying the unfiltered results. v4 request bodies already refuse an
unknown key (``extra="forbid"`` on every body model); this does the same for the query
string, so both halves of a request follow one rule.

The rejection is raised as a :class:`fastapi.exceptions.RequestValidationError`, so the
registered v4 handler shapes it like any other 422, and each entry uses pydantic's own
``extra_forbidden`` type — the one an unknown body key produces. A client handles an
unknown query parameter and an unknown body field with one branch.

Wired in :func:`api_v4.app.create_v4_app` as a dependency on every router. On the
auth-protected routers it is listed *after* the auth dependency, so a request with no
token still gets its 401 first, exactly as it does for a bad value in a declared
parameter.
"""

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute

#: Attribute the declared query-parameter names are cached under, on the route itself.
#: Stored on the route rather than in a dict keyed by ``id(route)``: ``APIRoute`` defines
#: ``__eq__`` without ``__hash__`` so it cannot be a key, and an id can be reused once a
#: route is garbage-collected (test fixtures build and drop routes), which would hand a
#: new route a dead one's set.
_CACHE_ATTRIBUTE = "_v4_declared_query_params"


def _declared_query_params(route: APIRoute) -> frozenset[str]:
    """Every query-parameter name ``route`` accepts, by its wire name (the alias).

    Walks the route's whole dependency tree, so parameters declared inside a shared
    dependency (``ResultScopeParams``, the pagination parameters) count, not only the
    ones on the endpoint function itself.

    The tree is the one on ``scope["route"]``, which FastAPI sets to the route as its
    router declared it. Since FastAPI includes routers lazily, that tree leaves out the
    dependencies added at ``include_router`` time in ``api_v4/app.py`` — today auth
    (a header) and this check, neither of which declares a query parameter. A query
    parameter on a future include-time dependency would be refused here, on every
    request that sent it, which its first test would show.

    Does not expand a Pydantic model used as a whole query (``Annotated[M, Query()]``).
    No v4 route declares one; the first that does will 422 on every one of its fields
    in its own tests, which is the cue to expand the model's fields here.
    """
    names = getattr(route, _CACHE_ATTRIBUTE, None)
    if names is None:
        found: set[str] = set()
        stack = [route.dependant]
        while stack:
            dependant = stack.pop()
            found.update(param.alias for param in dependant.query_params)
            stack.extend(dependant.dependencies)
        names = frozenset(found)
        setattr(route, _CACHE_ATTRIBUTE, names)
    return names


async def reject_unknown_query_params(request: Request) -> None:
    """422 if the query string names a parameter this route does not declare."""
    route = request.scope.get("route")
    if not isinstance(route, APIRoute):
        return
    declared = _declared_query_params(route)
    unknown = [name for name in request.query_params.keys() if name not in declared]
    if not unknown:
        return
    # dict.fromkeys de-duplicates a repeated name (?x=1&x=2) while keeping order.
    raise RequestValidationError(
        [
            {
                "type": "extra_forbidden",
                "loc": ("query", name),
                "msg": "Extra inputs are not permitted",
                "input": request.query_params.get(name),
            }
            for name in dict.fromkeys(unknown)
        ]
    )

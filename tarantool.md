
## `local tarantool = require'tarantool'`

Tarantool client for LuaJIT.
Uses [sock] by default but you can bring your own.

## API

------------------------------------------------- ----------------------------
`tarantool.connect(opt) -> tt`                    connect to server
&nbsp;`opt.host`                                        host (`'127.0.0.1'`)
`opt.port`                                        port (`3301`)
`opt.user`                                        user (optional)
`opt.password`                                    password (optional)
`opt.timeout`                                     timeout (`2`)
`opt.tcp`                                         tcp object (`sock.tcp()`)
`opt.clock`                                       clock function (`sock.clock`)
`tt:select(space,[index],[key],[sopt]) -> tuples` select tuples from a space
`sopt.limit`                                      limit (`4GB-1`)
`sopt.offset`                                     offset (`0`)
`sopt.iterator`                                   iterator
`tt:insert(space, tuple)`                         insert a tuple in a space
`tt:replace(space, tuple)`                        insert or update a tuple in a space
`tt:delete(space, key)`                           delete tuples from a space
`tt:update(space, index, key, oplist)`            [update tuples in bulk](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/)
`tt:upsert(space, index, key, oplist)`            [insert or update tuples in bulk](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/upsert/)
`tt:eval(expr, ...) -> ...`                       eval Lua expression on the server
`tt:call(fn, ...) -> ...`                         call Lua function on the server
`tt:query([opt,]sql|stmt_id, ...) -> ...`         execute SQL query
`tt:prepare(sql) -> stmt_id, params`              prepare SQL query
`tt:ping()`                                       ping
`tt:clear_metadata_cache()`                       clear `space` and `index` names
------------------------------------------------- ----------------------------

What the args mean:

* `space` and `index` can be given by name or number. Resolved names are
cached so you need to call `tt:clear_metadata_cache()` if you know that
a space or index got renamed or removed (but not when new ones are created).
* `tuple` is an array of values.
* `key` can be a string or an array of values.
* `oplist` is an array of update operations of form `{op, field, value}`.

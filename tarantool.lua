
--Tarantool client for LuaJIT.
--Written by Cosmin Apreutesei. Public Domain.

if not ... then require'tarantool_test'; return end

local ffi     = require'ffi'
local bit     = require'bit'
local mp      = require'messagepack'
local b64     = require'base64'
local sha1    = require'sha1'.sha1
local errors  = require'errors'
local glue    = require'glue'

local u8a     = glue.u8a
local u8p     = glue.u8p
local buffer  = glue.buffer
local empty   = glue.empty
local memoize = glue.memoize
local object  = glue.object

local check_io, checkp, check, protect = errors.tcp_protocol_errors'tarantool'

local c = {host = '127.0.0.1', port = 3301, timeout = 2, tracebacks = false}

--IPROTO_*
local OK         = 0
local SELECT     = 1
local INSERT     = 2
local REPLACE    = 3
local UPDATE     = 4
local DELETE     = 5
local AUTH       = 7
local EVAL       = 8
local UPSERT     = 9
local CALL       = 10
local EXECUTE    = 11
local NOP        = 12
local PREPARE    = 13
local PING       = 0x40

-- packet keys
local TYPE          = 0x00
local SYNC          = 0x01
local SPACE_ID      = 0x10
local INDEX_ID      = 0x11
local LIMIT         = 0x12
local OFFSET        = 0x13
local ITERATOR      = 0x14
local KEY           = 0x20
local TUPLE         = 0x21
local FUNCTION_NAME = 0x22
local USER_NAME     = 0x23
local EXPR          = 0x27
local OPS           = 0x28
local OPTIONS       = 0x2b
local DATA          = 0x30
local ERROR         = 0x31
local METADATA      = 0x32
local BIND_METADATA = 0x33
local BIND_COUNT    = 0x34
local SQL_TEXT      = 0x40
local SQL_BIND      = 0x41
local STMT_ID       = 0x43
local FIELD_NAME    = 0x00
local FIELD_TYPE    = 0x01
local FIELD_COLL    = 0x02
local FIELD_IS_NULLABLE = 0x03
local FIELD_IS_AUTOINCREMENT = 0x04
local FIELD_SPAN    = 0x05
local STREAM_ID     = 0x0a

-- default spaces
local SPACE_SCHEMA  = 272
local SPACE_SPACE   = 280
local SPACE_INDEX   = 288
local SPACE_FUNC    = 296
local SPACE_USER    = 304
local SPACE_PRIV    = 312
local SPACE_CLUSTER = 320

-- default views
local VIEW_SPACE = 281
local VIEW_INDEX = 289

-- index info
local INDEX_SPACE_NAME = 2
local INDEX_INDEX_NAME = 2

local function xor_strings(s1, s2)
	assert(#s1 == #s2)
	local n = #s1
	local p1 = ffi.cast(u8p, s1)
	local p2 = ffi.cast(u8p, s2)
	local b = u8a(n)
	for i = 0, n-1 do
		b[i] = bit.bxor(p1[i], p2[i])
	end
	return ffi.string(b, n)
end

local request, tselect --fw. decl.

c.connect = protect(function(opt)
	local c = object(c, opt)
	c:clear_metadata_cache()
	if not c.tcp then
		local sock = require'sock'
		c.tcp = sock.tcp
		c.clock = sock.clock
	end
	c.tcp = check_io(q, c.tcp()) --pin it so that it's closed automatically on error.
	local expires = c.clock() + c.timeout
	check_io(c, c.tcp:connect(c.host, c.port, expires))
	c._b = buffer()
	local b = c._b(64)
	check_io(c, c.tcp:recvn(b, 64, expires)) --greeting
	local salt = ffi.string(check_io(c, c.tcp:recvn(b, 64, expires)), 44)
	if c.user then
		local body = {[USER_NAME] = c.user, [TUPLE] = empty}
		if c.password and c.password ~= '' then
			local salt = b64.decode(salt):sub(1, 20)
			local s1 = sha1(c.password)
			local s2 = sha1(s1)
			local s3 = sha1(salt .. s2)
			local scramble = xor_strings(s1, s3)
			body[TUPLE] = {'chap-sha1', scramble}
		end
		request(c, AUTH, body, expires)
	end
	return c
end)

c.stream = function(c)
	c.last_stream_id = (c.last_stream_id or 0) + 1
	return object(c, {stream_id = c.last_stream_id})
end

c.close = function(c)
	return c.tcp:close()
end

--[[local]] function request(c, req_type, body, expires)
	c.sync_num = (c.sync_num or 0) + 1
	local header = {[SYNC] = c.sync_num, [TYPE] = req_type, [STREAM_ID] = c.stream_id}
	local header = mp.pack(header)
	local body = mp.pack(body)
	local len = mp.pack(#header + #body)
	local request = len .. header .. body
	check_io(c, c.tcp:send(request))
	local size = ffi.string(check_io(c, c.tcp:recvn(c._b(5), 5, expires)), 5)
	local size = mp.unpack(size)
	local s = ffi.string(check_io(c, c.tcp:recvn(c._b(size), size, expires)), size)
	local unpack_next = mp.unpacker(s)
	local _, res_header = unpack_next()
	checkp(c, res_header[SYNC] == c.sync_num)
	local _, res_body = unpack_next()
	local code = res_header[TYPE]
	if code ~= OK then
		check(c, false, res_body[ERROR])
	end
	return res_body
end

local function resolve_space(c, space)
	return type(space) == 'number' and space or c._lookup_space(space)
end

local function resolve_index(c, space, index)
	index = index or 0
	local space = resolve_space(c, space)
	return space, type(index) == 'number' and index or c._lookup_index(space, index)
end

c.clear_metadata_cache = function(c)
	c._lookup_space = memoize(function(space)
		local t = tselect(c, VIEW_SPACE, INDEX_SPACE_NAME, space)
		return check(c, t[1] and t[1][1], "no space '%s'", space)
	end)
	c._lookup_index = memoize(function(spaceno, index)
		if not spaceno then return end
		local t = tselect(c, VIEW_INDEX, INDEX_INDEX_NAME, {spaceno, index})
		return check(c, t[1] and t[1][2], "no index '%s'", index)
	end)
end

local function key_arg(key)
	return type(key) == 'table' and key or key == nil and empty or {key}
end

local function fields(t)
	local dt = {}
	for i, t in ipairs(t) do
		dt[i] = {
			name = t[FIELD_NAME],
			type = t[FIELD_TYPE],
			collation = t[FIELD_COLL],
			not_null = not t[FIELD_IS_NULLABLE],
			autoinc = t[FIELD_IS_AUTOINCREMENT],
			span = t[FIELD_SPAN],
		}
	end
	return dt
end

--[[local]] function tselect(c, space, index, key, opt)
	opt = opt or empty
	local space, index = resolve_index(c, space, index)
	local body = {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
	}
	body[LIMIT] = opt.limit or 0xFFFFFFFF
	body[OFFSET] = opt.offset or 0
	body[ITERATOR] = opt.iterator
	local expires = opt.expires or c.clock() + (opt.timeout or c.timeout)
	return request(c, SELECT, body, expires)[DATA]
end
c.select = protect(tselect)

c.insert = protect(function(c, space, tuple)
	return request(c, INSERT, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})[DATA]
end)

c.replace = protect(function(c, space, tuple)
	return request(c, REPLACE, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})[DATA]
end)

c.update = protect(function(c, space, index, key, oplist)
	local space, index = resolve_index(c, space, index)
	return request(c, UPDATE, {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
		[TUPLE] = oplist,
	})[DATA]
end)

c.delete = protect(function(c, space, key)
	local space, index = resolve_index(c, space, index)
	return request(c, DELETE, {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
	})[DATA]
end)

c.upsert = protect(function(c, space, index, key, oplist)
	return request(c, UPSERT, {
		[SPACE_ID] = resolve_space(c, space),
		[INDEX_ID] = index,
		[OPS] = oplist,
		[TUPLE] = key_arg(key),
	})[DATA]
end)

local function args(...)
	return {[mp.N] = select('#', ...), ...}
end

c.eval = protect(function(c, expr, ...)
	return unpack(request(c, EVAL, {[EXPR] = expr, [TUPLE] = args(...)})[DATA])
end)

c.call = protect(function(c, fn, ...)
	return unpack(request(c, CALL, {[FUNCTION_NAME] = fn, [TUPLE] = args(...)})[DATA])
end)

c.exec = protect(function(c, sql, params, opt, param_meta)
	if param_meta and param_meta.has_named_params then --pick params from named keys
		local t = params
		params = {}
		for i,f in ipairs(param_meta) do
			if f.index then
				params[i] = t[f.index]
			else
				params[i] = t[f.name]
			end
		end
	end
	local res = request(c, EXECUTE, {
		[STMT_ID] = type(sql) == 'number' and sql or nil,
		[SQL_TEXT] = type(sql) == 'string' and sql or nil,
		[SQL_BIND] = params,
		[OPTIONS] = opt or empty,
	})
	return res[DATA], fields(res[METADATA])
end)

local st = {}

local function params(t)
	t = fields(t)
	local j = 0
	for i,f in ipairs(t) do
		if f.name:sub(1, 1) == ':' then
			f.name = f.name:sub(2)
			t.has_named_params = true
		else
			j = j + 1
			f.index = j
		end
	end
	return t
end

c.prepare = protect(function(c, sql)
	local res = request(c, PREPARE, {
		[SQL_TEXT] = type(sql) == 'string' and sql or nil,
	})
	return object(st, {
		id = res[STMT_ID],
		conn = c,
		fields = fields(res[METADATA]),
		params = params(res[BIND_METADATA]),
	})
end)

function st:exec(params, opt)
	return self.conn:exec(self.id, params, opt, self.params)
end

c.ping = protect(function(c)
	return request(c, PING, empty)
end)


return c

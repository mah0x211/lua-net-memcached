--[[

  Copyright (C) 2016 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.


  lib/client.lua
  lua-net-memcached
  Created by Masatoshi Teruya on 16/06/02.

--]]

--- assign to local
local InetClient = require('net.stream.inet').client;
local concat = table.concat;
--- constants
local DEFAULT_PORT = '11211';
local SP = ' ';
local CRLF = '\r\n';
local NOREPLY = 'noreply' .. CRLF;


local function checkKey( key )
    if type( key ) ~= 'string' or #key < 1 or #key > 250 then
        error( 'invalid key', 2 );
    end

    return key;
end


local function checkVal( val )
    if type( val ) ~= 'string' or #val < 1 then
        error( 'invalid val', 2 );
    end

    return #val;
end


local function checkExp( exp )
    if exp == nil then
        return 0;
    elseif type( exp ) == 'number' then
        return exp;
    end

    error( 'invalid exp', 2 );
end


local function checkFlg( flg )
    if flg == nil then
        return 0;
    elseif type( flg ) == 'number' then
        return flg;
    end

    error( 'invalid flg', 2 );
end


local function checkUniq( uniq )
    if uniq then
        if type( uniq ) == 'number' then
            return uniq;
        end

        error( 'invalid uniq', 2 );
    end
end


local function checkNoreply( noreply )
    if noreply then
        if type( noreply ) ~= 'boolean' then
            error( 'invalid noreply', 2 );
        end
        return NOREPLY;
    end

    return CRLF;
end



local Client = {};


--- storageCommand
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
-- @return msg
-- @return noreply
local function storageCommand( self, cmd, key, val, exp, flg, ... )
    local msg = {
        cmd,
        SP,
        checkKey( key ),
        SP,
        checkFlg( flg ),
        SP,
        checkExp( exp ),
        SP,
        checkVal( val ),
        SP
    };
    local mlen = #msg + 1;
    local uniq, noreply, len, res, err;

    -- check cas-unique value
    if cmd == 'cas' then
        uniq, noreply = select( 1, ... );
        if uniq then
            msg[mlen] = checkUniq( uniq );
        else
            msg[mlen] = 0;
        end
        msg[mlen + 1] = SP;
        mlen = mlen + 2;
    else
        noreply = select( 1, ... );
    end

    msg[mlen] = checkNoreply( noreply );

    -- append value
    msg[mlen + 1] = val;
    msg[mlen + 2] = CRLF;
    msg = concat( msg );

    -- send
    len, err = self.sock:send( msg );
    if err then
        return false, err;
    elseif not len then
        return false, 'Connection reset by peer';
    end

    -- recv
    msg = '';
    while true do
        res, err = self.sock:recv();

        if err then
            return false, err;
        elseif not res then
            return false, 'Connection reset by peer';
        end
        msg = msg .. res;

        -- check response
        if not msg:find( CRLF, 1, true ) then
            res = nil;
        -- "STORED\r\n"
        elseif msg:find( '^STORED\r\n' ) then
            return true;
        else
            return false, msg;
        end
    end
end


--- set
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
function Client:set( ... )
    return storageCommand( self, 'set', ... );
end


--- add
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
function Client:add( ... )
    return storageCommand( self, 'add', ... );
end


--- replace
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
function Client:replace( ... )
    return storageCommand( self, 'replace', ... );
end


--- append
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
function Client:append( ... )
    return storageCommand( self, 'append', ... );
end


--- prepend
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
function Client:prepend( ... )
    return storageCommand( self, 'prepend', ... );
end


--- cas
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param uniq
-- @param noreply
function Client:prepend( ... )
    return storageCommand( self, 'cas', ... );
end


--- retrievalCommand
-- @param key
-- @param val
-- @param exp
-- @param flg
-- @param noreply
-- @return msg
-- @return noreply
local function retrievalCommand( self, cmd, ... )
    local len = select( '#', ... );
    local keys = {...};
    local msg = {
        cmd,
    };
    local key, flag, uniq, remain, tail, res, err, _;

    for i = 1, len do
        key = keys[i];
        msg[#msg + 1] = checkKey( key );
        keys[i] = nil;
        keys[key] = {};
    end

    msg[#msg + 1] = CRLF;
    msg = concat( msg, SP );
    -- send
    len, err = self.sock:send( msg );
    if err then
        return nil, err;
    elseif not len then
        return nil, 'Connection reset by peer';
    end

    -- recv
    msg = '';
    while true do
        if not res then
            res, err = self.sock:recv( remain );
        end

        if err then
            return nil, err;
        elseif not res then
            return nil, 'Connection reset by peer';
        -- check value or end response
        elseif not remain then
            msg = msg .. res;
            -- check tail(CRLF)
            _, tail = msg:find( CRLF, 1, true );
            if tail then
                -- got all response
                if msg:find('^END') then
                    return keys;
                -- probably, got error
                elseif not msg:find('^VALUE') then
                    return nil, msg;
                end

                -- extract value fields
                key, flag, len, uniq = msg:match(
                    '^VALUE (%S+) (%d+) (%d+)%s*(%d*)\r\n'
                );
                remain = len + 2;
                keys[key] = {
                    flag = flag,
                    uniq = uniq
                };
                -- subtract value fields
                msg = msg:sub( tail + 1 );
                res = '';
            else
                res = nil;
            end
        end

        -- check remain
        if remain then
            msg = msg .. res;
            remain = remain - #msg;
            if remain > 0 then
                res = nil;
            -- got invalid data
            elseif not msg:find( '^\r\n', len + 1 ) then
                return nil, 'invalid response format';
            else
                keys[key].val = msg:sub( 1, len );
                res = msg:sub( len + 3 );
                msg = '';
                remain = nil;
            end
        end
    end
end


--- get
-- @param ... keys
function Client:get( key )
    return retrievalCommand( self, 'get', key );
end


--- gets
-- @param ... keys
function Client:gets( ... )
    return retrievalCommand( self, 'gets', ... );
end


--- delete
-- @param key
-- @param noreply
function Client:delete( key, noreply )
    local msg = {
        'delete',
        SP,
        checkKey( key ),
        SP,
        checkNoreply( noreply )
    };
    local len, err, res;

    msg = concat( msg, SP );
    -- send
    len, err = self.sock:send( msg );
    if err then
        return false, err;
    elseif not len then
        return false, 'Connection reset by peer';
    end

    -- recv
    msg = '';
    while true do
        res, err = self.sock:recv();

        if err then
            return false, err;
        elseif not res then
            return false, 'Connection reset by peer';
        end

        msg = msg .. res;
        -- check response
        if not msg:find( CRLF, 1, true ) then
            res = nil;
        elseif msg:find( '^DELETED\r\n' ) then
            return true;
        else
            return false, msg;
        end
    end
end


--- new
-- @param cfg
--  host: string
--  port: string
--  nonblock: boolean
-- @return cli
-- @return err
local function new( cfg )
    local sock, err;

    if cfg == nil then
        cfg = {
            port = DEFAULT_PORT
        };
    elseif type( cfg ) == 'table' and cfg.port == nil then
        cfg.port = DEFAULT_PORT;
    end

    sock, err = InetClient.new( cfg );
    if err then
        return nil, err;
    end

    return setmetatable({
        sock = sock
    }, {
        __index = Client
    });
end


return {
    new = new
};


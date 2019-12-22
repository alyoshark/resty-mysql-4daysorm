------------------------------------------------------------------------------
--                               Require                                    --
------------------------------------------------------------------------------

local mysql = require("resty.mysql")

require('orm.class.global')
require("orm.tools.func")

local Table = require('orm.class.table')

------------------------------------------------------------------------------
--                                Constants                                 --
------------------------------------------------------------------------------
-- Global
ID = "id"
AGGREGATOR = "aggregator"
QUERY_LIST = "query_list"

------------------------------------------------------------------------------
--                              Model Settings                              --
------------------------------------------------------------------------------

if not DB then
    print("[SQL:Startup] Can't find global database settings variable 'DB'. Creating empty one.")
    DB = {}
end

DB = {
    -- ORM settings
    new = (DB.new == true),
    DEBUG = (DB.DEBUG == true),
    backtrace = (DB.backtrace == true),
    -- database settings
    name = DB.name or "app",
    host = DB.host or nil,
    port = DB.port or nil,
    username = DB.username or nil,
    password = DB.password or nil
}

local _conn = mysql:new()
local ok, err, errcode, sqlstate = _conn:connect(DB.name, DB.username, DB.password, DB.host, DB.port)

if not ok then
    BACKTRACE(ERROR, "Connect problem!")
    BACKTRACE(ERROR, err)
    BACKTRACE(ERROR, errcode)
    BACKTRACE(ERROR, table.concat({DB.host, DB.port, DB.name, DB.user}, ","))
end

------------------------------------------------------------------------------
--                               Database                                   --
------------------------------------------------------------------------------

-- Database settings
db = {
    -- Database connect instance
    connect = _conn,

    -- Execute SQL query
    execute = function (self, query)
        BACKTRACE(DEBUG, query)
        local result = self.connect:query(query)
        if result then
            return result
        else
            BACKTRACE(WARNING, "Wrong SQL query")
        end
    end,

    -- Return insert query id
    insert = function (self, query)
        local _cursor = self:execute(query)
        return 1
    end,

    -- get parced data
    rows = function (self, query, own_table)
        local _cursor = self:execute(query)
        local data = {}
        local current_row = {}
        local current_table
        local row

        if _cursor then
            row = _cursor:fetch({}, "a")

            while row do
                for colname, value in pairs(row) do
                    current_table, colname = string.divided_into(colname, "_")

                    if current_table == own_table.__tablename__ then
                        current_row[colname] = value
                    else
                        if not current_row[current_table] then
                            current_row[current_table] = {}
                        end

                        current_row[current_table][colname] = value
                    end
                end

                table.insert(data, current_row)

                current_row = {}
                row = _cursor:fetch({}, "a")
            end

        end

        return data
    end
}

return Table

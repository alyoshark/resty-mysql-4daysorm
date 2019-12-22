local quote_sql_str = ngx.quote_sql_str

local Property = require('orm.class.property')

_G.asc = Property({
    parse = function (self)
        return "`" .. self.__table__ .. "`.`" .. self.colname .. "` ASC"
    end
})

_G.desc = Property({
    parse = function (self)
        return "`" .. self.__table__ .. "`.`" .. self.colname .. "` DESC"
    end
})

_G.MAX = Property({
    parse = function (self)
        return "MAX(`" .. self.__table__ .. "`.`" .. self.colname .. "`)"
    end
})

_G.MIN = Property({
    parse = function (self)
        return "MIN(`" .. self.__table__ .. "`.`" .. self.colname .. "`)"
    end
})

_G.COUNT = Property({
    parse = function (self)
        return "COUNT(`" .. self.__table__ .. "`.`" .. self.colname .. "`)"
    end
})

_G.SUM = Property({
    parse = function (self)
        return "SUM(" .. self.colname .. ")"
    end
})

-- Escape text values to prevent sql injection
function _G.escapeValue(own_table, colname, colvalue)
  local coltype = own_table:get_column(colname)
  if coltype and coltype.settings.escape_value then
    local fieldtype = coltype.field.__type__
    if fieldtype:find("text") or fieldtype:find("char") then
      colvalue = quote_sql_str(colvalue)
    end
  end
  return colvalue;
end

#!lua name=lmgw

local function split(str,reps)
    local resultStrList = {}
    string.gsub(str,'[^'..reps..']+',function (w)
        table.insert(resultStrList,w)
    end)
    return resultStrList
end

local function test_add(keys, args)
  local field_value = tonumber(args[1])
  #local arg_value = split(args[2],',')
  return field_value + args[2] + args[3] + args[4]
end

redis.register_function{function_name='test_add',callback=test_add,flags={ 'no-writes' }}
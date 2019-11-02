--
-- Created by IntelliJ IDEA.
-- User: eugen
-- Date: 30.10.2019
-- Time: 21:22
-- To change this template use File | Settings | File Templates.
--
counter = 999999

request = function()
    path = "/v0/entity?id=key" .. counter .. "&replicas=3/3"
    wrk.method = "PUT"
    counter = counter + 1
    wrk.body = "value" .. counter
    return wrk.format(nil, path)
end
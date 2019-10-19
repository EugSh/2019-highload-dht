

counter =0

request = function()
    path = "/v0/entity?id=key" .. counter .. "&replicas=3/3"
    wrk.method = "PUT"
    counter = counter + 1
    wrk.body = "value" .. counter
    return wrk.format(nil, path)
end

for i,d in pairs(KEYS)  do

 local ks = redis.call("keys", "*/persistent/*" .. d .. "*")
 for i2,k in pairs(ks) do
    print("deleting " .. k)
    redis.call("del", k)
 end
 
end


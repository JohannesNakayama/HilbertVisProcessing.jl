for i in results[3]["result"]
    print(i["sourceUrl"], "\n")
end

protocols = []
for i in results
    tmp = [j["sourceUrl"] for j in i["result"]]
    append!(protocols, tmp)
end

unique(protocols)

l = unique(deepcopy(results[!, :domain]))

for i in l
    print(i, "\n")
end

t = unique(
    [
        i
        for i in l 
            if !startswith(i, "www") 
                & !startswith(i, "de") 
                & !startswith(i, "en")
                & !startswith(i, "de-de")
                & !startswith(i, "nl")
                & !startswith(i, "uk")
                & !startswith(i, "m.")
                & !startswith(i, "it")
                & !startswith(i, "it-it")
                & !startswith(i, "pt")
                & !startswith(i, "pl")
    ]
)

for i in t
    print(i, "\n")
end
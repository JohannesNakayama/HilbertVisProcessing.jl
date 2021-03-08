using JSON
pattern = ".7z"
data_path = "data"
compr_list = [i for i in readdir(data_path) if occursin(pattern, i)]

# extract json from 7z
function extract_json(data_path, filepaths_to_extract)
    cd(data_path)
    try
        if typeof(filepaths_to_extract) <: AbstractArray
            for fp in filepaths_to_extract
                Base.run(`7z x $fp`)
            end
        else
            Base.run(`7z x $filepaths_to_extract`)
        end
    catch e    
    end
    cd("..")
    return true
end

# remove json files (use after processing to save disk space)
function remove_json(data_path) 
    to_remove = joinpath.(data_path, [i for i in readdir(data_path) if occursin(".json", i)])
    for i in to_remove
        rm(i)
    end
    return true
end

# quick and dirty tests
begin  
    # input is array
    paths = compr_list[1:3]
    extract_json(data_path, paths)
    remove_json(data_path)
end  
begin
    # input is string
    paths = compr_list[1]
    extract_json(data_path, paths)
    remove_json(data_path)
end


test_path = compr_list[1]
extract_json(data_path, test_path)
json_list = joinpath.(data_path, [i for i in readdir(data_path) if occursin(".json", i)])

function read_json_sample(path)
    data_sample = Dict()
    open(path, "r") do file
        data_sample = JSON.parse(file)
    end
    return data_sample
end

ds = read_json_sample(json_list[1])
meta_data = pop!(ds)


# data issue: all urls the same in a large fraction of all samples on the first day
function get_urls(meta_data, index)
    [meta_data[index]["result"][i]["sourceUrl"] for i in 1:length(meta_data[index]["result"])]
end
allequal(x) = all(y -> y == x[1], x)
sum([allequal(urls) for urls in [get_urls(meta_data, index) for index in 1:length(meta_data)]])


using JSON
using DataFrames
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

# read json file from given path
function read_json_sample(path)
    data_sample = Dict()
    open(path, "r") do file
        data_sample = JSON.parse(file)
    end
    return data_sample
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

# extract, read and remove data
begin
    test_path = compr_list[1:3]  # how many archives to extract
    extract_json(data_path, test_path)  # the extraction
    json_list = joinpath.(data_path, [i for i in readdir(data_path) if occursin(".json", i)])  # list of the extracted files

    # read json files
    samples = []
    for fp in json_list
        sample = read_json_sample(fp)
        push!(samples, sample)
    end

    # remove json files
    remove_json(data_path)
end


# last item contains the actual search results with a unique hash
meta_data = [pop!(ds) for ds in samples]



##### -------------------------------------------- #####
##### DATA ISSUE: All results recorded as the same #####
##### -------------------------------------------- #####

# data issue: all urls the same in a large fraction of all samples on the first day
function get_urls(meta_data, index, day)
    [meta_data[day][index]["result"][i]["sourceUrl"] for i in 1:length(meta_data[day][index]["result"])]
end
allequal(x) = all(y -> y == x[1], x)
day = 3
sum([allequal(urls) for urls in [get_urls(meta_data, index, day) for index in 1:length(meta_data[day])]])






##### ----------------------------- #####
##### RAW DATA PROCESSING PROCEDURE #####
##### ----------------------------- #####

pattern = ".7z"
data_path = "data"
compr_list = [i for i in readdir(data_path) if occursin(pattern, i)]

extract_json(data_path, compr_list[1])

json_list = joinpath.(data_path, [i for i in readdir(data_path) if occursin(".json", i)])

result_sample = read_json_sample(json_list[1])
meta_data = pop!(result_sample)

df_list = DataFrame[]
for i in 1:length(result_sample)
    tmp_list = DataFrame[]
    for j in 1:length(result_sample[i])
        push!(tmp_list, DataFrame(result_sample[i][j]))
    end
    tmp_df = reduce(vcat, tmp_list)
    push!(df_list, tmp_df)
end

sample_df = reduce(vcat, df_list)


tmp_list = [DataFrame(meta_data[1]["result"][i]) for i in 1:length(meta_data[1]["result"])]
tmp_df = reduce(vcat, tmp_list)
tmp_df[:, :result_hash] .= meta_data[1]["result_hash"]
tmp_df[:, :rank] = 1:length(tmp_list)

innerjoin(sample_df, tmp_df, on = :result_hash)
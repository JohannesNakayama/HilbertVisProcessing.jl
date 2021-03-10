using JSON
using DataFrames
using Query
TEST = false
pattern = ".7z"
data_path = "data"

# extract json from 7z
function extract_json(data_path, filepaths_to_extract)
    cd(data_path)
    try
        if typeof(filepaths_to_extract) <: AbstractArray
            for fp in filepaths_to_extract
                Base.run(`7z x $fp -aos`)
            end
        else
            Base.run(`7z x $filepaths_to_extract -aos`)
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

# test if all elements in a list are equal [FIND REFERENCE]
allequal(x) = all(y -> y == x[1], x)

# quick and dirty tests
if TEST 
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
end


##### -------------------------------------------- #####
##### DATA ISSUE: All results recorded as the same #####
##### -------------------------------------------- #####

# meta_data == result_data -> needs new implementation

# data issue: all urls the same in a large fraction of all samples on the first day
function get_urls(meta_data, index, day)
    [meta_data[day][index]["result"][i]["sourceUrl"] for i in 1:length(meta_data[day][index]["result"])]
end
allequal(x) = all(y -> y == x[1], x)
day = 3
sum([allequal(urls) for urls in [get_urls(meta_data, index, day) for index in 1:length(meta_data[day])]])


# another data issue: apparently, there are a bunch of collisions in the hash function -.-
# SOLVED: collisions actually point to identical lists for different users (i.e., indicate less personalization)


# yet another data issue: &sa...&ved -> some urls with google specific paths seem broken
# fix broken urls (or don't break them in the first place)
# remove "sa=.*"


##### ----------------------------- #####
##### RAW DATA PROCESSING PROCEDURE #####
##### ----------------------------- #####

file_endings = Dict(
    "7z" => ".7z",
    "json" => ".json"
)
data_path = "data"
archive_list = [i for i in readdir(data_path) if occursin(file_endings["7z"], i)]
full_data_set = DataFrame[]
for archive in archive_list
    extract_json(data_path, archive)
    json_list = joinpath.(data_path, [i for i in readdir(data_path) if occursin(file_endings["json"], i)])

    # seperate meta data and search results
    meta_data = read_json_sample(json_list[1]);
    result_data = pop!(meta_data);

    # convert meta data into dataframe
    meta_data_df_list = DataFrame[]
    for i in 1:length(meta_data)
        tmp_df_list = DataFrame[]
        for j in 1:length(meta_data[i])
            push!(tmp_df_list, DataFrame(meta_data[i][j]))
        end
        tmp_df = reduce(vcat, tmp_df_list)
        push!(meta_data_df_list, tmp_df)
    end
    meta_data_df = reduce(vcat, meta_data_df_list)

    # convert search results to dataframe
    result_data_df_list = DataFrame[]
    for result_list in result_data
        # tmp_result_item = result_data[1]  # 1 becomes i
        tmp_url_list = [i["sourceUrl"] for i in result_list["result"]]
        # skip faulty result lists
        if allequal(tmp_url_list)
            continue
        end
        tmp_df = DataFrame(sourceUrl = tmp_url_list)
        tmp_df[:, :result_hash] .= result_list["result_hash"];
        tmp_df[:, :rank] = 1:length(tmp_url_list);
        push!(result_data_df_list, tmp_df)
    end
    result_data_df = reduce(vcat, result_data_df_list)
    
    # join meta data to result lists
    day_df = innerjoin(result_data_df, meta_data_df, on = :result_hash)

    # reduce data set to search (exclude news)
    day_df = day_df |> @filter(_.search_type == "search") |> DataFrame
    
    # remove redundant columns
    select!(day_df, Not([:search_type, :plugin_id]))

    # add data set for this day to the full data set and clean up
    push!(full_data_set, day_df)
    remove_json(data_path)
end

full_data_set[1]

test = reduce(vcat, full_data_set)



keywords = unique(full_data_set[1][!, :keyword])





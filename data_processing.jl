using JSON
using DataFrames
using Query
using CSV
using Suppressor
FLAG = false
BATCHSIZE = 10
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


function read_and_split_raw_data(jsonpath) 
    # seperate metadata and search results
    metadata = read_json_sample(jsonpath)
    results = pop!(metadata)
    return metadata, results
end


function metadata_to_df(metadata) 
    metadata_df_list = DataFrame[]
    for i in 1:length(metadata)
        tmp_df_list = DataFrame[]
        for j in 1:length(metadata[i])
            push!(tmp_df_list, DataFrame(metadata[i][j]))
        end
        tmp_df = reduce(vcat, tmp_df_list)
        push!(metadata_df_list, tmp_df)
    end
    metadata_df = reduce(vcat, metadata_df_list)
    return metadata_df
end

function results_to_df(results)
    results_df_list = DataFrame[]
    for result_list in results
        tmp_url_list = [i["sourceUrl"] for i in result_list["result"]]
        # skip faulty result lists
        if allequal(tmp_url_list)
            continue
        end
        tmp_df = DataFrame(sourceUrl = tmp_url_list)
        tmp_df[:, :result_hash] .= result_list["result_hash"]
        tmp_df[:, :rank] = 1:length(tmp_url_list)
        push!(results_df_list, tmp_df)
    end
    results_df = reduce(vcat, results_df_list)
    return results_df
end

function reduce_dataset(df)
    df = df |> @filter(_.search_type == "search") |> DataFrame
    select!(df, Not([:search_type, :plugin_id]))
    return df
end



##### ----------------------------- #####
##### RAW DATA PROCESSING PROCEDURE #####
##### ----------------------------- #####

if !("processed" in readdir()) 
    mkdir("processed")
end

file_endings = Dict(
    "7z" => ".7z",
    "json" => ".json"
)
data_path = "data"
archive_list = [i for i in readdir(data_path) if occursin(file_endings["7z"], i)]
filenames_map = Dict(
    "Alice Weidel" => "aliceweidel",
    "CSU" => "csu",
    "Christian Lindner" => "christianlindner",
    "Cem Özdemir" => "cemoezdemir",
    "Dietmar Bartsch" => "dietmarbartsch",
    "SPD" => "spd",
    "FDP" => "fdp",
    "CDU" => "cdu",
    "Bündnis90/Die Grünen" => "gruene",
    "Alexander Gauland" => "alexandergauland",
    "Angela Merkel" => "angelamerkel",
    "Martin Schulz" => "martinschulz",
    "AfD" => "afd",
    "Katrin Göring-Eckardt" => "katringoeringeckardt",
    "Die Linke" => "linke",
    "Sahra Wagenknecht" => "sahrawagenknecht"
)
keywords = collect(keys(filenames_map))

for archive in archive_list
    # read and seperate metadata and search results
    extract_json(data_path, archive)
    jsonpath = joinpath.(data_path, [i for i in readdir(data_path) if occursin(file_endings["json"], i)])[1]
    @suppress metadata, results = read_and_split_raw_data(jsonpath);
    remove_json(data_path)

    # put everything into one dataframe
    metadata = metadata_to_df(metadata)
    results = results_to_df(results)
    day_df = innerjoin(results, metadata, on = :result_hash)
    day_df = reduce_dataset(day_df)

    # create datasets by keyword
    for k in keywords
        filename = filenames_map[k] * ".csv"
        keyword_df = day_df |> @filter(_.keyword == k) |> DataFrame
        if filename in readdir("processed")
            CSV.write(joinpath("processed", filename), keyword_df, append = true)
        else
            CSV.write(joinpath("processed", filename), keyword_df)
        end
    end
end





##### ----------- #####
##### OTHER STUFF #####
##### ----------- #####

# quick and dirty tests
if FLAG 
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
if FLAG
    function get_urls(meta_data, index, day)
        [meta_data[day][index]["result"][i]["sourceUrl"] for i in 1:length(meta_data[day][index]["result"])]
    end
    day = 3
    sum([allequal(urls) for urls in [get_urls(meta_data, index, day) for index in 1:length(meta_data[day])]])
end

# another data issue: apparently, there are a bunch of collisions in the hash function -.-
# SOLVED: collisions actually point to identical lists for different users (i.e., indicate less personalization)


# yet another data issue: &sa...&ved -> some urls with google specific paths seem broken
# fix broken urls (or don't break them in the first place)
# remove "sa=.*"